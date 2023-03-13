package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "6.824/src/paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "6.824/src/shardmaster"

const backoffFactor = 2
const maxWaitTime = 500 * time.Millisecond
const initSleepTime = 10 * time.Millisecond
const maxSleepTime = 500 * time.Millisecond
const handoffShardsInterval = 200 * time.Millisecond

type Op struct {
	ClerkId int64
	OpId    int
	OpType  string
	Key     string
	Value   string
	Config  shardmaster.Config // the config to be installed.
}

type ShardState int

const (
	Serving    ShardState = iota // the server is serving the shard.
	NotServing                   // the server is not serving the shard.
	MovingIn                     // the server is waiting for the shard data to be moved in.
	MovingOut                    // the server is moving out the shard data.
)

type ShardDB struct {
	// uppercased since ShardDB needs to be sent through RPC.
	DB      map[string]string
	State   ShardState
	FromGid int64 // the group (id) from which the server is waiting for it to move in shard data.
	ToGid   int64 // the group (id) to which the server is moving out the shard data.
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	// the group id of the replica group this server is in.
	// each shard-kv server is a member belongs to a replica group.
	// this field is static.
	gid int64
	// all shards of the database.
	// each replica group will only serve a subset of shards of the complete database.
	shardDBs [shardmaster.NShards]ShardDB
	// current config.
	config shardmaster.Config
	// true if the server is waiting to install a config which was proposed previously by the server.
	waitingToInstallConfig bool

	// in order to interact with paxos, the following fields must be used.

	// the next sequence number to allocate for an op.
	nextAllocSeqNum int
	// the sequence number of the next op to execute.
	nextExecSeqNum int
	// the maximum op id among all the ops proposed for each clerk.
	// any op with op id less than the max id is regarded as a dup op.
	maxPropOpIdOfClerk map[int64]int
	// the maximum op id among all the applied ops of each clerk.
	// any op with op id less than the max id won't be applied.
	maxApplyOpIdOfClerk map[int64]int
	// all decided ops this server knows of.
	// key: sequence number, value: the decided op.
	decidedOps map[int]Op
	// to notify the executor that there's a new decided op.
	hasNewDecidedOp sync.Cond
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()

	// reply ErrWrongGroup if not serving the given key.
	// we assume the config change is not a frequent operation and hence
	// if the time we receive the request the server is not serving the
	// given key, there has little chance that this server will serve
	// the given key the time the op constructed from the request is being
	// executed.
	if !kv.isServingKey(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		println("S%v rejects Get (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)
		return nil
	}

	println("S%v accepts Get (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}

	// check if this is a dup request.
	isDup := false
	if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
		isDup = true
		println("S%v knows Get (C=%v Id=%v) is dup", kv.me, op.ClerkId, op.OpId)
	}

	if isDup {
		kv.mu.Unlock()
		if kv.waitUntilAppliedOrTimeout(op) {
			// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.shardDBs[key2shard(op.Key)].DB[op.Key]
			kv.mu.Unlock()

			println("S%v replies Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

		} else {
			// it's not necessary to differentiate between ErrNotExecuted and ErrWrongGroup here.
			// if the op is not executed because the server is not serving the shard the time the server is
			// executing the op, the client may resend the same request to the server because the reply is
			// ErrNotExecuted.
			// however, this time, the request would be rejected since the server is not serving the shard
			// and ErrWrongGroup is replied.
			// the client would then query the latest config from the shardmaster and send the request to
			// another replica group.
			//
			// note, the same reasoning applies to all places where ErrNotExecuted is returned.
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update the max proposed op id the server has ever seen to support
	// dup checking and filtering.
	if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		kv.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	kv.mu.Unlock()

	// start proposing the op.
	go kv.propose(op)

	// wait until the op is executed or timeout.
	if kv.waitUntilAppliedOrTimeout(op) {
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.shardDBs[key2shard(op.Key)].DB[op.Key]
		kv.mu.Unlock()

		println("S%v replies Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	// reply ErrWrongGroup if not serving the given key.
	if !kv.isServingKey(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		println("S%v rejects PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)
		return nil
	}

	println("S%v accepts PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}

	// check if this is a dup request.
	isDup := false
	if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows PutAppend (C=%v Id=%v) is dup", kv.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		kv.mu.Unlock()
		if kv.waitUntilAppliedOrTimeout(op) {
			reply.Err = OK
			println("S%v knows PutAppend (C=%v Id=%v) was applied", kv.me, op.ClerkId, op.OpId)

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update the max proposed op id the server has ever seen to support
	// dup checking and filtering.
	if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		kv.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	kv.mu.Unlock()

	// start proposing the op.
	go kv.propose(op)

	// wait until the op is executed or timeout.
	if kv.waitUntilAppliedOrTimeout(op) {
		reply.Err = OK
		println("S%v knows PutAppend (C=%v Id=%v) was applied", kv.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (kv *ShardKV) maybeApplyOp(op *Op) {
	if op.OpType == "ConfigChange" {
		// install the new config if it's more up-to-date than the current config and the server is not
		// migrating shards.
		if op.Config.Num > kv.config.Num && !kv.isMigrating() {
			kv.installConfig(op.Config)

			println("S%v applied ConfigChange op (CN=%v) at N=%v", kv.me, op.Config.Num, kv.nextExecSeqNum)
		}
	} else {
		// apply the client op if it's not executed previously and the server is serving the shard.
		if opId, exist := kv.maxApplyOpIdOfClerk[op.ClerkId]; (!exist || opId < op.OpId) && kv.isServingKey(op.Key) {
			kv.applyClientOp(op)

			// update the max applied op for each clerk to implement the at-most-once semantics.
			kv.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId

			println("S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
		}
	}
}

func (kv *ShardKV) applyClientOp(op *Op) {
	// the write is applied on the corresponding shard.
	shard := key2shard(op.Key)
	db := kv.shardDBs[shard].DB

	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		db[op.Key] = op.Value

	case "Append":
		// note: the default value is returned if the key does not exist.
		db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected op type %v", op.OpType)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.mu = sync.Mutex{}
	kv.config = shardmaster.Config{Num: 0}
	kv.waitingToInstallConfig = false
	kv.nextAllocSeqNum = 0
	kv.nextExecSeqNum = 0
	kv.maxPropOpIdOfClerk = make(map[int64]int)
	kv.maxApplyOpIdOfClerk = make(map[int64]int)
	kv.decidedOps = make(map[int]Op)
	kv.hasNewDecidedOp = *sync.NewCond(&kv.mu)

	for i := range kv.shardDBs {
		kv.shardDBs[i].DB = make(map[string]string)
	}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// start the executor thread.
	go kv.executor()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for !kv.isdead() {
			conn, err := kv.l.Accept()
			if err == nil && !kv.isdead() {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && !kv.isdead() {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for !kv.isdead() {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
