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
const maxWaitTime = 1000 * time.Millisecond
const initSleepTime = 10 * time.Millisecond
const maxSleepTime = 500 * time.Millisecond
const checkMigrationStateInterval = 200 * time.Millisecond
const proposeNoOpInterval = 250 * time.Millisecond

type ShardState string

// warning: remember to place the desired default value of the enum at the beginning so that the init value is the desired default value.
const (
	NotServing = "NotServing" // the server is not serving the shard.
	Serving    = "Serving"    // the server is serving the shard.
	MovingIn   = "MovingIn"   // the server is waiting for the shard data to be moved in.
	MovingOut  = "MovingOut"  // the server is moving out the shard data.
)

type ShardDB struct {
	dB    map[string]string // shard data, a key-value store.
	state ShardState
	// if used push-based migration, toGid is used.
	// if used pull-based migration, fromGid is used.
	fromGid int64 // the group (id) from which the server is waiting for it to move in shard data.
	toGid   int64 // the group (id) to which the server is moving out the shard data.
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
	gid int64
	// all shards of the database.
	// since the sharding is static, i.e. no shard splitting and coalescing,
	// it's convenient to store shards in a fixed-size array.
	shardDBs [shardmaster.NShards]ShardDB
	// server's current config.
	config shardmaster.Config
	// the config num of the config the server is reconfigureToConfigNum to.
	// set to -1 if the server is not reconfigureToConfigNum.
	reconfigureToConfigNum int

	// in order to interact with paxos, the following fields must be used.

	// the next sequence number to allocate for an op to be proposed.
	nextAllocSeqNum int
	// the sequence number of the next op to execute.
	nextExecSeqNum int
	// the maximum op id among all applied ops of each clerk.
	// any op with op id less than the max id won't be applied, so that the at-most-once semantics is guaranteed.
	maxApplyOpIdOfClerk map[int64]int
	// all decided ops this server knows of.
	// key: sequence number, value: the decided op.
	decidedOps map[int]Op
	// through which a proposer notifies the executor that there's a new decided op.
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
	// it's okay if we do not reject the request so long as we guarantee that
	// an op is applied only if it's not applied before.
	if !kv.isServingKey(args.Key) {
		println("S%v-%v rejects Get due to state=%v (C=%v Id=%v)", kv.gid, kv.me, kv.shardDBs[key2shard(args.Key)].state, args.ClerkId, args.OpId)
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}

	println("S%v-%v accepts Get (C=%v Id=%v)", kv.gid, kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}

	if !kv.isApplied(op) {
		go kv.propose(op)
	}
	kv.mu.Unlock()

	if applied, value := kv.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK
		reply.Value = value

		println("S%v-%v replies Get (C=%v Id=%v)", kv.gid, kv.me, op.ClerkId, op.OpId)

	} else {
		// it's not necessary to differentiate between ErrNotApplied and ErrWrongGroup here.
		// if the op is not executed because the server is not serving the shard the time the server is
		// executing the op, the client may resend the same request to this server because the reply is
		// ErrNotApplied.
		// however, this time, the request would be rejected since the server is not serving the shard
		// and ErrWrongGroup is replied.
		// the client would then query the latest config from the shardmaster and send the request to
		// another replica group who is serving the shard.
		//
		// note, the same reasoning applies to all places where ErrNotApplied is returned.
		reply.Err = ErrNotApplied
	}

	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	// reply ErrWrongGroup if not serving the given key.
	// see reasoning in `Get`.
	if !kv.isServingKey(args.Key) {
		println("S%v-%v rejects PutAppend due to state=%v (C=%v Id=%v)", kv.gid, kv.me, kv.shardDBs[key2shard(args.Key)].state, args.ClerkId, args.OpId)
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}

	println("S%v-%v accepts PutAppend (C=%v Id=%v)", kv.gid, kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}

	if !kv.isApplied(op) {
		go kv.propose(op)
	}
	kv.mu.Unlock()

	if applied, _ := kv.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK
		println("S%v-%v replies PutAppend (C=%v Id=%v)", kv.gid, kv.me, op.ClerkId, op.OpId)

	} else {
		// see reasoning in `Get`.
		reply.Err = ErrNotApplied
	}

	return nil
}

func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

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
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.mu = sync.Mutex{}

	kv.gid = gid
	kv.config = shardmaster.Config{Num: 0}
	kv.reconfigureToConfigNum = -1
	for i := range kv.shardDBs {
		kv.shardDBs[i].dB = make(map[string]string)
		kv.shardDBs[i].state = NotServing
	}

	kv.nextAllocSeqNum = 0
	kv.nextExecSeqNum = 0
	kv.maxApplyOpIdOfClerk = make(map[int64]int)
	kv.decidedOps = make(map[int]Op)
	kv.hasNewDecidedOp = *sync.NewCond(&kv.mu)

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

	// start a thread to periodically propose no-ops in order to let the server catches up quickly.
	go kv.noOpTicker()

	// start a thread to monitor migration state periodically.
	go kv.migrator()

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
