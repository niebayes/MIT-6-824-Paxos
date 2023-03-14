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
const checkMigrationStateInterval = 200 * time.Millisecond

// TODO：尝试将 polling waiting to apply 改为 channel。当时是因为啥原因来着，一开始用了 channel，后来好像 close channel 不太懂，就没用了。
// TODO: add doc for how i solve the two challenges in the 6.824 lab4.

// the normal execution phase of an op consists of:
// receive request, propose op, decide op, execute op, apply op.
type Op struct {
	ClerkId             int64
	OpId                int
	OpType              string // "Get", "Put", "Append", "InstallConfig", "InstallShard".
	Key                 string
	Value               string
	Config              shardmaster.Config // the config to be installed.
	Shard               int                // install shard op will install the shard data DB on the shard Shard.
	DB                  map[string]string
	MaxApplyOpIdOfClerk map[int64]int // the clerk state would also be installed upon the installation of the shard data.
}

type ShardState int

const (
	Serving    ShardState = iota // the server is serving the shard.
	NotServing                   // the server is not serving the shard.
	MovingIn                     // the server is waiting for the shard data to be moved in.
	MovingOut                    // the server is moving out the shard data.
)

type ShardDB struct {
	dB    map[string]string
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
	// since the sharding is static, it's convenient to store shards in a fixed-size array.
	shardDBs [shardmaster.NShards]ShardDB
	// current config.
	config shardmaster.Config
	// true if the server is reconfiguring.
	// the reconfiguring is set to true from the beginning of proposing a config change op
	// to the complete of shard migration.
	reconfiguring bool

	// in order to interact with paxos, the following fields must be used.

	// the next sequence number to allocate for an op to be proposed.
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
			reply.Value = kv.shardDBs[key2shard(op.Key)].dB[op.Key]
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
		reply.Value = kv.shardDBs[key2shard(op.Key)].dB[op.Key]
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

func (kv *ShardKV) executor() {
	kv.mu.Lock()
	for !kv.isdead() {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			// try to apply the decided op on the server.
			if kv.isAdminOp(&op) {
				kv.maybeApplyAdminOp(&op)

			} else {
				kv.maybeApplyClientOp(&op)

				if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
					// update the max proposed op id the server has ever seen to support
					// dup checking and filtering.
					kv.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
				}
			}

			// tell the paxos peer that this op is done and free server state.
			// if an op is not applied this time, it will never get applied.
			// however, a new op constructed from the same request is allowed to get applied in future.
			// therefore, this delete is safe.
			kv.px.Done(kv.nextExecSeqNum)
			delete(kv.decidedOps, kv.nextExecSeqNum)

			// update server state.
			kv.nextExecSeqNum++
			if kv.nextExecSeqNum > kv.nextAllocSeqNum {
				// although each server executes the decided ops independently,
				// a server may see ops proposed by other servers.
				// if the server proposes an op with a stale seq num, than the op would never get
				// decided.
				// hence, we need to update the seq num so that the server has more chance to
				// allocate a large-enough seq num to let the op get decided.
				kv.nextAllocSeqNum = kv.nextExecSeqNum
			}

			println("S%v state (ASN=%v ESN=%v CN=%v C=%v RId=%v EId=%v)", kv.me, kv.nextAllocSeqNum, kv.nextExecSeqNum, kv.config.Num, op.ClerkId, kv.maxPropOpIdOfClerk[op.ClerkId], kv.maxApplyOpIdOfClerk[op.ClerkId])

		} else {
			kv.hasNewDecidedOp.Wait()
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) isAdminOp(op *Op) bool {
	return op.OpType == "InstallConfig" || op.OpType == "InstallShard"
}

func (kv *ShardKV) maybeApplyAdminOp(op *Op) {
	switch op.OpType {
	case "InstallConfig":
		// install the config it's config num is one larger than the current config
		// and the server is not reconfiguring.
		if op.Config.Num == kv.config.Num+1 && !kv.reconfiguring {
			kv.installConfig(op.Config)

			println("S%v applied InstallConfig op (CN=%v) at N=%v", kv.me, op.Config.Num, kv.nextExecSeqNum)
		}

	case "InstallShard":
		// install the shard if it's not installed yet and the server is reconfiguring.
		// FIXME: it's necessary to only install the shard if the shard config is the same as the server's current config?
		if kv.reconfiguring && kv.shardDBs[op.Shard].state == MovingIn {
			kv.installShard(op)

			println("S%v applied InstallShard op (CN=%v, SN=%v) at N=%v", kv.me, op.Config.Num, op.Shard, kv.nextExecSeqNum)
		}

	default:
		log.Fatalf("unexpected admin op type %v", op.OpType)
	}
}

func (kv *ShardKV) maybeApplyClientOp(op *Op) {
	// apply the client op if it's not executed previously and the server is serving the shard.
	if opId, exist := kv.maxApplyOpIdOfClerk[op.ClerkId]; (!exist || opId < op.OpId) && kv.isServingKey(op.Key) {
		kv.applyClientOp(op)

		// update the max applied op for each clerk to implement the at-most-once semantics.
		kv.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId

		println("S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
	}
}

func (kv *ShardKV) applyClientOp(op *Op) {
	// the write is applied on the corresponding shard.
	shard := key2shard(op.Key)
	db := kv.shardDBs[shard].dB

	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		db[op.Key] = op.Value

	case "Append":
		// note: the default value is returned if the key does not exist.
		db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
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
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.mu = sync.Mutex{}
	kv.config = shardmaster.Config{Num: 0}
	kv.reconfiguring = false
	kv.nextAllocSeqNum = 0
	kv.nextExecSeqNum = 0
	kv.maxPropOpIdOfClerk = make(map[int64]int)
	kv.maxApplyOpIdOfClerk = make(map[int64]int)
	kv.decidedOps = make(map[int]Op)
	kv.hasNewDecidedOp = *sync.NewCond(&kv.mu)

	for i := range kv.shardDBs {
		kv.shardDBs[i].dB = make(map[string]string)
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
