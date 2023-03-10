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

type Op struct {
	ClerkId int64
	OpId    int
	OpType  string // Get, Put, Append, ConfigChange
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
	db      map[string]string
	state   ShardState
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
	// each shard-kv server is a member belongs to a replica group.
	// this field is static.
	gid int64
	// current config.
	config shardmaster.Config
	// true if this server is reconfiguring.
	reconfiguring bool
	// all shards of the database.
	// each replica group will only serve a subset of shards of the complete database.
	shardDBs [shardmaster.NShards]ShardDB

	// inorder to interact with paxos, the following fields must be used.
	// the next sequence number to allocate for an op.
	nextAllocSeqNum int
	// the sequence number of the next op to execute.
	nextExecSeqNum int
	// the maximum op ids received from each clerk.
	// any request with op id less than the max id is regarded as a dup request.
	maxRecvOpIdFromClerk map[int64]int
	// the maximum op id of the executed ops of each clerk.
	// any op with op id less than the max id won't be executed.
	maxExecOpIdOfClerk map[int64]int
	// all decided ops this server knows of.
	// key: sequence number, value: the decided op.
	decidedOps map[int]Op
	// to notify the executor that there's a new decided op.
	hasNewDecidedOp sync.Cond
}

// TODO: record the from-to move log, create a struct {from_gid, to_gid, moved_shards}

func (kv *ShardKV) allocateSeqNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seqNum := kv.nextAllocSeqNum
	kv.nextAllocSeqNum++
	return seqNum
}

// wait until the paxos instance with sequence number seqNum decided.
// return the decided value when decided.
func (kv *ShardKV) waitUntilDecided(seqNum int) interface{} {
	lastSleepTime := initSleepTime
	for !kv.isdead() {
		status, decidedValue := kv.px.Status(seqNum)
		if status != paxos.Pending {
			// if forgotten, decidedValue will be nil.
			// but this shall not happen since this value is forgotten only after
			// this server has called Done on this value.
			return decidedValue
		}

		// wait a while and retry.
		sleepTime := lastSleepTime * backoffFactor
		if sleepTime > maxSleepTime {
			sleepTime = maxSleepTime
		}
		time.Sleep(sleepTime)
		lastSleepTime = sleepTime
	}
	return nil
}

func (kv *ShardKV) executeOp(op *Op) {
	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":

	case "Append":

	case "ConfigChange":

	default:
		log.Fatalf("unexpected op type %v", op.OpType)
	}
}

func (kv *ShardKV) executor() {
	kv.mu.Lock()
	for !kv.isdead() {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			// execute the decided op if it is not executed yet.
			// this ensures the same op won't be execute more than once by a server.
			// the same op might be executed more than once if different servers proposes the same
			// request at different sequence numbers and just happens that they are all decided.
			// there's no way to avoid such case since the paxos has the ability to decide multiple values
			// at the same time.
			if opId, exist := kv.maxExecOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
				kv.executeOp(&op)
				println("S%v executes op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
			}

			// tell the paxos peer that this op is done.
			kv.px.Done(kv.nextExecSeqNum)

			// free server state.
			delete(kv.decidedOps, kv.nextExecSeqNum)

			// update server state.
			kv.nextExecSeqNum++
			if kv.nextExecSeqNum > kv.nextAllocSeqNum {
				kv.nextAllocSeqNum = kv.nextExecSeqNum
			}
			if opId, exist := kv.maxExecOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
				kv.maxExecOpIdOfClerk[op.ClerkId] = op.OpId
			}
			if opId, exist := kv.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
				kv.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
			}

			println("S%v state (ASN=%v ESN=%v C=%v RId=%v EId=%v)", kv.me, kv.nextAllocSeqNum, kv.nextExecSeqNum, op.ClerkId, kv.maxRecvOpIdFromClerk[op.ClerkId], kv.maxExecOpIdOfClerk[op.ClerkId])

		} else {
			kv.hasNewDecidedOp.Wait()
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) propose(op *Op) {
	for !kv.isdead() {
		// choose a sequence number for the op.
		seqNum := kv.allocateSeqNum()

		// starts proposing the op at this sequence number.
		kv.px.Start(seqNum, *op)
		println("S%v starts proposing op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, seqNum)

		// wait until the paxos instance with this sequence number is decided.
		decidedOp := kv.waitUntilDecided(seqNum).(Op)
		println("S%v knows op (C=%v Id=%v) is decided at N=%v", kv.me, decidedOp.ClerkId, decidedOp.OpId, seqNum)

		// store the decided op.
		kv.mu.Lock()
		kv.decidedOps[seqNum] = decidedOp

		// update server state.
		if opId, exist := kv.maxRecvOpIdFromClerk[decidedOp.ClerkId]; !exist || opId < decidedOp.OpId {
			kv.maxRecvOpIdFromClerk[decidedOp.ClerkId] = decidedOp.OpId
		}

		// notify the executor thread.
		kv.hasNewDecidedOp.Signal()

		// it's our op chosen as the decided value at sequence number seqNum.
		if decidedOp.ClerkId == op.ClerkId && decidedOp.OpId == op.OpId {
			// end proposing.
			println("S%v ends proposing (C=%v Id=%v)", kv.me, decidedOp.ClerkId, decidedOp.OpId)
			kv.mu.Unlock()
			return
		}
		// another op is chosen as the decided value at sequence number seqNum.
		// retry proposing the op at a different sequence number.
		kv.mu.Unlock()
	}
}

// return true if the op is executed before timeout.
func (kv *ShardKV) waitUntilExecutedOrTimeout(op *Op) bool {
	startTime := time.Now()
	for time.Since(startTime) < maxWaitTime {
		kv.mu.Lock()
		if opId, exist := kv.maxExecOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
			kv.mu.Unlock()
			return true
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()

	println("S%v receives Get (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}

	// check if this is a dup request.
	isDup := false
	if opId, exist := kv.maxRecvOpIdFromClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Get (C=%v Id=%v) is dup", kv.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		kv.mu.Unlock()
		if kv.waitUntilExecutedOrTimeout(op) {
			// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
			println("S%v replies Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
			reply.Err = OK
			kv.mu.Lock()
			// reply.Value = kv.db[op.Key]
			kv.mu.Unlock()

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := kv.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
		kv.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
	}
	kv.mu.Unlock()

	// start proposing the op.
	go kv.propose(op)

	// wait until the op is executed or timeout.
	if kv.waitUntilExecutedOrTimeout(op) {
		// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
		println("S%v replies Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
		reply.Err = OK
		kv.mu.Lock()
		// reply.Value = kv.db[op.Key]
		kv.mu.Unlock()

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

// TODO: reject handling the request if this server is not serving the key currently.
// do not block handling other keys even if this server is reconfiguring.
// i.e. for a shard, if its state is serving, serve it.
// if its state is not serving, moving in, moving out, do not serve it.
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	println("S%v receives PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}

	// check if this is a dup request.
	isDup := false
	if opId, exist := kv.maxRecvOpIdFromClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows PutAppend (C=%v Id=%v) is dup", kv.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		kv.mu.Unlock()
		if kv.waitUntilExecutedOrTimeout(op) {
			// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
			println("S%v replies PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
			reply.Err = OK

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := kv.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
		kv.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
	}
	kv.mu.Unlock()

	// start proposing the op.
	go kv.propose(op)

	// wait until the op is executed or timeout.
	if kv.waitUntilExecutedOrTimeout(op) {
		// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
		println("S%v replies PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
		reply.Err = OK

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	// a config change is performed only if there's no pending config change.
	if kv.reconfiguring {
		return
	}

	latestConfig := kv.sm.Query(-1)

	kv.mu.Lock()
	// if the config change affects this server, i.e. served shards changed.
	if latestConfig.Num > kv.config.Num {
		// TODO: do not update server state locally. You have to construct
		// a config change op and sync this op among all the servers in the replica group using paxos
		// when this op is being executed, start moving in/out shards if necessary.

		servedShardsChanged := false
		for shard := 0; shard < shardmaster.NShards; shard++ {
			currGid := kv.config.Shards[shard]
			newGid := latestConfig.Shards[shard]
			if currGid == kv.gid && newGid != kv.gid {
				// move this shard from this group to the group with gid newGid.
				kv.shardDBs[shard].state = MovingOut
				kv.shardDBs[shard].toGid = newGid
				servedShardsChanged = true
			}
			if currGid != kv.gid && newGid == kv.gid {
				// move this shard from the group with gid currGid to this group.
				kv.shardDBs[shard].state = MovingIn
				kv.shardDBs[shard].fromGid = currGid
				servedShardsChanged = true
			}
		}

		if servedShardsChanged {
			kv.reconfiguring = true
		}
	}
	kv.mu.Unlock()
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
	kv.reconfiguring = false
	kv.nextAllocSeqNum = 0
	kv.nextExecSeqNum = 0
	kv.maxRecvOpIdFromClerk = make(map[int64]int)
	kv.maxExecOpIdOfClerk = make(map[int64]int)
	kv.decidedOps = make(map[int]Op)
	kv.hasNewDecidedOp = *sync.NewCond(&kv.mu)

	for i := range kv.shardDBs {
		kv.shardDBs[i].db = map[string]string{}
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
