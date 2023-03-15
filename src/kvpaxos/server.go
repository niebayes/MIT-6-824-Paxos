package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"6.824/src/paxos"
)

const initSleepTime = 10 * time.Millisecond
const maxSleepTime = 500 * time.Millisecond
const proposeNoOpInterval = 250 * time.Millisecond

type Op struct {
	ClerkId int64
	OpId    int
	OpType  string // "Get", "Put", "Append", "NoOp".
	Key     string
	Value   string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// key-value database.
	db map[string]string

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

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()

	println("S%v receives Get (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}

	// check if this is a dup request.
	isDup := false
	if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Get (C=%v Id=%v) is dup", kv.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		kv.mu.Unlock()
		if kv.waitUntilAppliedOrTimeout(op) {
			// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.db[op.Key]
			kv.mu.Unlock()

			println("S%v replies Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		kv.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	kv.mu.Unlock()

	// start proposing the op.
	go kv.propose(op)

	// wait until the op is executed or timeout.
	if kv.waitUntilAppliedOrTimeout(op) {
		// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()

		println("S%v replies Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	println("S%v receives PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

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

			println("S%v replies PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		kv.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	kv.mu.Unlock()

	// start proposing the op.
	go kv.propose(op)

	// wait until the op is executed or timeout.
	if kv.waitUntilAppliedOrTimeout(op) {
		reply.Err = OK

		println("S%v replies PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (kv *KVPaxos) executor() {
	kv.mu.Lock()
	for !kv.isdead() {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			if kv.isNoOp(&op) {
				// skip no-ops.

			} else {
				kv.maybeApplyClientOp(&op)
			}

			// tell the paxos peer that this op is done and free server state.
			// if an op is not applied this time, it will never get applied.
			// however, a new op constructed from the same request is allowed to get applied in future.
			// therefore, this delete is safe.
			kv.px.Done(kv.nextExecSeqNum)

			// free server state.
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

			// println("S%v state (ASN=%v ESN=%v C=%v RId=%v EId=%v)", kv.me, kv.nextAllocSeqNum, kv.nextExecSeqNum, op.ClerkId, kv.maxPropOpIdOfClerk[op.ClerkId], kv.maxPropOpIdOfClerk[op.ClerkId])

		} else {
			kv.hasNewDecidedOp.Wait()
		}
	}
	kv.mu.Unlock()
}

func (kv *KVPaxos) maybeApplyClientOp(op *Op) {
	// apply the client op if it's not executed previously and the server is serving the shard.
	if opId, exist := kv.maxApplyOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		kv.applyClientOp(op)

		// update the max applied op for each clerk to implement the at-most-once semantics.
		kv.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId

		println("S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
	}
}

func (kv *KVPaxos) applyClientOp(op *Op) {
	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		kv.db[op.Key] = op.Value

	case "Append":
		// note: the default value is returned if the key does not exist.
		kv.db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
}

func (kv *KVPaxos) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (kv *KVPaxos) noOpTicker() {
	for !kv.isdead() {
		op := &Op{OpType: "NoOp"}
		go kv.propose(op)

		time.Sleep(proposeNoOpInterval)
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.mu = sync.Mutex{}

	kv.db = make(map[string]string)

	kv.nextAllocSeqNum = 0
	kv.nextExecSeqNum = 0
	kv.maxPropOpIdOfClerk = make(map[int64]int)
	kv.maxApplyOpIdOfClerk = make(map[int64]int)
	kv.decidedOps = make(map[int]Op)
	kv.hasNewDecidedOp = *sync.NewCond(&kv.mu)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// start the executor thread.
	go kv.executor()

	// start a thread to periodically propose no-ops in order to let the server catches up quickly.
	go kv.noOpTicker()

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for !kv.isdead() {
			conn, err := kv.l.Accept()
			if err == nil && !kv.isdead() {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					println("S%v discards a request", me)
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					println("S%v discards a reply", me)
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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
