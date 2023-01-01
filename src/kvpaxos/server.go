package kvpaxos

import "net"
import "fmt"
import "time"
import "net/rpc"
import "log"
import "6.824/src/paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const initSleepTime = 10 * time.Millisecond

// const maxSleepTime = 1 * time.Second
const maxSleepTime = 500 * time.Millisecond

type Op struct {
	ClerkId int64
	OpId    int
	OpType  string // Get, Put, Append.
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
	// all decided ops this server knows.
	// key: sequence number, value: the decided op.
	decidedOps map[int]Op
	// to notify the executor that there's a new decided op.
	hasNewDecidedOp sync.Cond
}

func (kv *KVPaxos) allocateSeqNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seqNum := kv.nextAllocSeqNum
	kv.nextAllocSeqNum++
	return seqNum
}

// wait until the paxos instance with sequence number sn decided.
// return the decided value when decided.
func (kv *KVPaxos) waitDecided(seqNum int) interface{} {
	lastSleepTime := initSleepTime
	for !kv.isdead() {
		status, decidedValue := kv.px.Status(seqNum)
		if status != paxos.Pending {
			// if forgotten, decidedValue will be nil.
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

func (kv *KVPaxos) executeOp(op *Op) {
	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		kv.db[op.Key] = op.Value

	case "Append":
		kv.db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected op type %v", op.OpType)
	}
}

func (kv *KVPaxos) executor() {
	kv.mu.Lock()
	for !kv.isdead() {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			// execute the decided op.
			kv.executeOp(&op)

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

		} else {
			kv.hasNewDecidedOp.Wait()
		}
	}
	kv.mu.Unlock()
}

func (kv *KVPaxos) propose(op *Op) {
	for !kv.isdead() {
		// choose a sequence number for the op.
		seqNum := kv.allocateSeqNum()

		// starts proposing the op at this sequence number.
		kv.px.Start(seqNum, *op)
		printf("S%v starts proposing op (C=%v Id=%v T=%v K=%v V=%v) at N=%v", kv.me, op.ClerkId, op.OpId, op.OpType, op.Key, op.Value, seqNum)

		// wait until the paxos instance with this sequence number is decided.
		decidedOp := kv.waitDecided(seqNum).(Op)
		printf("S%v knows op (C=%v Id=%v T=%v K=%v V=%v) is decided at N=%v", kv.me, decidedOp.ClerkId, decidedOp.OpId, decidedOp.OpType, decidedOp.Key, decidedOp.Value, seqNum)

		// store the decided op.
		kv.mu.Lock()
		kv.decidedOps[seqNum] = decidedOp

		// notify the executor thread.
		kv.hasNewDecidedOp.Signal()

		// it's our op chosen as the decided value at sequence number seqNum.
		if decidedOp.ClerkId == op.ClerkId && decidedOp.OpId == op.OpId {
			// end proposing.
			printf("S%v ends proposing (C=%v Id=%v T=%v K=%v V=%v)", kv.me, decidedOp.ClerkId, decidedOp.OpId, decidedOp.OpType, decidedOp.Key, decidedOp.Value)
			kv.mu.Unlock()
			return
		}
		// another op is chosen as the decided value at sequence number seqNum.
		// retry proposing the op at a different sequence number.
		kv.mu.Unlock()
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	printf("S%v receives Get (C=%v Id=%v K=%v)", kv.me, args.ClerkId, args.OpId, args.Key)

	// check if this is a dup request.
	isDup := false
	if opId, exist := kv.maxRecvOpIdFromClerk[args.ClerkId]; exist && opId >= args.OpId {
		isDup = true
	}

	if isDup {
		// this dup request was executed, fetch the value.
		if opId, exist := kv.maxExecOpIdOfClerk[args.ClerkId]; exist && opId >= args.OpId {
			// simply return OK whatsoever since the clerk is able to differentiate between OK and ErrNoKey from the value.
			reply.Err = OK
			reply.Value = kv.db[args.Key]
			return nil
		}

		// this dup request is not executed yet, tell the clerk to retry later.
		reply.Err = ErrNotExecuted
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := kv.maxRecvOpIdFromClerk[args.ClerkId]; !exist || opId < args.OpId {
		kv.maxRecvOpIdFromClerk[args.ClerkId] = args.OpId
	}

	// wrap the request into an op and start proposing it.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}
	go kv.propose(op)
	reply.Err = ErrNotExecuted

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	printf("S%v receives PutAppend (Id=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, args.OpType, args.Key, args.Value, args.ClerkId)

	// check if this is a dup request.
	isDup := false
	if opId, exist := kv.maxRecvOpIdFromClerk[args.ClerkId]; exist && opId >= args.OpId {
		isDup = true
	}

	if isDup {
		// this dup request was executed, simply return OK.
		if opId, exist := kv.maxExecOpIdOfClerk[args.ClerkId]; exist && opId >= args.OpId {
			reply.Err = OK
			return nil
		}

		// this dup request is not executed yet, tell the clerk to retry later.
		reply.Err = ErrNotExecuted
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := kv.maxRecvOpIdFromClerk[args.ClerkId]; !exist || opId < args.OpId {
		kv.maxRecvOpIdFromClerk[args.ClerkId] = args.OpId
	}

	// wrap the request into an op and start proposing it.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}
	go kv.propose(op)
	reply.Err = ErrNotExecuted

	return nil
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
	kv.maxRecvOpIdFromClerk = make(map[int64]int)
	kv.maxExecOpIdOfClerk = make(map[int64]int)
	kv.decidedOps = make(map[int]Op)
	kv.hasNewDecidedOp = *sync.NewCond(&kv.mu)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// start the executor thread.
	go kv.executor()

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for !kv.isdead() {
			conn, err := kv.l.Accept()
			if err == nil && !kv.isdead() {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					printf("S%v discards a request", me)
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					printf("S%v discards a reply", me)
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
