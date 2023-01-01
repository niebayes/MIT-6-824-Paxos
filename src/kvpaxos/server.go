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
	OpType  string // Get, Put, Append or NoOp.
	Key     string
	Value   string
}

type Reply struct {
	opType         string
	getReply       *GetReply
	putAppendReply *PutAppendReply
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
	nextSeqNum int
	// the sequence number of the next op to execute.
	nextExecSeqNum int
	// key: sequence number, value: the decided op.
	decidedOps map[int]Op
	// the chosen sequence number for the op with the given clerk id and op id.
	opChosenSeqNum map[int64]map[int]int
	// key: clerk id, value: the id of the pending op, i.e. the op waiting to be replicated by paxos.
	pendingOpId map[int64]int
	// the id of the expected next op from each clerk.
	// given an op with op id opId from the clerk with clerk id clerkId.
	// there're following possible cases:
	// (1) opId < nextOpId: this op must have been executed by this server.
	// (1-a) opId < nextOpId - 1: the clerk must have received the reply for the op. Server simply discards the request.
	// (1-b) opId == nextOpId - 1: the clerk may not have received the reply for the op. Server replies with the cached reply.
	// (2) opId > nextOpId: the clerk may have received the reply for the op from some other server, and this server lags behind.
	//                      to catch up, this server needs to propose a no-op and then it will find some decided ops which may include this op.
	// (3) opId == nextOpId:
	// (3-a) this is the first time the server receives the op.
	//       server chooses a sequence number for the op and starts proposing the op at this sequence number.
	//       server replies immediately with ErrNotDecided and the clerk will wait a while and send the same request later to check if the op is decided.
	// (3-b) this is not the first time the server receives the op.
	//       server knows the op is not decided.
	//       server replies immediately with ErrNotDecided and the clerk will wait a while and send the same request later to check if the op is decided.
	// (3-c) this is not the first time the server receieves the op.
	//       server knows the op is decided, i.e. chosen as the value at some sequence number.
	//       however, ops before this sequence number are not executed yet in this server.
	//       server will try to execute ops before this op according to the chosen sequence number.
	//       if fails, server replies immediately with ErrNotExecuted and the clerk will wait a while and send the same request later to check if the op is executed.
	//       the next time server receives the same request, will run into the case (1), aka. opId < nextOpId since the op is executed and the nextOpId is incremented.
	//       if succeeds, server replies with the execution result.
	// At the clerk side, if a clerk cannot contact with a server, then it will send the request to another server.
	// but everything will be okay since each server will execute ops in the same order.
	nextOpId map[int64]int
	// cached reply for the last op received from each clerk.
	lastOpReply map[int64]Reply
}

func (kv *KVPaxos) allocateSeqNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seqNum := kv.nextSeqNum
	kv.nextSeqNum++
	return seqNum
}

// wait until the paxos instance is decided and return the decided value.
func (kv *KVPaxos) waitDecided(seqNum int) interface{} {
	lastSleepTime := initSleepTime
	for !kv.isdead() {
		status, decidedValue := kv.px.Status(seqNum)
		// FIXME: Shall I check for forgotten?
		if status == paxos.Decided {
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

func (kv *KVPaxos) executeOp(op *Op) (Err, string) {
	switch op.OpType {
	case "Get":
		value, exist := kv.db[op.Key]
		if !exist {
			return ErrNoKey, ""
		}
		return OK, value

	case "Put":
		kv.db[op.Key] = op.Value
		return OK, kv.db[op.Key]

	case "Append":
		kv.db[op.Key] += op.Value
		return OK, kv.db[op.Key]

	case "NoOp":
		// FIXME: Shall not happen.
		return OK, ""

	default:
		log.Fatalf("unexpected op type %v", op.OpType)
	}

	return "", ""
}

func (kv *KVPaxos) executeOpsUntil(seqNum int) {
	for kv.nextExecSeqNum <= seqNum {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			err, value := kv.executeOp(&op)
			printf("S%v executes op (C=%v Id=%v T=%v K=%v V=%v) at N=%v", kv.me, op.ClerkId, op.OpId, op.OpType, op.Key, op.Value, seqNum)
			kv.doneOpsUntil(kv.nextExecSeqNum)
			kv.nextExecSeqNum++
			printf("S%v updates nextExecSeqNum to %v", kv.me, kv.nextExecSeqNum)

			// update next op id.
			oldNextOpId := kv.nextOpId[op.ClerkId]
			kv.nextOpId[op.ClerkId] = op.OpId + 1
			printf("S%v updates nextOpId for C%v from %v to %v", kv.me, op.ClerkId, oldNextOpId, kv.nextOpId[op.ClerkId])

			// cache the reply.
			reply := Reply{opType: op.OpType}
			if op.OpType == "Get" {
				reply.getReply = &GetReply{Err: err, Value: value}
			} else if op.OpType == "Put" || op.OpType == "Append" {
				reply.putAppendReply = &PutAppendReply{Err: err}
			}
			kv.lastOpReply[op.ClerkId] = reply
			printf("S%v caches the reply for op (C=%v Id=%v T=%v K=%v V=%v)", kv.me, op.ClerkId, op.OpId, op.OpType, op.Key, op.Value)

		} else {
			break
		}
	}
}

func (kv *KVPaxos) doneOpsUntil(seqNum int) {
	doneSeqNums := make([]int, 0)
	for seq := range kv.decidedOps {
		if seq <= seqNum {
			doneSeqNums = append(doneSeqNums, seq)
		}
	}

	for _, seq := range doneSeqNums {
		delete(kv.decidedOps, seq)
	}

	kv.px.Done(seqNum)

	printf("S%v dones instances until sequence number %v", kv.me, seqNum)
}

// keep proposing no-ops until we've executed a op with clerk id clerkId and op id opId.
func (kv *KVPaxos) catchUp(clerkId int64, opId int) {
	printf("S%v starts catching up to (C=%v Id=%v)", kv.me, clerkId, opId)
	op := Op{ClerkId: clerkId, OpId: opId, OpType: "NoOp"}
	kv.propose(&op)
	printf("S%v caught up to (C=%v Id=%v)", kv.me, clerkId, opId)
}

func (kv *KVPaxos) getChosenSeqNum(op *Op) int {
	chosenSeqNumOfClerk, ok := kv.opChosenSeqNum[op.ClerkId]
	if ok {
		chosenSeqNum, ok := chosenSeqNumOfClerk[op.OpId]
		if ok {
			return chosenSeqNum
		}
	}
	return -1
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
		chosenSeqNumOfClerk, ok := kv.opChosenSeqNum[decidedOp.ClerkId]
		if !ok {
			kv.opChosenSeqNum[decidedOp.ClerkId] = make(map[int]int)
			chosenSeqNumOfClerk = kv.opChosenSeqNum[decidedOp.ClerkId]
		}
		chosenSeqNumOfClerk[decidedOp.OpId] = seqNum

		// try to executes ops until the decided op.
		kv.executeOpsUntil(seqNum)

		// it's our op chosen as the decided value at sequence number seqNum.
		if decidedOp.ClerkId == op.ClerkId && decidedOp.OpId == op.OpId {
			// end proposing.
			printf("S%v ends proposing (C=%v Id=%v T=%v K=%v V=%v)", kv.me, decidedOp.ClerkId, decidedOp.OpId, decidedOp.OpType, decidedOp.Key, decidedOp.Value)
			kv.pendingOpId[op.ClerkId] = -1
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

	printf("S%v receives Get (Id=%v K=%v) from C%v", kv.me, args.OpId, args.Key, args.ClerkId)

	nextOpId := kv.nextOpId[args.ClerkId]

	// reject stale requests.
	if args.OpId < nextOpId-1 {
		printf("S%v rejects stale Get (Id=%v NId=%v K=%v) from C%v", kv.me, args.OpId, nextOpId, args.Key, args.ClerkId)
		reply.Err = ErrRejected
		return nil
	}

	// try to reply the last executed op with the cached reply.
	if args.OpId == nextOpId-1 {
		lastReply, exist := kv.lastOpReply[args.ClerkId]
		if !exist || lastReply.getReply == nil {
			log.Fatalf("S%v failed to fetch the cached reply", kv.me)
		}
		reply.Err = lastReply.getReply.Err
		reply.Value = lastReply.getReply.Value
		printf("S%v replies Get (Id=%v NId=%v K=%v) from C%v with cache (E=%v RV=%v)", kv.me, args.OpId, nextOpId, args.Key, args.ClerkId, reply.Err, reply.Value)
		return nil
	}

	// reject ahead requests.
	if args.OpId > nextOpId {
		printf("S%v rejects ahead Get (Id=%v NId=%v K=%v) from C%v", kv.me, args.OpId, nextOpId, args.Key, args.ClerkId)
		// propose no-ops to catch up.
		go kv.catchUp(args.ClerkId, args.OpId)
		reply.Err = ErrRejected
		return nil
	}

	// reject if this op is pending to be decided.
	if pendingOpId, exist := kv.pendingOpId[args.ClerkId]; exist && pendingOpId == args.OpId {
		printf("S%v rejects pending Get (Id=%v NId=%v K=%v) from C%v", kv.me, args.OpId, nextOpId, args.Key, args.ClerkId)
		reply.Err = ErrNotDecided
		return nil
	}

	// try to execute the op if the op is decided.
	chosenSeqNumOfClerk, ok := kv.opChosenSeqNum[args.ClerkId]
	if ok {
		chosenSeqNum, ok := chosenSeqNumOfClerk[args.OpId]
		if ok {
			// FIXME: Shall I execute asyncly?
			kv.executeOpsUntil(chosenSeqNum)
			if kv.nextExecSeqNum == chosenSeqNum+1 {
				lastReply, exist := kv.lastOpReply[args.ClerkId]
				if !exist || lastReply.getReply == nil {
					log.Fatalf("S%v lastReply shall exist for op (C=%v N=%v Id=%v T=%v K=%v V=%v)", kv.me, args.ClerkId, chosenSeqNum, args.OpId, "Get", args.Key, "")
				}
				reply.Err = kv.lastOpReply[args.ClerkId].getReply.Err
				reply.Value = kv.lastOpReply[args.ClerkId].getReply.Value
				return nil
			}
			reply.Err = ErrNotExecuted
			return nil
		}
	}

	// start proposing the op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}
	go kv.propose(op)
	reply.Err = ErrNotDecided

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	printf("S%v receives PutAppend (Id=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, args.OpType, args.Key, args.Value, args.ClerkId)

	nextOpId := kv.nextOpId[args.ClerkId]

	// reject stale requests.
	if args.OpId < nextOpId-1 {
		printf("S%v rejects stale PutAppend (Id=%v NId=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, nextOpId, args.OpType, args.Key, args.Value, args.ClerkId)
		reply.Err = ErrRejected
		return nil
	}

	// try to reply the last executed op with the cached reply.
	if args.OpId == nextOpId-1 {
		lastReply, exist := kv.lastOpReply[args.ClerkId]
		if !exist || lastReply.putAppendReply == nil {
			log.Fatalf("S%v failed to fetch the cached reply", kv.me)
		}
		reply.Err = lastReply.putAppendReply.Err
		printf("S%v replies PutAppend (Id=%v NId=%v T=%v K=%v V=%v) from C%v with cache (E=%v)", kv.me, args.OpId, nextOpId, args.OpType, args.Key, args.Value, args.ClerkId, reply.Err)
		return nil
	}

	// reject ahead requests.
	if args.OpId > nextOpId {
		printf("S%v rejects ahead PutAppend (Id=%v NId=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, nextOpId, args.OpType, args.Key, args.Value, args.ClerkId)
		// propose no-ops to catch up.
		go kv.catchUp(args.ClerkId, args.OpId)
		reply.Err = ErrRejected
		return nil
	}

	// reject if this op is pending to be decided.
	if pendingOpId, exist := kv.pendingOpId[args.ClerkId]; exist && pendingOpId == args.OpId {
		printf("S%v rejects pending PutAppend (Id=%v NId=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, nextOpId, args.OpType, args.Key, args.Value, args.ClerkId)
		reply.Err = ErrNotDecided
		return nil
	}

	// try to execute the op if the op is decided.
	chosenSeqNumOfClerk, ok := kv.opChosenSeqNum[args.ClerkId]
	if ok {
		chosenSeqNum, ok := chosenSeqNumOfClerk[args.OpId]
		if ok {
			// FIXME: Shall I execute asyncly?
			kv.executeOpsUntil(chosenSeqNum)
			if kv.nextExecSeqNum == chosenSeqNum+1 {
				lastReply, exist := kv.lastOpReply[args.ClerkId]
				if !exist || lastReply.putAppendReply == nil {
					log.Fatalf("S%v lastReply shall exist for op (C=%v N=%v Id=%v T=%v K=%v V=%v)", kv.me, args.ClerkId, chosenSeqNum, args.OpId, args.OpType, args.Key, args.OpType)
				}
				reply.Err = kv.lastOpReply[args.ClerkId].putAppendReply.Err
				return nil
			}
			reply.Err = ErrNotExecuted
			return nil
		}
	}

	// start proposing the op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}
	go kv.propose(op)
	reply.Err = ErrNotDecided

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
	kv.nextSeqNum = 0
	kv.nextExecSeqNum = 0
	kv.decidedOps = make(map[int]Op)
	kv.opChosenSeqNum = make(map[int64]map[int]int)
	kv.pendingOpId = make(map[int64]int)
	kv.nextOpId = make(map[int64]int)
	kv.lastOpReply = make(map[int64]Reply)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

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
