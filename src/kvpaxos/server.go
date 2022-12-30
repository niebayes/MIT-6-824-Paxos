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
const maxSleepTime = 3 * time.Second

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
	// the next free sequence number.
	nextSeqNum int
	// key: clerk id, value: true if the op with id next op id is pending to be decided.
	hasPendingOp map[int64]bool
	// the id of the expected next op from each clerk.
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
		return OK, ""

	case "Append":
		kv.db[op.Key] += op.Value
		return OK, ""

	default:
		log.Fatalf("unexpected op type %v", op.OpType)
	}
	return "", ""
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()

	expectedOpId := kv.nextOpId[args.ClerkId]

	// reject ahead requests.
	if args.OpId > expectedOpId {
		printf("S%v rejects ahead Get (Id=%v K=%v) from C%v", kv.me, args.OpId, args.Key, args.ClerkId)
		reply.Err = ErrRejected
		kv.mu.Unlock()
		return nil
	}

	// reject stale requests.
	if args.OpId < expectedOpId-1 {
		printf("S%v rejects stale Get (Id=%v K=%v) from C%v", kv.me, args.OpId, args.Key, args.ClerkId)
		reply.Err = ErrRejected
		kv.mu.Unlock()
		return nil
	}

	// reject if this op is pending to be decided.
	if kv.hasPendingOp[args.ClerkId] {
		printf("S%v rejects Get (Id=%v K=%v) from C%v since it's waiting to be decided", kv.me, args.OpId, args.Key, args.ClerkId)
		reply.Err = ErrRejected
		kv.mu.Unlock()
		return nil
	}

	// try to get the cached reply if it's the latest processed op.
	if args.OpId == expectedOpId-1 {
		lastReply, cached := kv.lastOpReply[args.ClerkId]
		if !cached || lastReply.getReply == nil {
			printf("S%v failed to fetch the cached reply for Get (Id=%v K=%v) from C%v", kv.me, args.OpId, args.Key, args.ClerkId)
			reply.Err = ErrRejected
			kv.mu.Unlock()
			return nil
		}

		reply.Err = lastReply.getReply.Err
		reply.Value = lastReply.getReply.Value
		kv.mu.Unlock()
		return nil
	}

	kv.hasPendingOp[args.ClerkId] = true
	kv.mu.Unlock()

	// keep proposing this op until it's decided.
	for !kv.isdead() {
		// allocate a new sequence number for this op.
		seqNum := kv.allocateSeqNum()

		// start proposing the op and wait it to be decided
		propOp := Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key, Value: ""}
		kv.px.Start(seqNum, propOp)
		decidedOp := kv.waitDecided(seqNum).(Op)

		// execute this op whatsoever.
		kv.mu.Lock()
		err, value := kv.executeOp(&decidedOp)
		printf("S%v executes op (N=%v Id=%v T=%v K=%v V=%v) from C%v", kv.me, seqNum, decidedOp.OpId, decidedOp.OpType, decidedOp.Key, decidedOp.Value, decidedOp.ClerkId)

		if propOp.ClerkId == decidedOp.ClerkId && propOp.OpId == decidedOp.OpId {
			// the latest executed op is our op, set reply and update server state.
			reply.Err = err
			reply.Value = value

			kv.hasPendingOp[args.ClerkId] = false
			kv.lastOpReply[args.ClerkId] = Reply{opType: "Get", getReply: reply}
			kv.nextOpId[args.ClerkId] = args.OpId + 1
			kv.mu.Unlock()

			break
		}
		// otherwise, the latest executed op is not our op, retry proposing this op.
		kv.mu.Unlock()
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	reply.Err = OK
	expectedOpId := kv.nextOpId[args.ClerkId]

	// reject ahead requests.
	if args.OpId > expectedOpId {
		printf("S%v rejects ahead PutAppend (Id=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, args.OpType, args.Key, args.Value, args.ClerkId)
		reply.Err = ErrRejected
		kv.mu.Unlock()
		return nil
	}

	// reject stale requests.
	if args.OpId < expectedOpId-1 {
		printf("S%v rejects stale PutAppend (Id=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, args.OpType, args.Key, args.Value, args.ClerkId)
		reply.Err = ErrRejected
		kv.mu.Unlock()
		return nil
	}

	// reject if this op is pending to be decided.
	if kv.hasPendingOp[args.ClerkId] {
		printf("S%v rejects PutAppend (Id=%v T=%v K=%v V=%v) from C%v since it's waiting to be decided", kv.me, args.OpId, args.OpType, args.Key, args.Value, args.ClerkId)
		reply.Err = ErrRejected
		kv.mu.Unlock()
		return nil
	}

	// try to get the cached reply if it's the latest processed op.
	if args.OpId == expectedOpId-1 {
		lastReply, cached := kv.lastOpReply[args.ClerkId]
		if !cached || lastReply.getReply == nil {
			printf("S%v failed to fetch the cached reply for PutAppend (Id=%v T=%v K=%v V=%v) from C%v", kv.me, args.OpId, args.OpType, args.Key, args.Value, args.ClerkId)
			reply.Err = ErrRejected
			kv.mu.Unlock()
			return nil
		}

		reply.Err = lastReply.putAppendReply.Err
		kv.mu.Unlock()
		return nil
	}

	kv.hasPendingOp[args.ClerkId] = true
	kv.mu.Unlock()

	// keep proposing this op until it's decided.
	for !kv.isdead() {
		// allocate a new sequence number for this op.
		seqNum := kv.allocateSeqNum()

		// start proposing the op and wait it to be decided
		propOp := Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}
		kv.px.Start(seqNum, propOp)
		decidedOp := kv.waitDecided(seqNum).(Op)

		// execute this op whatsoever.
		kv.mu.Lock()
		err, _ := kv.executeOp(&decidedOp)
		printf("S%v executes op (N=%v Id=%v T=%v K=%v V=%v) from C%v", kv.me, seqNum, decidedOp.OpId, decidedOp.OpType, decidedOp.Key, decidedOp.Value, decidedOp.ClerkId)

		if propOp.ClerkId == decidedOp.ClerkId && propOp.OpId == decidedOp.OpId {
			// the latest executed op is our op, set reply and update server state.
			reply.Err = err

			kv.hasPendingOp[args.ClerkId] = false
			kv.lastOpReply[args.ClerkId] = Reply{opType: args.OpType, putAppendReply: reply}
			kv.nextOpId[args.ClerkId] = args.OpId + 1
			kv.mu.Unlock()

			break
		}
		// otherwise, the latest executed op is not our op, retry proposing this op.
		kv.mu.Unlock()
	}

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
	kv.hasPendingOp = make(map[int64]bool)
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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
