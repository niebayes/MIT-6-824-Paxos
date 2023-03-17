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

const (
	NotServing = "NotServing"
	Serving    = "Serving"
	MovingIn   = "MovingIn"
	MovingOut  = "MovingOut"
)

type ShardDB struct {
	dB      map[string]string
	state   ShardState
	fromGid int64
	toGid   int64
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32
	unreliable int32
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid                    int64
	shardDBs               [shardmaster.NShards]ShardDB
	config                 shardmaster.Config
	reconfigureToConfigNum int

	nextAllocSeqNum     int
	nextExecSeqNum      int
	maxApplyOpIdOfClerk map[int64]int
	decidedOps          map[int]Op
	hasNewDecidedOp     sync.Cond
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()

	if !kv.isServingKey(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}

	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}

	if !kv.isApplied(op) {
		go kv.propose(op)
	}
	kv.mu.Unlock()

	if applied, value := kv.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK
		reply.Value = value

	} else {
		reply.Err = ErrNotApplied
	}

	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	if !kv.isServingKey(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}

	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}

	if !kv.isApplied(op) {
		go kv.propose(op)
	}
	kv.mu.Unlock()

	if applied, _ := kv.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK

	} else {
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

	go kv.executor()
	go kv.noOpTicker()
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
