package shardmaster

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

const backoffFactor = 2
const maxWaitTime = 1000 * time.Millisecond
const initSleepTime = 10 * time.Millisecond
const maxSleepTime = 500 * time.Millisecond
const proposeNoOpInterval = 250 * time.Millisecond

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// all decided configurations. Indexed by config nums.
	configs []Config

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
	// to notify the executor that there's a new decided op.
	hasNewDecidedOp sync.Cond
}

type GidAndShards struct {
	gid    int64
	shards []int
}

type Movement struct {
	from   int64 // from group.
	to     int64 // to group.
	shards []int // moved shards.
}

// Join, Leave and Move are write operations and hence definitely
// need to be decided by paxos before being executed.
// Query is a read operation. However, in order to let the clients see a consistent view
// of the configurations, Query also needs to be decided before being executed.

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	println("S%v receives Join (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Join, GID: args.GID, Servers: args.Servers}

	sm.mu.Lock()
	if !sm.isApplied(op) {
		go sm.propose(op)
	}
	sm.mu.Unlock()

	// wait until the op is executed or timeout.
	if applied, _ := sm.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK

		println("S%v replies Join (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotApplied
	}

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	println("S%v receives Leave (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Leave, GID: args.GID}

	sm.mu.Lock()
	if !sm.isApplied(op) {
		go sm.propose(op)
	}
	sm.mu.Unlock()

	// wait until the op is executed or timeout.
	if applied, _ := sm.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK

		println("S%v replies Leave (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotApplied
	}

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	println("S%v receives Move (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Move, GID: args.GID, Shard: args.Shard}

	sm.mu.Lock()
	if !sm.isApplied(op) {
		go sm.propose(op)
	}
	sm.mu.Unlock()

	// wait until the op is executed or timeout.
	if applied, _ := sm.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK

		println("S%v replies Move (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotApplied
	}

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	println("S%v receives Query (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Query, ConfigNum: args.Num}

	sm.mu.Lock()
	if !sm.isApplied(op) {
		go sm.propose(op)
	}
	sm.mu.Unlock()

	if applied, config := sm.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK
		reply.Config = config

		println("S%v replies Query (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotApplied
	}

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.mu = sync.Mutex{}

	sm.nextAllocSeqNum = 0
	sm.nextExecSeqNum = 0
	sm.maxApplyOpIdOfClerk = make(map[int64]int)
	sm.decidedOps = make(map[int]Op)
	sm.hasNewDecidedOp = *sync.NewCond(&sm.mu)

	// quote:
	// The very first configuration should be numbered zero.
	// It should contain no groups, and all shards should be assigned to GID zero (an invalid GID).
	// sm.configs[0].Shards is by default an array 0's and hence no need to explicitly init it.
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// start the executor thread.
	go sm.executor()

	// start a thread to periodically propose no-ops in order to let the server catches up quickly.
	go sm.noOpTicker()

	go func() {
		for !sm.isdead() {
			conn, err := sm.l.Accept()
			if err == nil && !sm.isdead() {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && !sm.isdead() {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
