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
const maxWaitTime = 500 * time.Millisecond
const initSleepTime = 10 * time.Millisecond
const maxSleepTime = 500 * time.Millisecond
const proposeNoOpInterval = 250 * time.Millisecond

type OpType string

// op types.
const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

// fields have to be in upper-case since ops would be wrapped into RPC messages.
type Op struct {
	ClerkId   int64
	OpId      int
	OpType    OpType   // "Join", "Leave", "Move", "Query", "NoOp".
	GID       int64    // group id. Used by Join, Leave, Move.
	Servers   []string // group server ports. Used by Join.
	Shard     int      // shard id. Used by Move.
	ConfigNum int      // configuration id. Used by Query.
}

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
	sm.mu.Lock()

	println("S%v receives Join (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Join, GID: args.GID, Servers: args.Servers}

	// check if this is a dup request.
	isDup := false
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Join (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilAppliedOrTimeout(op) {
			println("S%v replies Join (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
			reply.Err = OK

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilAppliedOrTimeout(op) {
		println("S%v replies Join (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
		reply.Err = OK

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()

	println("S%v receives Leave (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Leave, GID: args.GID}

	// check if this is a dup request.
	isDup := false
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Leave (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilAppliedOrTimeout(op) {
			println("S%v replies Leave (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
			reply.Err = OK

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilAppliedOrTimeout(op) {
		println("S%v replies Leave (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
		reply.Err = OK

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()

	println("S%v receives Move (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Move, GID: args.GID, Shard: args.Shard}

	// check if this is a dup request.
	isDup := false
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Move (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilAppliedOrTimeout(op) {
			println("S%v replies Move (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
			reply.Err = OK

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilAppliedOrTimeout(op) {
		println("S%v replies Move (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
		reply.Err = OK

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()

	println("S%v receives Query (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Query, ConfigNum: args.Num}

	// check if this is a dup request.
	isDup := false
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Query (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilAppliedOrTimeout(op) {
			sm.mu.Lock()
			latestConfig := sm.configs[len(sm.configs)-1]
			if op.ConfigNum == -1 || op.ConfigNum > latestConfig.Num {
				reply.Config = latestConfig
			} else {
				reply.Config = sm.configs[op.ConfigNum]
			}
			sm.mu.Unlock()

			reply.Err = OK
			println("S%v replies Query (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := sm.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilAppliedOrTimeout(op) {
		sm.mu.Lock()
		latestConfig := sm.configs[len(sm.configs)-1]
		if op.ConfigNum == -1 || op.ConfigNum > latestConfig.Num {
			reply.Config = latestConfig
		} else {
			reply.Config = sm.configs[op.ConfigNum]
		}
		sm.mu.Unlock()

		reply.Err = OK
		println("S%v replies Query (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

func (sm *ShardMaster) executor() {
	sm.mu.Lock()
	for !sm.isdead() {
		op, decided := sm.decidedOps[sm.nextExecSeqNum]
		if decided {
			if sm.isNoOp(&op) {
				// skip no-ops.

			} else {
				sm.maybeApplyClientOp(&op)
			}

			// tell the paxos peer that this op is done and free server state.
			// if an op is not applied this time, it will never get applied.
			// however, a new op constructed from the same request is allowed to get applied in future.
			// therefore, this delete is safe.
			sm.px.Done(sm.nextExecSeqNum)

			// free server state.
			delete(sm.decidedOps, sm.nextExecSeqNum)

			// update server state.
			sm.nextExecSeqNum++
			if sm.nextExecSeqNum > sm.nextAllocSeqNum {
				// although each server executes the decided ops independently,
				// a server may see ops proposed by other servers.
				// if the server proposes an op with a stale seq num, than the op would never get
				// decided.
				// hence, we need to update the seq num so that the server has more chance to
				// allocate a large-enough seq num to let the op get decided.
				sm.nextAllocSeqNum = sm.nextExecSeqNum
			}

			// println("S%v state (ASN=%v ESN=%v C=%v RId=%v EId=%v)", sm.me, sm.nextAllocSeqNum, sm.nextExecSeqNum, op.ClerkId, sm.maxPropOpIdOfClerk[op.ClerkId], sm.maxApplyOpIdOfClerk[op.ClerkId])

		} else {
			sm.hasNewDecidedOp.Wait()
		}
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) maybeApplyClientOp(op *Op) {
	// apply the client op if it's not executed previously and the server is serving the shard.
	if opId, exist := sm.maxApplyOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.applyClientOp(op)

		// update the max applied op for each clerk to implement the at-most-once semantics.
		sm.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId

		println("S%v applied client op (C=%v Id=%v) at N=%v", sm.me, op.ClerkId, op.OpId, sm.nextExecSeqNum)

		// fmt.Printf("ShardMaster's latest config:\n")
		// PrintGidToShards(&sm.configs[len(sm.configs)-1], true)
	}
}

func (sm *ShardMaster) applyClientOp(op *Op) {
	switch op.OpType {
	case Join:
		println("S%v is about to executing Op Join (GID=%v Servers=%v)", sm.me, op.GID, op.Servers)

		currConfig := sm.configs[len(sm.configs)-1]
		if _, exist := currConfig.Groups[op.GID]; exist {
			// do not execute the op if trying to join an existing group.
			println("S%v skips the Op Join (GID=%v Servers=%v)", sm.me, op.GID, op.Servers)
			return
		}

		// create a new config with the addition of the joined replica group.
		newConfig := currConfig.clonedWithIncNum()
		newConfig.Groups[op.GID] = op.Servers

		if len(newConfig.Groups) == 1 {
			// this is the very first join, assign all shards to the only group.
			for shard := range newConfig.Shards {
				newConfig.Shards[shard] = op.GID
			}
			println("Assign all shards to G%v", op.GID)
			PrintGidToShards(&newConfig, DEBUG)

		} else {
			// rebalance shards on replica groups.
			rebalanceShards(&newConfig, true, op.GID)
		}

		sm.configs = append(sm.configs, newConfig)

	case Leave:
		println("S%v is about to executing Op Leave (GID=%v)", sm.me, op.GID)

		currConfig := sm.configs[len(sm.configs)-1]
		if _, exist := currConfig.Groups[op.GID]; !exist {
			// do not execute the op if trying to leave an non-existing group.
			println("S%v skips the Op Leave (GID=%v)", sm.me, op.GID)
			return
		}

		// create a new config with the removal of the leaved replica group.
		newConfig := currConfig.clonedWithIncNum()
		delete(newConfig.Groups, op.GID)

		// warning: we assume Leave won't leave no groups, and hence no need to assign shards to the invalid gid 0.

		// rebalance shards on replica groups.
		rebalanceShards(&newConfig, false, op.GID)

		sm.configs = append(sm.configs, newConfig)

	case Move:
		println("S%v is about to executing Op Move (GID=%v Shard=%v)", sm.me, op.GID, op.Shard)

		currConfig := sm.configs[len(sm.configs)-1]
		if _, exist := currConfig.Groups[op.GID]; !exist {
			// do not execute the op if trying to assign a shard to a non-existing group.
			println("S%v skips the Op Move (GID=%v Shard=%v)", sm.me, op.GID, op.Shard)
			return
		}

		// on the current config?

		// create a new config.
		newConfig := currConfig.clonedWithIncNum()
		// reassign the shard.
		newConfig.Shards[op.Shard] = op.GID

		PrintGidToShards(&newConfig, DEBUG)

		sm.configs = append(sm.configs, newConfig)

	case Query:
		// we choose not to execute query op at the executor thread.
		// the executor and the Query handler live in different concurrent threads,
		// therefore, the queried config may be out-of-date when the control flow
		// backs to the Query handler.

	default:
		log.Fatalf("unexpected op type %v", op.OpType)
	}
}

func (sm *ShardMaster) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (sm *ShardMaster) noOpTicker() {
	for !sm.isdead() {
		op := &Op{OpType: "NoOp"}
		go sm.propose(op)

		time.Sleep(proposeNoOpInterval)
	}
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
	sm.maxPropOpIdOfClerk = make(map[int64]int)
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
