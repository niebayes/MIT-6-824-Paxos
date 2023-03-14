package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sort"
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
	OpType    OpType   // Join, Leave, Move, Query.
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

func (sm *ShardMaster) allocateSeqNum() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	seqNum := sm.nextAllocSeqNum
	sm.nextAllocSeqNum++
	return seqNum
}

// wait until the paxos instance with sequence number seqNum decided.
// return the decided value when decided.
func (sm *ShardMaster) waitUntilDecided(seqNum int) interface{} {
	lastSleepTime := initSleepTime
	for !sm.isdead() {
		status, decidedValue := sm.px.Status(seqNum)
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

type GidAndShards struct {
	gid    int64
	shards []int
}

type Movement struct {
	from   int64 // from group.
	to     int64 // to group.
	shards []int // moved shards.
}

func printGidToShards(config *Config) {
	if !DEBUG {
		return
	}

	gidToShards := make(map[int64][]int)
	for shard, gid := range config.Shards {
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = make([]int, 0)
		}
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// add mappings for groups with empty shards.
	for gid := range config.Groups {
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = make([]int, 0)
		}
	}

	// convert map to slice to ensure the printing is in order.
	gidAndShardsArray := make([]GidAndShards, 0)
	for gid, shards := range gidToShards {
		gidAndShardsArray = append(gidAndShardsArray, GidAndShards{gid: gid, shards: shards})
	}

	// sort the array by group id in ascending order.
	sort.Slice(gidAndShardsArray, func(i, j int) bool { return gidAndShardsArray[i].gid < gidAndShardsArray[j].gid })

	for _, gidAndShards := range gidAndShardsArray {
		println("G%v: %v", gidAndShards.gid, gidAndShards.shards)
	}
}

// note: everything is pass-by-value in Go. However, the passed-by slice is a header which points
// to the backing array and any modification on the copied slice header will be made on the backing array.
func rebalanceShards(config *Config, isJoin bool, movedGid int64) {
	println("####################\nBefore rebalaning:")
	printGidToShards(config)

	gidToShards := make(map[int64][]int)
	for shard, gid := range config.Shards {
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = make([]int, 0)
		}
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// add mappings for groups with empty shards.
	for gid := range config.Groups {
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = make([]int, 0)
		}
	}

	gidAndShardsArray := make([]GidAndShards, 0)
	for gid, shards := range gidToShards {
		gidAndShardsArray = append(gidAndShardsArray, GidAndShards{gid: gid, shards: shards})
	}

	// sort the array by the number of shards in descending order.
	// note: a stable sort is not needed, since only the #shards matters.
	sort.Slice(gidAndShardsArray, func(i, j int) bool { return len(gidAndShardsArray[i].shards) > len(gidAndShardsArray[j].shards) })

	// compute the expected number of shards of each group after the rebalancing.
	numGroups := len(config.Groups)   // the Groups must have been added or removed the group.
	base := NShards / numGroups       // any group could get at least base shards.
	totalBonus := NShards % numGroups // only some groups could get bonus shards.

	if isJoin {
		// append an element for the new group so that the new group could be iterated
		gidAndShardsArray = append(gidAndShardsArray, GidAndShards{gid: movedGid, shards: make([]int, 0)})
	}
	expectedNumShardsOfGroup := make(map[int64]int)
	for _, gidAndShards := range gidAndShardsArray {
		if !isJoin && gidAndShards.gid == movedGid {
			// the leaved group has no shards after the rebalancing.
			expectedNumShardsOfGroup[movedGid] = 0
			continue
		}

		expectedNumShards := base
		if totalBonus > 0 {
			expectedNumShards += 1
			totalBonus -= 1
		}
		expectedNumShardsOfGroup[gidAndShards.gid] = expectedNumShards
	}

	from := make([]GidAndShards, 0) // shards from which group need to be moved out.
	to := make([]GidAndShards, 0)   // to which group shards need to be moved in.
	for i, gidAndShards := range gidAndShardsArray {
		gid := gidAndShards.gid
		currShards := gidAndShards.shards
		expectedNumShards := expectedNumShardsOfGroup[gid]
		diff := len(currShards) - expectedNumShards
		if diff > 0 {
			// for a group, if its current number of shards is greater than the expected number of shards after rebalancing,
			// then it shall give out the overflowed shards.

			// select the last diff shards as the overflowed shards.
			// note: many info, for e.g. the time the group serves the shard, the shard size, etc., could
			// be utilized to devise a better algorithm to select the overflowed shards. For now, the selection
			// algorithm is trivial.
			remainingShards := currShards[:expectedNumShards]
			movedOutShards := currShards[expectedNumShards:]
			from = append(from, GidAndShards{gid: gid, shards: movedOutShards})
			gidAndShardsArray[i].shards = remainingShards
		} else if diff < 0 {
			// for a group, if its current number of shards is less than the expected number of shards,
			// then some other groups shall hand off their overflowed shards to the group.

			// only the number of shards matters.
			to = append(to, GidAndShards{gid: gid, shards: make([]int, -diff)})
		}
	}

	// hand off overflowed shards to groups who need shards.
	movements := make([]Movement, 0)
	for i, toGidAndShards := range to {
		totalNeededNumShards := len(toGidAndShards.shards)
		cursor := 0

		for j, fromGidAndShards := range from {
			// no need to check if neededNumShards > 0 since the number of moved-in shards
			// must be equal to the number of moved-out shards.
			neededNumShards := totalNeededNumShards - cursor
			numMovedOutShards := min(len(fromGidAndShards.shards), neededNumShards)
			if numMovedOutShards <= 0 {
				// all shards are moved out, skip this group.
				continue
			}
			movedOutShards := fromGidAndShards.shards[:numMovedOutShards]

			if numMovedOutShards >= len(fromGidAndShards.shards) {
				from[j].shards = make([]int, 0)
			} else {
				from[j].shards = fromGidAndShards.shards[numMovedOutShards:]
			}

			for k := 0; k < numMovedOutShards; k++ {
				toGidAndShards.shards[cursor+k] = movedOutShards[k]
			}
			cursor += numMovedOutShards

			// record the movement.
			movements = append(movements, Movement{from: fromGidAndShards.gid, to: toGidAndShards.gid, shards: movedOutShards})
		}

		to[i] = toGidAndShards
	}

	println("Movements:")
	for _, movement := range movements {
		for _, shard := range movement.shards {
			config.Shards[shard] = movement.to
		}
		println("G%v -> G%v: %v", movement.from, movement.to, movement.shards)
	}

	println("After rebalaning:")
	printGidToShards(config)
	println("####################")
}

func (sm *ShardMaster) executeOp(op *Op) {
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
			printGidToShards(&newConfig)

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

		printGidToShards(&newConfig)

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

func (sm *ShardMaster) executor() {
	sm.mu.Lock()
	for !sm.isdead() {
		op, decided := sm.decidedOps[sm.nextExecSeqNum]
		if decided {
			// execute the decided op if it is not executed yet.
			// this ensures the same op won't be execute more than once by a server.
			// the same op might be executed more than once if different servers proposes the same
			// request at different sequence numbers and just happens that they are all decided.
			// there's no way to avoid such case since the paxos has the ability to decide multiple values
			// at the same time.
			if opId, exist := sm.maxExecOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
				sm.executeOp(&op)
				println("S%v executes op (C=%v Id=%v) at N=%v", sm.me, op.ClerkId, op.OpId, sm.nextExecSeqNum)
			}

			// tell the paxos peer that this op is done.
			sm.px.Done(sm.nextExecSeqNum)

			// free server state.
			delete(sm.decidedOps, sm.nextExecSeqNum)

			// update server state.
			sm.nextExecSeqNum++
			if sm.nextExecSeqNum > sm.nextAllocSeqNum {
				sm.nextAllocSeqNum = sm.nextExecSeqNum
			}
			if opId, exist := sm.maxExecOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
				sm.maxExecOpIdOfClerk[op.ClerkId] = op.OpId
			}
			if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
				sm.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
			}

			println("S%v state (ASN=%v ESN=%v C=%v RId=%v EId=%v)", sm.me, sm.nextAllocSeqNum, sm.nextExecSeqNum, op.ClerkId, sm.maxRecvOpIdFromClerk[op.ClerkId], sm.maxExecOpIdOfClerk[op.ClerkId])

		} else {
			sm.hasNewDecidedOp.Wait()
		}
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) propose(op *Op) {
	for !sm.isdead() {
		// choose a sequence number for the op.
		seqNum := sm.allocateSeqNum()

		// starts proposing the op at this sequence number.
		sm.px.Start(seqNum, *op)
		println("S%v starts proposing op (C=%v Id=%v) at N=%v", sm.me, op.ClerkId, op.OpId, seqNum)

		// wait until the paxos instance with this sequence number is decided.
		decidedOp := sm.waitUntilDecided(seqNum).(Op)
		println("S%v knows op (C=%v Id=%v) is decided at N=%v", sm.me, decidedOp.ClerkId, decidedOp.OpId, seqNum)

		// store the decided op.
		sm.mu.Lock()
		sm.decidedOps[seqNum] = decidedOp

		// update server state.
		if opId, exist := sm.maxRecvOpIdFromClerk[decidedOp.ClerkId]; !exist || opId < decidedOp.OpId {
			sm.maxRecvOpIdFromClerk[decidedOp.ClerkId] = decidedOp.OpId
		}

		// notify the executor thread.
		sm.hasNewDecidedOp.Signal()

		// it's our op chosen as the decided value at sequence number seqNum.
		if decidedOp.ClerkId == op.ClerkId && decidedOp.OpId == op.OpId {
			// end proposing.
			println("S%v ends proposing (C=%v Id=%v)", sm.me, decidedOp.ClerkId, decidedOp.OpId)
			sm.mu.Unlock()
			return
		}
		// another op is chosen as the decided value at sequence number seqNum.
		// retry proposing the op at a different sequence number.
		sm.mu.Unlock()
	}
}

// return true if the op is executed before timeout.
func (sm *ShardMaster) waitUntilExecutedOrTimeout(op *Op) bool {
	startTime := time.Now()
	for time.Since(startTime) < maxWaitTime {
		sm.mu.Lock()
		if opId, exist := sm.maxExecOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
			sm.mu.Unlock()
			return true
		}
		sm.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	return false
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
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Join (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilExecutedOrTimeout(op) {
			println("S%v replies Join (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
			reply.Err = OK

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilExecutedOrTimeout(op) {
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
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Leave (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilExecutedOrTimeout(op) {
			println("S%v replies Leave (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
			reply.Err = OK

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilExecutedOrTimeout(op) {
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
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Move (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilExecutedOrTimeout(op) {
			println("S%v replies Move (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
			reply.Err = OK

		} else {
			reply.Err = ErrNotExecuted
		}
		return nil
	}
	// not a dup request.

	// update server state.
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilExecutedOrTimeout(op) {
		println("S%v replies Move (C=%v Id=%v)", sm.me, op.ClerkId, op.OpId)
		reply.Err = OK

	} else {
		reply.Err = ErrNotExecuted
	}

	return nil
}

// TODO: separate paxos stub codes out.
// TODO: update paxos inferface, e.g. max recv to max prop, etc.

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()

	println("S%v receives Query (C=%v Id=%v)", sm.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: Query, ConfigNum: args.Num}

	// check if this is a dup request.
	isDup := false
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; exist && opId >= op.OpId {
		println("S%v knows Query (C=%v Id=%v) is dup", sm.me, op.ClerkId, op.OpId)
		isDup = true
	}

	if isDup {
		sm.mu.Unlock()
		if sm.waitUntilExecutedOrTimeout(op) {
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
	if opId, exist := sm.maxRecvOpIdFromClerk[op.ClerkId]; !exist || opId < op.OpId {
		sm.maxRecvOpIdFromClerk[op.ClerkId] = op.OpId
	}
	sm.mu.Unlock()

	// start proposing the op.
	go sm.propose(op)

	// wait until the op is executed or timeout.
	if sm.waitUntilExecutedOrTimeout(op) {
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
	sm.maxRecvOpIdFromClerk = make(map[int64]int)
	sm.maxExecOpIdOfClerk = make(map[int64]int)
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

	// please do not change any of the following code,
	// or do anything to subvert it.

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
