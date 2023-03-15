package shardmaster

import (
	"time"

	"6.824/src/paxos"
)

func (sm *ShardMaster) allocateSeqNum() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	seqNum := sm.nextAllocSeqNum
	sm.nextAllocSeqNum++
	return seqNum
}

func isSameOp(opX *Op, opY *Op) bool {
	// comparing op types is used to compare two no-ops.
	// comparing clerk id and op id is used to compare two client ops.
	// it's okay the opX and opY that literally are not the same ops as long as they have the same
	// effect when they're executed.
	return opX.OpType == opY.OpType && opX.ClerkId == opY.ClerkId && opX.OpId == opY.OpId
}

func (sm *ShardMaster) propose(op *Op) {
	for !sm.isdead() {
		// choose a sequence number for the op.
		seqNum := sm.allocateSeqNum()

		// starts proposing the op at this sequence number.
		sm.px.Start(seqNum, *op)
		println("S%v starts proposing op (T=%v C=%v Id=%v) at N=%v", sm.me, op.OpType, op.ClerkId, op.OpId, seqNum)

		// wait until the paxos instance with this sequence number is decided.
		decidedOp := sm.waitUntilDecided(seqNum).(Op)
		println("S%v knows op (T=%v C=%v Id=%v) is decided at N=%v", sm.me, decidedOp.OpType, decidedOp.ClerkId, decidedOp.OpId, seqNum)

		// store the decided op.
		sm.mu.Lock()
		sm.decidedOps[seqNum] = decidedOp

		// update server state.
		if opId, exist := sm.maxPropOpIdOfClerk[decidedOp.ClerkId]; !exist || opId < decidedOp.OpId {
			sm.maxPropOpIdOfClerk[decidedOp.ClerkId] = decidedOp.OpId
		}

		// notify the executor thread.
		sm.hasNewDecidedOp.Signal()

		// it's our op chosen as the decided value at sequence number seqNum.
		if isSameOp(op, &decidedOp) {
			// end proposing.
			println("S%v ends proposing (T=%v C=%v Id=%v)", sm.me, decidedOp.OpType, decidedOp.ClerkId, decidedOp.OpId)
			sm.mu.Unlock()
			return
		}
		// another op is chosen as the decided value at sequence number seqNum.
		// retry proposing the op at a different sequence number.
		sm.mu.Unlock()
	}
}

// wait until the paxos instance with sequence number sn decided.
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

	// warning: the test suites will call `cleanup` upon termination which will kill the server.
	// it's possible that `waitUntilDecided` exits prior to the termination of `propose`.
	// in such a case, `decidedOp := sm.waitUntilDecided(seqNum).(Op)` would panic since a nil
	// is returned from `waitUntilDecided`.
	// to workaround such an issue, we choose to return a no-op instead of a nil.
	return Op{OpType: "NoOp"}
}

// return true if the op is executed before timeout.
func (sm *ShardMaster) waitUntilAppliedOrTimeout(op *Op) bool {
	startTime := time.Now()
	for time.Since(startTime) < maxWaitTime {
		sm.mu.Lock()
		if opId, exist := sm.maxApplyOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
			sm.mu.Unlock()
			return true
		}
		sm.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
