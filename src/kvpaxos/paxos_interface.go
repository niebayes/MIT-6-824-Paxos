package kvpaxos

import (
	"time"

	"6.824/src/paxos"
)

func (kv *KVPaxos) allocateSeqNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seqNum := kv.nextAllocSeqNum
	kv.nextAllocSeqNum++
	return seqNum
}

func isSameOp(opX *Op, opY *Op) bool {
	// comparing op types is used to compare two no-ops.
	// comparing clerk id and op id is used to compare two client ops.
	// it's okay the opX and opY that literally are not the same ops as long as they have the same
	// effect when they're executed.
	return opX.OpType == opY.OpType && opX.ClerkId == opY.ClerkId && opX.OpId == opY.OpId
}

func (kv *KVPaxos) propose(op *Op) {
	for !kv.isdead() {
		// choose a sequence number for the op.
		seqNum := kv.allocateSeqNum()

		// starts proposing the op at this sequence number.
		kv.px.Start(seqNum, *op)
		println("S%v starts proposing op (T=%v C=%v Id=%v) at N=%v", kv.me, op.OpType, op.ClerkId, op.OpId, seqNum)

		// wait until the paxos instance with this sequence number is decided.
		decidedOp := kv.waitUntilDecided(seqNum).(Op)
		println("S%v knows op (T=%v C=%v Id=%v) is decided at N=%v", kv.me, decidedOp.OpType, decidedOp.ClerkId, decidedOp.OpId, seqNum)

		// store the decided op.
		kv.mu.Lock()
		kv.decidedOps[seqNum] = decidedOp

		// update server state.
		if opId, exist := kv.maxPropOpIdOfClerk[decidedOp.ClerkId]; !exist || opId < decidedOp.OpId {
			kv.maxPropOpIdOfClerk[decidedOp.ClerkId] = decidedOp.OpId
		}

		// notify the executor thread.
		kv.hasNewDecidedOp.Signal()

		// it's our op chosen as the decided value at sequence number seqNum.
		if isSameOp(op, &decidedOp) {
			// end proposing.
			println("S%v ends proposing (T=%v C=%v Id=%v)", kv.me, decidedOp.OpType, decidedOp.ClerkId, decidedOp.OpId)
			kv.mu.Unlock()
			return
		}
		// another op is chosen as the decided value at sequence number seqNum.
		// retry proposing the op at a different sequence number.
		kv.mu.Unlock()
	}
}

// wait until the paxos instance with sequence number sn decided.
// return the decided value when decided.
func (kv *KVPaxos) waitUntilDecided(seqNum int) interface{} {
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

	// warning: the test suites will call `cleanup` upon termination which will kill the server.
	// it's possible that `waitUntilDecided` exits prior to the termination of `propose`.
	// in such a case, `decidedOp := kv.waitUntilDecided(seqNum).(Op)` would panic since a nil
	// is returned from `waitUntilDecided`.
	// to workaround such an issue, we choose to return a no-op instead of a nil.
	return Op{OpType: "NoOp"}
}

// return true if the op is executed before timeout.
func (kv *KVPaxos) waitUntilAppliedOrTimeout(op *Op) bool {
	startTime := time.Now()
	for time.Since(startTime) < maxWaitTime {
		kv.mu.Lock()
		if opId, exist := kv.maxApplyOpIdOfClerk[op.ClerkId]; exist && opId >= op.OpId {
			kv.mu.Unlock()
			return true
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
