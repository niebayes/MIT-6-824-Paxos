package shardkv

import "time"
import "6.824/src/paxos"

//
// this file contains codes for interacting with paxos.
//

func (kv *ShardKV) allocateSeqNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seqNum := kv.nextAllocSeqNum
	kv.nextAllocSeqNum++
	return seqNum
}

func (kv *ShardKV) propose(op *Op) {
	for !kv.isdead() {
		// choose a sequence number for the op.
		seqNum := kv.allocateSeqNum()

		// starts proposing the op at this sequence number.
		kv.px.Start(seqNum, *op)
		println("S%v starts proposing op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, seqNum)

		// wait until the paxos instance with this sequence number is decided.
		decidedOp := kv.waitUntilDecided(seqNum).(Op)
		println("S%v knows op (C=%v Id=%v) is decided at N=%v", kv.me, decidedOp.ClerkId, decidedOp.OpId, seqNum)

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
		if decidedOp.ClerkId == op.ClerkId && decidedOp.OpId == op.OpId {
			// end proposing.
			println("S%v ends proposing (C=%v Id=%v)", kv.me, decidedOp.ClerkId, decidedOp.OpId)
			kv.mu.Unlock()
			return
		}
		// another op is chosen as the decided value at sequence number seqNum.
		// retry proposing the op at a different sequence number.
		kv.mu.Unlock()
	}
}

// wait until the paxos instance with sequence number seqNum decided.
// return the decided value when decided.
func (kv *ShardKV) waitUntilDecided(seqNum int) interface{} {
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
	return nil
}

func (kv *ShardKV) waitUntilAppliedOrTimeout(op *Op) bool {
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
