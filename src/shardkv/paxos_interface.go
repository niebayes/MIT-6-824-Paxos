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

func (kv *ShardKV) executor() {
	kv.mu.Lock()
	for !kv.isdead() {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			// try to apply the decided op on the server.
			kv.maybeApplyOp(&op)

			if op.OpType != "ConfigChange" {
				if opId, exist := kv.maxPropOpIdOfClerk[op.ClerkId]; !exist || opId < op.OpId {
					// update the max proposed op id the server has ever seen to support
					// dup checking and filtering.
					kv.maxPropOpIdOfClerk[op.ClerkId] = op.OpId
				}
			}

			// tell the paxos peer that this op is done and free server state.
			// if an op is not applied this time, it will never get applied.
			// however, a new op constructed from the same request is allowed to get applied in future.
			// therefore, this delete is safe.
			kv.px.Done(kv.nextExecSeqNum)
			delete(kv.decidedOps, kv.nextExecSeqNum)

			// update server state.
			kv.nextExecSeqNum++
			if kv.nextExecSeqNum > kv.nextAllocSeqNum {
				// although each server executes the decided ops independently,
				// a server may see ops proposed by other servers.
				// if the server proposes an op with a stale seq num, than the op would never get
				// decided.
				// hence, we need to update the seq num so that the server has more chance to
				// allocate a large-enough seq num to let the op get decided.
				kv.nextAllocSeqNum = kv.nextExecSeqNum
			}

			println("S%v state (ASN=%v ESN=%v CN=%v C=%v RId=%v EId=%v)", kv.me, kv.nextAllocSeqNum, kv.nextExecSeqNum, kv.config.Num, op.ClerkId, kv.maxPropOpIdOfClerk[op.ClerkId], kv.maxApplyOpIdOfClerk[op.ClerkId])

		} else {
			kv.hasNewDecidedOp.Wait()
		}
	}
	kv.mu.Unlock()
}
