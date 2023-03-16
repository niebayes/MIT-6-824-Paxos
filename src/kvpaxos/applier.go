package kvpaxos

import (
	"log"
	"time"
)

func (kv *KVPaxos) executor() {
	kv.mu.Lock()
	for !kv.isdead() {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			if kv.isNoOp(&op) {
				// skip no-ops.

			} else {
				kv.maybeApplyClientOp(&op)
			}

			// tell the paxos peer that this op is done and free server state.
			// if an op is not applied this time, it will never get applied.
			// however, a new op constructed from the same request is allowed to get applied in future.
			// therefore, this delete is safe.
			kv.px.Done(kv.nextExecSeqNum)

			// free server state.
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

		} else {
			kv.hasNewDecidedOp.Wait()
		}
	}
	kv.mu.Unlock()
}

func (kv *KVPaxos) maybeApplyClientOp(op *Op) {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		kv.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId

		println("S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
	}
}

func (kv *KVPaxos) applyClientOp(op *Op) {
	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		kv.db[op.Key] = op.Value

	case "Append":
		// note: the default value is returned if the key does not exist.
		kv.db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
}

func (kv *KVPaxos) waitUntilAppliedOrTimeout(op *Op) (bool, string) {
	var value string = ""
	startTime := time.Now()

	for time.Since(startTime) < maxWaitTime {
		kv.mu.Lock()

		if kv.isApplied(op) {
			value = kv.db[op.Key]
			kv.mu.Unlock()
			return true, value
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	return false, value
}

func (kv *KVPaxos) isApplied(op *Op) bool {
	maxApplyOpId, exist := kv.maxApplyOpIdOfClerk[op.ClerkId]
	return exist && maxApplyOpId >= op.OpId
}
