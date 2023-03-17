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

// TODO: rewrite propose with a proposer thread.
// make a channel between the service handlers and the proposer thread.
// rewrite executor with a channel from proposer to executor.
// make a channel between the service handlers and the applier.
// let the service handlers waiting on the channel from applier.
// let the service handlers starts a thread to free the channel from applier.
// remains a problem: how to tag a channel to uniquely identify a waiting client? use clerkid and opid is okay? any other? only clerkid?
func (kv *ShardKV) propose(op *Op) {
	for !kv.isdead() {
		// TODO: check if we're still eligible to apply the op.
		// if not, break.
		// only good for throughput.

		// allocate a sequence number for the op.
		seqNum := kv.allocateSeqNum()

		// starts proposing the op at this sequence number.
		kv.px.Start(seqNum, *op)
		println("S%v-%v starts proposing op (T=%v ACN=%v SN=%v C=%v Id=%v) at N=%v", kv.gid, kv.me, op.OpType, op.Config.Num, op.Shard, op.ClerkId, op.OpId, seqNum)

		// wait until the paxos instance with this sequence number is decided.
		decidedOp := kv.waitUntilDecided(seqNum).(Op)
		println("S%v-%v knows op (T=%v ACN=%v C=%v Id=%v) is decided at N=%v", kv.gid, kv.me, decidedOp.OpType, decidedOp.Config.Num, decidedOp.ClerkId, decidedOp.OpId, seqNum)

		// store the decided op.
		kv.mu.Lock()
		kv.decidedOps[seqNum] = decidedOp

		// notify the executor thread.
		kv.hasNewDecidedOp.Signal()

		// it's our op chosen as the decided value at sequence number seqNum.
		if isSameOp(op, &decidedOp) {
			// end proposing.
			println("S%v-%v ends proposing (T=%v ACN=%v C=%v Id=%v)", kv.gid, kv.me, decidedOp.OpType, decidedOp.Config.Num, decidedOp.ClerkId, decidedOp.OpId)
			kv.mu.Unlock()
			return
		}
		// otherwise, it's another op chosen as the decided value at the sequence number seqNum.
		// retry proposing the op at a different sequence number.
		kv.mu.Unlock()
	}
}

// wait until the paxos instance with the sequence number seqNum decided.
// return the decided value when decided.
func (kv *ShardKV) waitUntilDecided(seqNum int) interface{} {
	lastSleepTime := initSleepTime
	for !kv.isdead() {
		status, decidedValue := kv.px.Status(seqNum)
		if status != paxos.Pending {
			// if forgotten, decidedValue will be nil.
			// but this shall not happen since this value is forgotten only after
			// this server has called Done on this value.
			// hence, the returned decidedValue shall not be nil.
			return decidedValue
		}

		// wait a while and retry.
		// this backoff sleeping scheme is not necessary.
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
