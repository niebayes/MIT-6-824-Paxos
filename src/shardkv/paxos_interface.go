package shardkv

import "time"
import "6.824/src/paxos"

func (kv *ShardKV) allocateSeqNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seqNum := kv.nextAllocSeqNum
	kv.nextAllocSeqNum++
	return seqNum
}

func (kv *ShardKV) propose(op *Op) {
	for !kv.isdead() {
		if !kv.isEligibleToApply(op) {
			break
		}

		seqNum := kv.allocateSeqNum()

		kv.px.Start(seqNum, *op)

		decidedOp := kv.waitUntilDecided(seqNum).(Op)

		kv.mu.Lock()

		kv.decidedOps[seqNum] = decidedOp
		kv.hasNewDecidedOp.Signal()

		if isSameOp(op, &decidedOp) {
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) waitUntilDecided(seqNum int) interface{} {
	lastSleepTime := initSleepTime

	for !kv.isdead() {
		status, decidedValue := kv.px.Status(seqNum)
		if status != paxos.Pending {
			return decidedValue
		}

		sleepTime := lastSleepTime * backoffFactor
		if sleepTime > maxSleepTime {
			sleepTime = maxSleepTime
		}
		time.Sleep(sleepTime)
		lastSleepTime = sleepTime
	}

	return Op{OpType: "NoOp"}
}
