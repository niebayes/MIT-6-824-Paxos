package shardkv

import (
	"log"
	"time"
)

func (kv *ShardKV) executor() {
	kv.mu.Lock()
	for !kv.isdead() {
		op, decided := kv.decidedOps[kv.nextExecSeqNum]
		if decided {
			if kv.isNoOp(&op) {
				// skip no-ops.

			} else if kv.isAdminOp(&op) {
				kv.maybeApplyAdminOp(&op)

			} else {
				kv.maybeApplyClientOp(&op)
			}

			kv.px.Done(kv.nextExecSeqNum)
			delete(kv.decidedOps, kv.nextExecSeqNum)

			kv.nextExecSeqNum++
			if kv.nextExecSeqNum > kv.nextAllocSeqNum {
				kv.nextAllocSeqNum = kv.nextExecSeqNum
			}

		} else {
			kv.hasNewDecidedOp.Wait()
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) maybeApplyAdminOp(op *Op) {
	switch op.OpType {
	case "InstallConfig":
		if (kv.reconfigureToConfigNum == op.Config.Num || op.Config.Num == kv.config.Num+1) && !kv.isMigrating() {
			kv.reconfigureToConfigNum = op.Config.Num
			kv.installConfig(op.Config)
		}

	case "InstallShard":
		if kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].state == MovingIn {
			kv.installShard(op)
		}

	case "DeleteShard":
		if kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].state == MovingOut {
			kv.deleteShard(op)
		}

	default:
		log.Fatalf("unexpected admin op type %v", op.OpType)
	}
}

func (kv *ShardKV) maybeApplyClientOp(op *Op) {
	if !kv.isApplied(op) && kv.isServingKey(op.Key) {
		kv.applyClientOp(op)
		kv.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId
	}
}

func (kv *ShardKV) applyClientOp(op *Op) {
	// the write is applied on the corresponding shard.
	shard := key2shard(op.Key)
	db := kv.shardDBs[shard].dB

	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		db[op.Key] = op.Value

	case "Append":
		db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
}

func (kv *ShardKV) isEligibleToApply(op *Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	eligible := false

	if kv.isNoOp(op) {
		eligible = true

	} else if kv.isAdminOp(op) {
		switch op.OpType {
		case "InstallConfig":
			eligible = (kv.reconfigureToConfigNum == op.Config.Num || op.Config.Num == kv.config.Num+1) && !kv.isMigrating()

		case "InstallShard":
			eligible = kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].state == MovingIn

		case "DeleteShard":
			eligible = kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].state == MovingOut

		default:
			log.Fatalf("unexpected admin op type %v", op.OpType)
		}

	} else {
		// client op.
		eligible = !kv.isApplied(op) && kv.isServingKey(op.Key)
	}

	return eligible
}

func (kv *ShardKV) waitUntilAppliedOrTimeout(op *Op) (bool, string) {
	var value string = ""
	startTime := time.Now()

	for time.Since(startTime) < maxWaitTime {
		kv.mu.Lock()

		if kv.isApplied(op) && kv.isServingKey(op.Key) {
			if op.OpType == "Get" {
				value = kv.shardDBs[key2shard(op.Key)].dB[op.Key]
			}

			kv.mu.Unlock()
			return true, value
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	return false, value
}

func (kv *ShardKV) isApplied(op *Op) bool {
	maxApplyOpId, exist := kv.maxApplyOpIdOfClerk[op.ClerkId]
	return exist && maxApplyOpId >= op.OpId
}
