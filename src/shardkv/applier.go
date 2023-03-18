package shardkv

import (
	"log"
	"time"
)

// the reason why the thread is called `executor` instead of `applier` is that
// all decided ops will be executed but not all ops will be applied.
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
				// note: this is not necessary.
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
		// there're two sources of an install config op:
		// (1) it's proposed by this server.
		// (2) it's proposed by another server in the same replica group.
		// if it's (1), `reconfigureToConfigNum` == `op.Config.Num`.
		// if it's (2), `reconfigureToConfigNum` may still be -1 while the `kv.config.Num + 1` == `op.Config.Num`.
		// in either cases, we need to install the config.
		// besires, this install config op gets installed only if the server is not migrating shards.

		if (kv.reconfigureToConfigNum == op.Config.Num || op.Config.Num == kv.config.Num+1) && !kv.isMigrating() {
			kv.reconfigureToConfigNum = op.Config.Num
			kv.installConfig(op.Config)

			println("S%v-%v installed config (ACN=%v)", kv.gid, kv.me, kv.config.Num)
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
		kv.maxAppliedOpIdOfClerk[op.ClerkId] = op.OpId

		println("S%v-%v applied client op at config (CN=%v) (C=%v Id=%v) at N=%v", kv.gid, kv.me, kv.config.Num, op.ClerkId, op.OpId, kv.nextExecSeqNum)
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
		// note: the default value is returned if the key does not exist.
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
	maxAppliedOpId, exist := kv.maxAppliedOpIdOfClerk[op.ClerkId]
	return exist && maxAppliedOpId >= op.OpId
}
