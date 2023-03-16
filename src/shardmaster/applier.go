package shardmaster

import (
	"log"
	"time"
)

func (sm *ShardMaster) executor() {
	sm.mu.Lock()
	for !sm.isdead() {
		op, decided := sm.decidedOps[sm.nextExecSeqNum]
		if decided {
			if sm.isNoOp(&op) {
				// skip no-ops.

			} else {
				sm.maybeApplyClientOp(&op)
			}

			// tell the paxos peer that this op is done and free server state.
			// if an op is not applied this time, it will never get applied.
			// however, a new op constructed from the same request is allowed to get applied in future.
			// therefore, this delete is safe.
			sm.px.Done(sm.nextExecSeqNum)

			// free server state.
			delete(sm.decidedOps, sm.nextExecSeqNum)

			// update server state.
			sm.nextExecSeqNum++
			if sm.nextExecSeqNum > sm.nextAllocSeqNum {
				// although each server executes the decided ops independently,
				// a server may see ops proposed by other servers.
				// if the server proposes an op with a stale seq num, than the op would never get
				// decided.
				// hence, we need to update the seq num so that the server has more chance to
				// allocate a large-enough seq num to let the op get decided.
				sm.nextAllocSeqNum = sm.nextExecSeqNum
			}

		} else {
			sm.hasNewDecidedOp.Wait()
		}
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) maybeApplyClientOp(op *Op) {
	if !sm.isApplied(op) {
		sm.applyClientOp(op)
		sm.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId

		println("S%v applied client op (C=%v Id=%v) at N=%v", sm.me, op.ClerkId, op.OpId, sm.nextExecSeqNum)
	}
}

func (sm *ShardMaster) applyClientOp(op *Op) {
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
			PrintGidToShards(&newConfig, DEBUG)

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

		// create a new config.
		newConfig := currConfig.clonedWithIncNum()
		// reassign the shard.
		newConfig.Shards[op.Shard] = op.GID

		PrintGidToShards(&newConfig, DEBUG)

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

// return true if the op is executed before timeout.
func (sm *ShardMaster) waitUntilAppliedOrTimeout(op *Op) (bool, Config) {
	config := Config{Num: 0}
	startTime := time.Now()

	for time.Since(startTime) < maxWaitTime {
		sm.mu.Lock()

		if sm.isApplied(op) {
			latestConfig := sm.configs[len(sm.configs)-1]
			if op.ConfigNum == -1 || op.ConfigNum > latestConfig.Num {
				config = latestConfig
			} else {
				config = sm.configs[op.ConfigNum]
			}

			sm.mu.Unlock()
			return true, config
		}

		sm.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	return false, config
}

func (sm *ShardMaster) isApplied(op *Op) bool {
	maxApplyOpId, exist := sm.maxApplyOpIdOfClerk[op.ClerkId]
	return exist && maxApplyOpId >= op.OpId
}
