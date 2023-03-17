package shardkv

import "log"
import "time"

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
		// if it's (1), the `reconfiguring` was set to true by this server.
		// if it's (2), the `reconfiguring` may be false yet.
		// no matter of which cases, the install config gets installed if its
		// config num is one larger than the server's current config num and
		// the server is not migration shards.
		//
		// note: the shard migration state checking is not necessary if there's no server crash.
		//
		// consider the scenario (a):
		// the server's current config is X and it's migrating shards for config X.
		// another server in the same replica group has proposed an install config op
		// for config X since it learns the config X from the shardmaster but has not installed it yet.
		// if there's no one-larger config num checking, the config X would be installed again.
		// since the configs are identical, served shards would no change and hence `reconfiguring`
		// will be set to false which stops the shard migration for config X.
		//
		// consider the scenario (b):
		// the server's current config is X and it's migrating shards for config X.
		// another server in the same replica group has done migrating shards for config X,
		// and it has proposed the config X+1 since it learns the config from the shardmaster.
		// further assume such an `another server` has done reconfiguring for config X+1 and
		// it has proposed the config X+2.
		// further assume we've used the migration state checking, then it's possible that
		// the config X+1 is discarded by this server since this server is migrating shards
		// for config X.
		// however, happens, the time the install config op for config X+2 arrives is being executed
		// the shard migration for config X is done and hence the install config op for config X+2
		// is going to be installed.
		//
		// consider the scenario (c):
		// the server's current config is X and it's migrating shards for config X.
		// another server in the same replica group has done migrating shards for config X,
		// and it proposes the config X+1 since it learns the config from the shardmaster.
		// if no checking on migration state, the config X+1 would be installed and the shard
		// migration for config X would stop.
		// if the shard migration for config X is moving out shards, a server is allowed to
		// propose the config X+1 only if a server of the receiver replica group has proposed
		// an install shard op for those moving shards.
		// if there's server crash, the receiver replica group may lose those shards since
		// it's possible that all servers in the sender replica groups have installed the
		// config X+1 which stops the shard migration for config X.
		// hence, such an early stop for a shard migration is only okay if there's no server crash.
		// since lab4 does not involve server crash, the checking for migration state is not necessary.
		//
		// in summary, the canonical codes are the following whether there's server crash or not.
		// if (kv.reconfigureToConfigNum == op.Config.Num || op.Config.Num == kv.config.Num+1) && !kv.isMigrating() {
		// 	kv.reconfigureToConfigNum = op.Config.Num
		// 	kv.installConfig(op.Config)
		// }

		if op.Config.Num == kv.config.Num+1 {
			kv.reconfiguring = true
			kv.installConfig(op.Config)
		}

	case "InstallShard":
		// install the shard if it's not installed yet.
		//
		// note: simply checking shard state == MovingIn is not necessary.
		// consider such a scenario:
		// the server's current config is X and it's waiting to take over shards for config X.
		// another server in the same replica group has reconfigured to config X+2.
		// this is possible since only a majority of servers is necessary to push paxos towards.
		//
		// in config X+1, the shard is moved out to another replica group.
		// in config X+2, the shard is agian moved in to this replica group.
		// all client ops of the shard sent to this replica group would not be applied
		// until the complete of the reconfiguting to config X+2.
		//
		// assume all install config ops for config X+1 and config X+2 are discarded
		// since this server is waiting to take over shards for config X.
		// further assume it happens that the time the install shard op for config X+2 arrives
		// this server is still waiting to take over the shard, then the install shard op
		// would be installed.
		//
		// this means although this server is reconfiguring to config X, it has installed
		// a shard of config X+2.
		// it's possible that when the shard is served by another replica group in config X+1,
		// some requests are applied on the shard.
		// when this server completes the reconfiguring to config X, it needs to send the shard
		// to another replica group.
		//
		// this means some servers in such an `another replica group` would install a shard on
		// which some client ops that shall be applied after the installaton of the shard
		// were applied already.
		// this violates the at-most-once semantics.
		//
		// however, simply applying the migration state checking is able to make the
		// program pass the tests for 500 times without any error.
		//
		// for the sake of safety, we choose to add the config num checking to work around the issue
		// we mentioned above.

		if kv.shardDBs[op.Shard].state == MovingIn {
			kv.installShard(op)
		}

	default:
		log.Fatalf("unexpected admin op type %v", op.OpType)
	}
}

// FIXME: leave a bug: a client op is only applied by one server in a replica group.
// maybe fix the bug by reverting to use reconfiguring instead of reconfigureToConfigNum.
func (kv *ShardKV) maybeApplyClientOp(op *Op) {
	if !kv.isApplied(op) && kv.isServingKey(op.Key) {
		kv.applyClientOp(op)
		kv.maxApplyOpIdOfClerk[op.ClerkId] = op.OpId

		println("S%v-%v applied client op (C=%v Id=%v) at N=%v", kv.gid, kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
	} else {
		if kv.isApplied(op) {
			println("S%v-%v discards client op due to already applied (C=%v Id=%v) at N=%v", kv.gid, kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
		}
		if !kv.isServingKey(op.Key) {
			println("S%v-%v discards client op due to not serving (C=%v Id=%v) at N=%v", kv.gid, kv.me, op.ClerkId, op.OpId, kv.nextExecSeqNum)
		}
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
		println("S%v-%v appens %v to %v", kv.gid, kv.me, op.Value, db[op.Key])
		db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
}

func (kv *ShardKV) waitUntilAppliedOrTimeout(op *Op) (bool, string) {
	var value string = ""
	startTime := time.Now()

	for time.Since(startTime) < maxWaitTime {
		kv.mu.Lock()

		if kv.isApplied(op) {
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
