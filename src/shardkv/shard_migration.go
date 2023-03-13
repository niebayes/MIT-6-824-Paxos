package shardkv

import "time"
import "6.824/src/shardmaster"

type InstallShardArgs struct {
	// besides the shard database, the FromGid and ToGid are also required
	// to be conveyed in the args.
	// they are used by the receiver to check if the shard migration is correct.
	ShardDB ShardDB
	// client state to be synced with the receiver to support dup checking and filtering
	// and to implement the at-most-once semantics.
	maxPropOpIdOfClerk  map[int64]int
	MaxApplyOpIdOfClerk map[int64]int
}

type InstallShardReply struct {
	Err    Err
	Config shardmaster.Config
}

func (kv *ShardKV) handoffShards(configNum int) {
	for !kv.isdead() {
		kv.mu.Lock()

		// each reconfiguring will incur a `handoffShards` if there're shards required to migrate.
		// put another way, the `handoffShards` is associated with this reconfiguring.
		// therefore, this `handoffShards` keeps running only if being reconfiguring
		// to the config with the config number configNum.
		if kv.config.Num != configNum {
			kv.mu.Unlock()
			break
		}

		movingOutShards := false
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.shardDBs[shard].State == MovingOut {
				movingOutShards = true

				// we could send to a group all shards it needs in one message,
				// however, considering the data of all shards may be way too large
				// and the network is unreliable, there're much overhead for resending
				// all shard data.
				// hence, we choose to send one shard in one message.
				kv.sendShard(shard)
			}
		}

		kv.mu.Unlock()

		if !movingOutShards {
			break
		}

		time.Sleep(handoffShardsInterval)
	}
}

func (kv *ShardKV) sendShard(shard int) {}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) error {
	kv.mu.Lock()

	kv.mu.Unlock()

	return nil
}

func (kv *ShardKV) isMigrating() bool {
	migrating := false
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.shardDBs[shard].State == MovingIn || kv.shardDBs[shard].State == MovingOut {
			migrating = true
			break
		}
	}
	return migrating
}
