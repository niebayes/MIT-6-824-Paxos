package shardkv

import "6.824/src/shardmaster"

func (kv *ShardKV) tick() {
	kv.mu.Lock()
	nextConfigNum := kv.config.Num + 1
	kv.mu.Unlock()

	nextConfig := kv.sm.Query(nextConfigNum)

	kv.mu.Lock()

	if kv.reconfigureToConfigNum == -1 && nextConfig.Num == kv.config.Num+1 {
		kv.reconfigureToConfigNum = nextConfig.Num

		op := &Op{OpType: "InstallConfig", Config: nextConfig}
		go kv.propose(op)
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) installConfig(nextConfig shardmaster.Config) {
	hasShardsToHandOff := false
	hasShardsToTakeOver := false

	for shard := 0; shard < shardmaster.NShards; shard++ {
		currGid := kv.config.Shards[shard]
		newGid := nextConfig.Shards[shard]

		if currGid == 0 && newGid == kv.gid {
			// nothing to move in if the from group is the invalid group 0.
			kv.shardDBs[shard].state = Serving
			continue
		}

		if newGid == 0 {
			// noting to move out if the to group is the invalid group 0.
			kv.shardDBs[shard].state = NotServing
			continue
		}

		if currGid == kv.gid && newGid != kv.gid {
			// move this shard from this group to the group with gid newGid.
			kv.shardDBs[shard].state = MovingOut
			kv.shardDBs[shard].toGid = newGid
			hasShardsToHandOff = true
		}
		if currGid != kv.gid && newGid == kv.gid {
			// move this shard from the group with gid currGid to this group.
			kv.shardDBs[shard].state = MovingIn
			kv.shardDBs[shard].fromGid = currGid
			hasShardsToTakeOver = true
		}
	}

	kv.config = nextConfig

	if !hasShardsToHandOff && !hasShardsToTakeOver {
		kv.reconfigureToConfigNum = -1
	}
}

func (kv *ShardKV) isServingKey(key string) bool {
	shard := key2shard(key)
	return kv.shardDBs[shard].state == Serving
}
