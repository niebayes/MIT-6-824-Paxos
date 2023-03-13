package shardkv

import "6.824/src/shardmaster"

// ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	// a config change is performed only if there's no pending config change.
	// config change has to be performed one by one and hence if we've proposed a config change op
	// previously and it's not installed yet, we'd better not to accept any new config until the pending config is installed.
	// however, we could still accept a new config even if there's a pending config change.
	// it's okay so long as we ensure that a reconfiguring starts only after the previous reconfiguring is completed.
	if kv.waitingToInstallConfig {
		return
	}

	latestConfig := kv.sm.Query(-1)

	kv.mu.Lock()
	if latestConfig.Num > kv.config.Num {
		kv.waitingToInstallConfig = true

		// the new config is installed when the op is applied.
		op := &Op{OpType: "ConfigChange", Config: latestConfig}
		go kv.propose(op)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) installConfig(newConfig shardmaster.Config) {
	// check if there's any change on the served shards.
	servedShardsChanged := false
	for shard := 0; shard < shardmaster.NShards; shard++ {
		currGid := kv.config.Shards[shard]
		newGid := newConfig.Shards[shard]
		if currGid == kv.gid && newGid != kv.gid {
			// move this shard from this group to the group with gid newGid.
			kv.shardDBs[shard].State = MovingOut
			kv.shardDBs[shard].ToGid = newGid
			servedShardsChanged = true
		}
		if currGid != kv.gid && newGid == kv.gid {
			// move this shard from the group with gid currGid to this group.
			kv.shardDBs[shard].State = MovingIn
			kv.shardDBs[shard].FromGid = currGid
			servedShardsChanged = true
		}
	}

	// install the new config.
	kv.config = newConfig

	if !servedShardsChanged {
		// if the served shards do not change, the reconfiguring is done.
		return

	} else {
		// otherwise, the server has to take over moved-in shards or/and hand off moved-out shards.

		// the shard migration is performed in a push-based way, i.e. the initiator of a shard migration
		// is the replica group who is going to handoff shards.
		// on contrary, if performed in a pull-based way, the initiator of the shard migration is
		// the replica group who is going to take over shards. This replica group sends a pull request
		// to another replica group, and then that replica group starts sending shard data to the sender.
		go kv.handoffShards(kv.config.Num)
	}
}

func (kv *ShardKV) isServingKey(key string) bool {
	shard := key2shard(key)
	return kv.shardDBs[shard].State == Serving
}
