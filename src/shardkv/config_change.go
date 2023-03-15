package shardkv

import "6.824/src/shardmaster"

// ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	// note, the config change has to be contiguous.
	// for example, there're three groups A, B, C, and three contiguous configs X, Y, Z and a shard S.
	// config X: A serves S.
	// config Y: B serves S.
	// config Z: C serves S.
	// assume A learns config X and B learns config Y, further assume A learns Z instead of Y after installing X,
	// B is not allowed to serve S until A hands off S to B.
	// however, A is handing off S to C and hence B may never get shard S.
	// say, there's a final config which assigns all shards to B.
	// since B is reconfigruing, any new config cannot be learned by B
	// and hence all client requests would be rejected and the system goes in a live lock.
	nextConfig := kv.sm.Query(kv.config.Num + 1)

	// a config change is performed only if there's no pending config change.
	// however, it's okay so long as we ensure that a reconfiguring starts only after
	// the previous reconfiguring is completed.
	kv.mu.Lock()

	if !kv.reconfiguring && nextConfig.Num == kv.config.Num+1 {
		kv.reconfiguring = true

		// the next config is installed when the install config op is applied.
		op := &Op{OpType: "InstallConfig", Config: nextConfig}
		go kv.propose(op)

		println("S%v-%v starts reconfiguring to config (CN=%v)", kv.gid, kv.me, nextConfig.Num)
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) installConfig(nextConfig shardmaster.Config) {
	// check if there's any change on the served shards.
	shardsToHandoff := make([]int, 0)
	shardsToTakeOver := make([]int, 0)

	for shard := 0; shard < shardmaster.NShards; shard++ {
		currGid := kv.config.Shards[shard]
		newGid := nextConfig.Shards[shard]

		if currGid == 0 && newGid == kv.gid {
			// nothing to move if the from group is the invalid group 0.
			kv.shardDBs[shard].state = Serving
			continue
		}

		if currGid == kv.gid && newGid != kv.gid {
			// move this shard from this group to the group with gid newGid.
			kv.shardDBs[shard].state = MovingOut
			kv.shardDBs[shard].toGid = newGid
			shardsToHandoff = append(shardsToHandoff, shard)
		}
		if currGid != kv.gid && newGid == kv.gid {
			// move this shard from the group with gid currGid to this group.
			kv.shardDBs[shard].state = MovingIn
			kv.shardDBs[shard].fromGid = currGid
			shardsToTakeOver = append(shardsToTakeOver, shard)
		}
	}

	println("S%v-%v installed config (CN=%v)", kv.gid, kv.me, kv.config.Num)
	println("old config:")
	shardmaster.PrintGidToShards(&kv.config, true)
	println("new config:")
	shardmaster.PrintGidToShards(&nextConfig, true)

	// install the next config.
	kv.config = nextConfig

	if len(shardsToHandoff) == 0 && len(shardsToTakeOver) == 0 {
		// if the served shards do not change, the reconfiguring is done.
		kv.reconfiguring = false

		println("S%v-%v reconfigure done (CN=%v)", kv.gid, kv.me, kv.config.Num)
		return

	} else {
		// otherwise, the server has to take over moved-in shards or/and hand off moved-out shards.

		// the shard migration is performed in a push-based way, i.e. the initiator of a shard migration
		// is the replica group who is going to handoff shards.
		// on contrary, if performed in a pull-based way, the initiator of the shard migration is
		// the replica group who is going to take over shards. This replica group sends a pull request
		// to another replica group, and then that replica group starts sending shard data to the sender.
		if len(shardsToHandoff) > 0 {
			go kv.handoffShards(kv.config.Num)

			println("S%v-%v starts handing off shards %v", kv.gid, kv.me, shardsToHandoff)
		}

		if len(shardsToTakeOver) > 0 {
			println("S%v-%v waiting to take over shards %v", kv.gid, kv.me, shardsToTakeOver)
		}

		// periodically check the migration state.
		go kv.checkMigrationState(kv.config.Num)
	}
}

func (kv *ShardKV) isServingKey(key string) bool {
	shard := key2shard(key)
	return kv.shardDBs[shard].state == Serving
}
