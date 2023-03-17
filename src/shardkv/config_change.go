package shardkv

import "6.824/src/shardmaster"

// ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	// the config change has to be contiguous.
	// for example, there're three groups A, B, C, and three contiguous configs X, Y, Z and a shard S.
	// config X: A serves S.
	// config Y: B serves S.
	// config Z: C serves S.
	// assume A learns config X and B learns config Y, further assume A learns Z instead of Y after installing X,
	// B is not allowed to serve S until S is moved from A to B.
	// however, A is going to hand off S to C and hence B may never get shard S.
	// say, there's a final config which assigns all shards to B.
	// since B is reconfigruing, any new config cannot be learned by B
	// and hence all client requests would be rejected and the system goes in a live lock.

	kv.mu.Lock()
	nextConfigNum := kv.config.Num + 1
	kv.mu.Unlock()

	nextConfig := kv.sm.Query(nextConfigNum)

	kv.mu.Lock()

	// a config change is performed only if there's no pending config change.
	// however, it's not necessary to reject proposing new configs during reconfiguring
	// so long as we ensure that a reconfiguring starts only after the previous reconfiguring is completed.
	if kv.reconfigureToConfigNum == -1 && nextConfig.Num == kv.config.Num+1 {
		kv.reconfigureToConfigNum = nextConfig.Num

		// propose an install config op.
		op := &Op{OpType: "InstallConfig", Config: nextConfig}
		go kv.propose(op)

		println("S%v-%v starts reconfiguring to config (CN=%v)", kv.gid, kv.me, nextConfig.Num)
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) installConfig(nextConfig shardmaster.Config) {
	// check if there's any change on the served shards.
	hasShardsToHandOff := false
	hasShardsToTakeOver := false

	for shard := 0; shard < shardmaster.NShards; shard++ {
		currGid := kv.config.Shards[shard]
		newGid := nextConfig.Shards[shard]

		if currGid == 0 && newGid == kv.gid {
			// nothing to move in if the from group is the invalid group 0.
			kv.shardDBs[shard].state = Serving

			println("S%v-%v set shard (SN=%v) to state=%v at installing config (ACN=%v CN=%v)", kv.gid, kv.me, shard, kv.shardDBs[shard].state, nextConfig.Num, kv.config.Num)
			continue
		}

		// warning: this seems not necessary.
		if newGid == 0 {
			// noting to move out if the to group is the invalid group 0.
			kv.shardDBs[shard].state = NotServing

			println("S%v-%v set shard (SN=%v) to state=%v at installing config (ACN=%v CN=%v)", kv.gid, kv.me, shard, kv.shardDBs[shard].state, nextConfig.Num, kv.config.Num)
			continue
		}

		if currGid == kv.gid && newGid != kv.gid {
			// move this shard from this group to the group with gid newGid.
			kv.shardDBs[shard].state = MovingOut
			kv.shardDBs[shard].toGid = newGid
			hasShardsToHandOff = true

			println("S%v-%v set shard (SN=%v) to state=%v at installing config (ACN=%v CN=%v)", kv.gid, kv.me, shard, kv.shardDBs[shard].state, nextConfig.Num, kv.config.Num)
		}
		if currGid != kv.gid && newGid == kv.gid {
			// move this shard from the group with gid currGid to this group.
			kv.shardDBs[shard].state = MovingIn
			kv.shardDBs[shard].fromGid = currGid
			hasShardsToTakeOver = true

			println("S%v-%v set shard (SN=%v) to state=%v at installing config (ACN=%v CN=%v)", kv.gid, kv.me, shard, kv.shardDBs[shard].state, nextConfig.Num, kv.config.Num)
		}
	}

	// install the next config.
	kv.config = nextConfig

	// if the served shards do not change, the reconfiguring is done.
	if !hasShardsToHandOff && !hasShardsToTakeOver {
		kv.reconfigureToConfigNum = -1

		println("S%v-%v reconfigure done (CN=%v)", kv.gid, kv.me, kv.config.Num)
		return

	} else {
		// otherwise, the server has to take over moved-in shards or/and hand off moved-out shards.
		if hasShardsToHandOff {
			println("S%v-%v starts handing off shards", kv.gid, kv.me)
		}

		if hasShardsToTakeOver {
			println("S%v-%v waiting to take over shards", kv.gid, kv.me)
		}
	}
}

func (kv *ShardKV) isServingKey(key string) bool {
	shard := key2shard(key)
	return kv.shardDBs[shard].state == Serving
}
