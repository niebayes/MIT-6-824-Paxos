package shardkv

import "time"
import "6.824/src/shardmaster"

type InstallShardArgs struct {
	ReconfigureToConfigNum int               // the config num of the config the sender is reconfiguring to.
	Shard                  int               // shard id.
	DB                     map[string]string // shard data.
	MaxAppliedOpIdOfClerk  map[int64]int     // necessary for implementing the at-most-once semantics.
}

type InstallShardReply struct {
	Err Err
}

func (kv *ShardKV) makeInstallShardArgs(shard int) InstallShardArgs {
	shardDB := kv.shardDBs[shard]

	args := InstallShardArgs{
		// since kv.config.Num and kv.reconfigureToConfigNum must be identical at this time,
		// either of which could be assigned to ConfigNum.
		ReconfigureToConfigNum: kv.reconfigureToConfigNum,
		Shard:                  shard,
		DB:                     make(map[string]string),
		MaxAppliedOpIdOfClerk:  make(map[int64]int),
	}

	// deep clone shard data.
	// deep clone is necessary. See the bug described in `installShard`.
	for k, v := range shardDB.dB {
		args.DB[k] = v
	}

	// deep clone clerk state.
	for k, v := range kv.maxAppliedOpIdOfClerk {
		args.MaxAppliedOpIdOfClerk[k] = v
	}

	return args
}

func (kv *ShardKV) migrator() {
	for !kv.isdead() {
		kv.mu.Lock()

		migrating := false

		if kv.reconfigureToConfigNum != -1 {
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.shardDBs[shard].state == MovingIn {
					migrating = true
				}

				if kv.shardDBs[shard].state == MovingOut {
					migrating = true

					// the shard migration is performed in a push-based way, i.e. the initiator of a shard migration
					// is the replica group who is going to handoff shards.
					// on contrary, if performed in a pull-based way, the initiator of the shard migration is
					// the replica group who is going to take over shards. This replica group sends a pull request
					// to another replica group, and then that replica group starts sending shard data to the sender.
					//
					// deciding on which migration way to use is tricky, but generally the pull-based is prefered since
					// there mighe be less shard data to transfer by network since shard pulling is on demand while
					// shard pushing is not.

					// we could send to a group all shards it needs in one message,
					// however, considering the data of all shards may be way too large
					// and the network is unreliable, there're much overhead for resending
					// all shard data.
					// hence, we choose to send one shard in one message.

					args := kv.makeInstallShardArgs(shard)
					servers := kv.config.Groups[kv.shardDBs[shard].toGid]
					// this deep clone may be not necessary.
					serversClone := make([]string, 0)
					serversClone = append(serversClone, servers...)

					go kv.sendShard(&args, serversClone)

					println("S%v-%v sends shard (SN=%v) to G%v", kv.gid, kv.me, shard, kv.shardDBs[shard].toGid)
				}
			}
		}

		if kv.reconfigureToConfigNum != -1 && !migrating {
			kv.reconfigureToConfigNum = -1
		}

		kv.mu.Unlock()
		time.Sleep(checkMigrationStateInterval)
	}
}

func (kv *ShardKV) sendShard(args *InstallShardArgs, servers []string) {
	// send the shard to the receiver replica group.
	for _, server := range servers {
		reply := &InstallShardReply{}
		ok := call(server, "ShardKV.InstallShard", args, reply)
		// OK if the receiver has successfully proposed an install shard op corresponding to the moved-out shard.
		// otherwise, we retry sending the shard to other servers in the receiver replica group.
		if ok && reply.Err == OK {
			kv.mu.Lock()

			if kv.isEligibleToUpdateShard(args.ReconfigureToConfigNum) && kv.shardDBs[args.Shard].state == MovingOut {
				// propose a delete shard op to sync the uninstallation of a shard.
				//
				// why this op is necessary?
				//
				// assume:
				// the server's current config is X and it's reconfiguring to X+1.
				// other servers in the same replica group are reconfiguring to X+2.
				// this is possible since only a majority of servers is necessary to push server state and paxos state towards.
				// one of the other servers has proposed a install config op for config X+2.
				//
				// if there's no shard delete sync, then the install config op for config X+2 might arrive when this server
				// is reconfiguring to X+1.
				// the server will discard the install config op since it has not done reconfiguting to X+1.
				//
				// further assume:
				// the server completes the reconfiguring to X+1.
				// in config X+1, the server is serving a shard.
				// in config X+2, the server would not serve the shard.
				//
				// further assume:
				// a client request arrives at this server.
				// the client request corresponds to the shard served by the server in config X+1.
				//
				// then this server will propose the op and apply it.
				// but other servers which are reconfiguring to the config X+2 would not apply the op.
				// this introduces an async in client op executions.
				// so we have to add a delete shard op and sync it among servers in a group.

				op := &Op{OpType: "DeleteShard", ReconfigureToConfigNum: args.ReconfigureToConfigNum, Shard: args.Shard}
				go kv.propose(op)
			}

			kv.mu.Unlock()
			break
		}
	}
	// if no server in the receiver replica group has proposed an install shard op,
	// the next round of `handoffShards` will retry.
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if this server needs the shard, then it must have
	// `kv.config.Num == kv.reconfigureToConfigNum` && `kv.config.Num == args.ReconfigureToConfigNum`.
	// therefore, if args.ReconfigureToConfigNum < kv.config.Num, then the shard is stale and shall be rejected.
	//
	// however, the reply OK of the previous InstallShards may get lost due to network issues
	// and the sender would re-send the same InstallShards to the receiver.
	// if we do not reply OK, the sender will keep re-sending the requests and make no progress.
	if args.ReconfigureToConfigNum < kv.config.Num {
		reply.Err = OK
		println("S%v-%v rejects InstallShard due to already installed (CN=%v ACN=%v SN=%v)", kv.gid, kv.me, kv.config.Num, args.ReconfigureToConfigNum, args.Shard)
		return nil
	}

	// accept the shard only if the receiver and the sender are reconfiguring to the same config,
	// and the shard has not been installed yet.
	if kv.isEligibleToUpdateShard(args.ReconfigureToConfigNum) && kv.shardDBs[args.Shard].state == MovingIn {
		// note, there's a gap between the proposing of the op and the execution of the op.
		// we could filter the dup op so that the bandwidth pressure on the paxos level could be reduced.
		// however, this would require us to assign a unique server id and op id for each admin op.
		// hence, we choose not to filter out dup admin ops but restricted to apply it at most once.

		println("S%v-%v accepts InstallShard (ACN=%v SN=%v)", kv.gid, kv.me, args.ReconfigureToConfigNum, args.Shard)

		op := &Op{OpType: "InstallShard", ReconfigureToConfigNum: args.ReconfigureToConfigNum, Shard: args.Shard, DB: args.DB, MaxAppliedOpIdOfClerk: args.MaxAppliedOpIdOfClerk}
		go kv.propose(op)

		// reply OK so that the sender knows an install shard op is proposed.
		//
		// warning: immediately replying OK is okay only if there's no server crash.
		// if the server crashes, the proposer thread is killed and the install shard op is lost.
		// however, the sender would not re-send the shard data since OK is replied.
		// if there's server crash, the correct way is to let the receiver replies OK only if the
		// install shard op is applied.
		reply.Err = OK
		return nil
	}

	reply.Err = ErrNotApplied
	return nil
}

func (kv *ShardKV) installShard(op *Op) {
	// install server state.
	// the installation could be performed either by replacing or updating.
	// all shard data is migrated during shard migration, so replacing is okay.
	// undeleted stale shard data will be shadowed, so updating is okay as well.
	//
	// warning: simply assign op.DB to kv.DB is incorrect since variables in Go are references
	// which may incur a tricky bug that an update in one server of the group will also
	// be reflected in another server of the same group.
	// this is because the tests run a mocking cluster wherein all servers run in the same local machine.
	for k, v := range op.DB {
		kv.shardDBs[op.Shard].dB[k] = v
	}

	// this update could also be performed by replacing.
	// the above reasoning applies on here as well since not only shard data is synced, the max apply op id is also synced.
	for clerkId, otherOpId := range op.MaxAppliedOpIdOfClerk {
		if opId, exist := kv.maxAppliedOpIdOfClerk[clerkId]; !exist || otherOpId > opId {
			kv.maxAppliedOpIdOfClerk[clerkId] = otherOpId
		}
	}

	kv.shardDBs[op.Shard].state = Serving

	println("S%v-%v starts serving shard (SN=%v)", kv.gid, kv.me, op.Shard)
}

func (kv *ShardKV) deleteShard(op *Op) {
	kv.shardDBs[op.Shard] = ShardDB{
		dB:      make(map[string]string),
		state:   NotServing,
		fromGid: 0,
		toGid:   0,
	}

	println("S%v-%v starts not serving shard (SN=%v)", kv.gid, kv.me, op.Shard)
}

func (kv *ShardKV) isMigrating() bool {
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.shardDBs[shard].state == MovingIn || kv.shardDBs[shard].state == MovingOut {
			return true
		}
	}
	return false
}

func (kv *ShardKV) isEligibleToUpdateShard(reconfigureToConfigNum int) bool {
	// these two checkings ensure that the server must have been installed the config with the config num `reconfigureToConfigNum`
	// and must be waiting to install or delete shards.
	return kv.reconfigureToConfigNum == reconfigureToConfigNum && kv.reconfigureToConfigNum == kv.config.Num
}
