package shardkv

import "time"
import "6.824/src/shardmaster"

type InstallShardArgs struct {
	// the receiver would reject the shard if it's not in the same config.
	// if the receiver is allowed to propose an install shard op before it's reconfiguring to
	// the corresponding config, then the install shard would be applied prior to the start of
	// the reconfiguring which is definitely incorrect.
	ConfigNum int               // sender's config number.
	Shard     int               // shard id.
	DB        map[string]string // shard data.
	// to support the at-most-once semantics, max apply op id is necessary to be synced
	// while the max prop op id is not.
	MaxApplyOpIdOfClerk map[int64]int
}

type InstallShardReply struct {
	Err Err
}

func (kv *ShardKV) makeInstallShardArgs(shard int) InstallShardArgs {
	shardDB := kv.shardDBs[shard]

	args := InstallShardArgs{
		ConfigNum:           kv.config.Num,
		Shard:               shard,
		DB:                  make(map[string]string),
		MaxApplyOpIdOfClerk: make(map[int64]int),
	}

	// deep clone shard data.
	for k, v := range shardDB.dB {
		args.DB[k] = v
	}

	// deep clone clerk state.
	for k, v := range kv.maxApplyOpIdOfClerk {
		args.MaxApplyOpIdOfClerk[k] = v
	}

	return args
}

func (kv *ShardKV) handoffShards(configNum int) {
	for !kv.isdead() {
		kv.mu.Lock()

		// each reconfiguring will incur a `handoffShards` if there're shards required to move out.
		// put another way, the `handoffShards` is associated with this reconfiguring.
		// therefore, this `handoffShards` keeps running only if the server is reconfiguring
		// to the config with the config number configNum.
		// simply checking `reconfiguring` is not safe due to concurrency.
		if kv.config.Num != configNum || !kv.reconfiguring {
			kv.mu.Unlock()
			break
		}

		movingOutShards := false
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.shardDBs[shard].state == MovingOut {
				movingOutShards = true

				// we could send to a group all shards it needs in one message,
				// however, considering the data of all shards may be way too large
				// and the network is unreliable, there're much overhead for resending
				// all shard data.
				// hence, we choose to send one shard in one message.

				args := kv.makeInstallShardArgs(shard)
				go kv.sendShard(&args, kv.shardDBs[shard].toGid)

				println("S%v-%v sends shard (SN=%v) to G%v", kv.gid, kv.me, shard, kv.shardDBs[shard].toGid)
			}
		}

		kv.mu.Unlock()

		if !movingOutShards {
			break
		}

		time.Sleep(handoffShardsInterval)
	}

	println("S%v-%v done handing off shards", kv.gid, kv.me)
}

func (kv *ShardKV) checkMigrationState(configNum int) {
	for !kv.isdead() {
		kv.mu.Lock()

		if kv.config.Num != configNum || !kv.reconfiguring {
			kv.mu.Unlock()
			break
		}

		migrating := false
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.shardDBs[shard].state == MovingIn || kv.shardDBs[shard].state == MovingOut {
				migrating = true
				break
			}
		}

		if !migrating {
			kv.reconfiguring = false
			println("S%v-%v reconfigure done (CN=%v)", kv.gid, kv.me, kv.config.Num)
			kv.mu.Unlock()
			break
		}

		kv.mu.Unlock()

		time.Sleep(checkMigrationStateInterval)
	}
}

func (kv *ShardKV) sendShard(args *InstallShardArgs, gid int64) {
	// send the shard to the receiver replica group.
	servers := kv.config.Groups[gid]
	for _, server := range servers {
		reply := &InstallShardReply{}
		ok := call(server, "ShardKV.InstallShard", args, reply)
		// OK if the receiver has successfully proposed an install shard op corresponding to the moved-out shard.
		// otherwise, we retry sending the shard to other servers in the receiver replica group.
		if ok && reply.Err == OK {
			kv.mu.Lock()
			// if no server crash, once an op is proposed, paxos guarantees that the op would be decided.
			// hence, the proposed install shard op would sooner or later be executed by the receiver,
			// and hence the sender can safely set the shard state to NotServing.
			//
			// note, there's no need to propose a reconfigure done op to sync the complete of moving out shards.
			// whether the server is moving out the shard or is not serving the shard, all client ops corresponding
			// to the shard would not be applied.
			// each server in the replica group will try to send the shard to the receiver replica group.
			// the receiver would reply OK upon receiving a message if the receiver has proposed an install shard op for the shard.
			// hence, all servers in the sender replica group will sooner or later learns the complete of the shard migration
			// and no live lock would be introduced.
			//
			// note: if used the pull-based migration, the logic in the sender side would be more complicated.
			kv.shardDBs[args.Shard].state = NotServing
			kv.mu.Unlock()

			println("S%v-%v starts not serving shard (SN=%v)", kv.gid, kv.me, args.Shard)
			break
		}
	}
	// if no server in the receiver replica group has proposed an install shard op,
	// the next round of `handoffShards` will retry.
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// reject the shard since the receiver and the sender are not in the same config.
	if args.ConfigNum != kv.config.Num {
		reply.Err = ErrNotProposed
		println("S%v-%v rejects InstallShard due to inconsistent config (CN=%v ACN=%v SN=%v)", kv.gid, kv.me, kv.config.Num, args.ConfigNum, args.Shard)
		return nil
	}

	// reject the shard since the receiver has installed the shard.
	if kv.shardDBs[args.Shard].state != MovingIn {
		reply.Err = OK
		println("S%v-%v rejects InstallShard due to already installed (ACN=%v SN=%v)", kv.gid, kv.me, args.ConfigNum, args.Shard)
		return nil
	}

	println("S%v-%v accepts InstallShard (ACN=%v SN=%v)", kv.gid, kv.me, args.ConfigNum, args.Shard)

	// note, there's a gap between the proposing of the op and the execution of the op.
	// we could filter the dup op in order to reject the shard during the gap.
	// however, this would complicate the implementation and is not necessary.

	// propose an install shard op.
	op := &Op{OpType: "InstallShard", Shard: args.Shard, DB: args.DB, MaxApplyOpIdOfClerk: args.MaxApplyOpIdOfClerk}
	go kv.propose(op)

	// reply OK so that the sender knows an install shard op is proposed.
	reply.Err = OK

	return nil
}

func (kv *ShardKV) installShard(op *Op) {
	// install server state.
	kv.shardDBs[op.Shard].dB = op.DB
	kv.shardDBs[op.Shard].state = Serving

	// update clerk state.
	for clerkId, otherOpId := range op.MaxApplyOpIdOfClerk {
		if opId, exist := kv.maxApplyOpIdOfClerk[clerkId]; !exist || otherOpId > opId {
			kv.maxApplyOpIdOfClerk[clerkId] = otherOpId
		}
	}

	// TODO: add op to notify servers in one group to start serve / not serve a shard.

	println("S%v-%v starts serving shard (SN=%v)", kv.gid, kv.me, op.Shard)
}
