package shardkv

import "time"
import "6.824/src/shardmaster"

type InstallShardArgs struct {
	// the receiver would reject the shard if it's not in the same config.
	// see the reasoning in `InstallShard`.
	ReconfigureToConfigNum int               // sender's current config number.
	Shard                  int               // shard id.
	DB                     map[string]string // shard data.
	MaxApplyOpIdOfClerk    map[int64]int     // necessary for implementing the at-most-once semantics.
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
		MaxApplyOpIdOfClerk:    make(map[int64]int),
	}

	// deep clone shard data.
	// deep clone is necessary. See the bug described in `installShard`.
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
		// put another way, this `handoffShards` is associated with a reconfiguring.
		// therefore, this `handoffShards` keeps running only if the server is reconfiguring
		// to the config with the config number configNum.
		//
		// note: it's okay we simply check `reconfiguring`.
		// assume a reconfiguring starts just follow the complete of the last reconfugring.
		// so that the `handoffShards` would not exit since `reconfiguring` is set to true
		// by the current reconfiguring.
		// although this is okay, we choose to associate each `handoffShards` to a reconfiguring
		// and kill it once the corresponding reconfiguring is done.
		if kv.config.Num != configNum || kv.reconfigureToConfigNum != configNum {
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

		// see the reasoning in `handoffShards`.
		if kv.config.Num != configNum || kv.reconfigureToConfigNum != configNum {
			kv.mu.Unlock()
			break
		}

		if !kv.isMigrating() {
			kv.reconfigureToConfigNum = -1
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
			// if there's no server crash, once an op is givens to a proposer thread, the proposer thread guarantees
			// it would be proposed to paxos and paxos guarantees that the op would be decided.
			// hence, the install shard op would sooner or later be executed by the receiver,
			// and hence the sender can safely set the shard state to NotServing.
			//
			// note, there's no need to propose a reconfigure done op to sync the complete of moving out shards.
			// whether the server is moving out the shard or is not serving the shard, all client ops corresponding
			// to the shard would not be applied.
			// each server in the replica group will try to send the shard to the receiver replica group.
			// the receiver would reply OK upon receiving a message if the receiver has proposed an install shard op for the shard.
			// hence, all servers in the sender replica group will sooner or later learns the complete of the shard migration
			// and starts not serving the moved-out shards.
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

	// the args' config num is the sender's reconfigureToConfigNum.
	// if it's less than or equal to the receiver's current config num, this means the receiver has
	// installed the shards and has done the configuring.
	// the reply OK of the previous InstallShards may get lost due to network issues
	// and the sender re-sends the install shard requests to the receiver.
	// if we do not reply OK, the sender will keep re-sending the requests and make no progress.
	if args.ReconfigureToConfigNum < kv.config.Num {
		reply.Err = OK
		println("S%v-%v rejects InstallShard due to already installed (CN=%v ACN=%v SN=%v)", kv.gid, kv.me, kv.config.Num, args.ReconfigureToConfigNum, args.Shard)
		return nil
	}

	// accept the shard only if the receiver and the sender are reconfiguring to the same config
	// and the shard has not been installed yet.
	// for example, the group serves a shard in the last config and moves the shard out in the current config.
	// it then serves the shard in the next config.
	// due to network delay and other issues, the install shard request corresponding to the last config
	// happens to arrive at the next config.
	// if we do not reject the request, the stale shard data may get installed.
	// although we could delay the staleness checking to the time the op gets executed,
	// it'd be better to filter out such requests upon receiving the requests.
	if args.ReconfigureToConfigNum == kv.reconfigureToConfigNum && kv.config.Num == kv.reconfigureToConfigNum && kv.shardDBs[args.Shard].state == MovingIn {
		// note, there's a gap between the proposing of the op and the execution of the op.
		// we could filter the dup op so that the bandwidth pressure on the paxos level could be reduced.
		// however, this would require us to assign a unique server id and op id for each admin op.
		// hence, we choose not to filter out dup admin ops but restricted to apply it at most once.

		println("S%v-%v accepts InstallShard (ACN=%v SN=%v)", kv.gid, kv.me, args.ReconfigureToConfigNum, args.Shard)

		op := &Op{OpType: "InstallShard", ReconfigureToConfigNum: args.ReconfigureToConfigNum, Shard: args.Shard, DB: args.DB, MaxApplyOpIdOfClerk: args.MaxApplyOpIdOfClerk}
		go kv.propose(op)

		// reply OK so that the sender knows an install shard op is proposed.
		// warning: immediately replying OK is okay only if there's no server crash.
		// if the server may crash, the proposer thread is killed and the install shard op is lost.
		// however, the sender would not re-send the shard data since OK is replied.
		// if there's server crash, the correct way is to let the receiver replies OK only if the
		// install shard op is applied.
		reply.Err = OK

		return nil
	}

	if kv.config.Num != kv.reconfigureToConfigNum {
		println("S%v-%v rejects InstallShard due to I'm not reconfiguring (CN=%v RCN=%v ACN=%v SN=%v)", kv.gid, kv.me, kv.config.Num, kv.reconfigureToConfigNum, args.ReconfigureToConfigNum, args.Shard)
	}

	if args.ReconfigureToConfigNum != kv.reconfigureToConfigNum {
		println("S%v-%v rejects InstallShard due to not reconfiguring to the same config (CN=%v ACN=%v SN=%v)", kv.gid, kv.me, kv.reconfigureToConfigNum, args.ReconfigureToConfigNum, args.Shard)
	}

	if kv.shardDBs[args.Shard].state != MovingIn {
		println("S%v-%v rejects InstallShard due to state %v (CN=%v ACN=%v SN=%v)", kv.gid, kv.me, kv.shardDBs[args.Shard].state, kv.reconfigureToConfigNum, args.ReconfigureToConfigNum, args.Shard)
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

	println("old db SN=%v:", op.Shard)
	for k, v := range kv.shardDBs[op.Shard].dB {
		println("S%v-%v K=%v V=%v", kv.gid, kv.me, k, v)
	}

	for k, v := range op.DB {
		kv.shardDBs[op.Shard].dB[k] = v
	}

	println("new db:")
	for k, v := range kv.shardDBs[op.Shard].dB {
		println("S%v-%v K=%v V=%v", kv.gid, kv.me, k, v)
	}
	kv.shardDBs[op.Shard].state = Serving

	// update clerk state.
	// this update could also be performed by replacing.
	// the above reasoning applies on here as well since not only shard data is synced, the max apply op id is also synced.
	println("old max apply SN=%v:", op.Shard)
	for k, v := range kv.maxApplyOpIdOfClerk {
		println("S%v-%v C=%v AId=%v", kv.gid, kv.me, k, v)
	}
	for clerkId, otherOpId := range op.MaxApplyOpIdOfClerk {
		if opId, exist := kv.maxApplyOpIdOfClerk[clerkId]; !exist || otherOpId > opId {
			kv.maxApplyOpIdOfClerk[clerkId] = otherOpId
		}
	}
	println("new max apply SN=%v:", op.Shard)
	for k, v := range kv.maxApplyOpIdOfClerk {
		println("S%v-%v C=%v AId=%v", kv.gid, kv.me, k, v)
	}

	println("S%v-%v starts serving shard (SN=%v)", kv.gid, kv.me, op.Shard)
}

func (kv *ShardKV) isMigrating() bool {
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.shardDBs[shard].state == MovingIn || kv.shardDBs[shard].state == MovingOut {
			return true
		}
	}
	return false
}
