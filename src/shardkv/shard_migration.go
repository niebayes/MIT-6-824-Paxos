package shardkv

import "time"
import "6.824/src/shardmaster"

type InstallShardArgs struct {
	ReconfigureToConfigNum int
	Shard                  int
	DB                     map[string]string
	MaxApplyOpIdOfClerk    map[int64]int
}

type InstallShardReply struct {
	Err Err
}

func (kv *ShardKV) makeInstallShardArgs(shard int) InstallShardArgs {
	shardDB := kv.shardDBs[shard]

	args := InstallShardArgs{
		ReconfigureToConfigNum: kv.reconfigureToConfigNum,
		Shard:                  shard,
		DB:                     make(map[string]string),
		MaxApplyOpIdOfClerk:    make(map[int64]int),
	}

	for k, v := range shardDB.dB {
		args.DB[k] = v
	}

	for k, v := range kv.maxApplyOpIdOfClerk {
		args.MaxApplyOpIdOfClerk[k] = v
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

					args := kv.makeInstallShardArgs(shard)
					servers := kv.config.Groups[kv.shardDBs[shard].toGid]
					serversClone := make([]string, 0)
					serversClone = append(serversClone, servers...)

					go kv.sendShard(&args, serversClone)
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
	for _, server := range servers {
		reply := &InstallShardReply{}
		ok := call(server, "ShardKV.InstallShard", args, reply)
		if ok && reply.Err == OK {
			kv.mu.Lock()

			if kv.isEligibleToUpdateShard(args.ReconfigureToConfigNum) && kv.shardDBs[args.Shard].state == MovingOut {
				op := &Op{OpType: "DeleteShard", ReconfigureToConfigNum: args.ReconfigureToConfigNum, Shard: args.Shard}
				go kv.propose(op)
			}

			kv.mu.Unlock()
			break
		}
	}
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ReconfigureToConfigNum < kv.config.Num {
		reply.Err = OK
		return nil
	}

	if kv.isEligibleToUpdateShard(args.ReconfigureToConfigNum) && kv.shardDBs[args.Shard].state == MovingIn {
		op := &Op{OpType: "InstallShard", ReconfigureToConfigNum: args.ReconfigureToConfigNum, Shard: args.Shard, DB: args.DB, MaxApplyOpIdOfClerk: args.MaxApplyOpIdOfClerk}
		go kv.propose(op)

		reply.Err = OK
		return nil
	}

	reply.Err = ErrNotApplied
	return nil
}

func (kv *ShardKV) installShard(op *Op) {
	for k, v := range op.DB {
		kv.shardDBs[op.Shard].dB[k] = v
	}

	for clerkId, otherOpId := range op.MaxApplyOpIdOfClerk {
		if opId, exist := kv.maxApplyOpIdOfClerk[clerkId]; !exist || otherOpId > opId {
			kv.maxApplyOpIdOfClerk[clerkId] = otherOpId
		}
	}

	kv.shardDBs[op.Shard].state = Serving
}

func (kv *ShardKV) deleteShard(op *Op) {
	kv.shardDBs[op.Shard] = ShardDB{
		dB:      make(map[string]string),
		state:   NotServing,
		fromGid: 0,
		toGid:   0,
	}
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
	return kv.reconfigureToConfigNum == reconfigureToConfigNum && kv.reconfigureToConfigNum == kv.config.Num
}
