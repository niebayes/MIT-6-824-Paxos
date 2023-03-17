package shardkv

import (
	"6.824/src/shardmaster"
	"time"
)

type Op struct {
	ClerkId                int64
	OpId                   int
	OpType                 string // "Get", "Put", "Append", "InstallConfig", "InstallShard", "DeleteShard", "NoOp".
	Key                    string
	Value                  string
	Config                 shardmaster.Config
	ReconfigureToConfigNum int
	Shard                  int
	DB                     map[string]string
	MaxApplyOpIdOfClerk    map[int64]int
}

func isSameOp(opX *Op, opY *Op) bool {
	return opX.OpType == opY.OpType && opX.ClerkId == opY.ClerkId && opX.OpId == opY.OpId && opX.Config.Num == opY.Config.Num && opX.Shard == opY.Shard
}

func (kv *ShardKV) isAdminOp(op *Op) bool {
	return op.OpType == "InstallConfig" || op.OpType == "InstallShard" || op.OpType == "DeleteShard"
}

func (kv *ShardKV) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (kv *ShardKV) noOpTicker() {
	for !kv.isdead() {
		op := &Op{OpType: "NoOp"}
		go kv.propose(op)

		time.Sleep(proposeNoOpInterval)
	}
}
