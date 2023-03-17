package shardkv

import (
	"6.824/src/shardmaster"
	"time"
)

// the normal execution phase of an op/request consists of:
// receive request, propose op, decide op, execute op, apply op, reply request.
type Op struct {
	ClerkId                int64
	OpId                   int
	OpType                 string // "Get", "Put", "Append", "InstallConfig", "InstallShard", "DeleteShard", "NoOp".
	Key                    string
	Value                  string
	Config                 shardmaster.Config // the config to be installed.
	ReconfigureToConfigNum int                // the associated config num of the InstallShard or the DeleteShard op.
	Shard                  int                // used by InstallShard or DeleteShard to identify a shard.
	DB                     map[string]string
	MaxApplyOpIdOfClerk    map[int64]int // the clerk state would also be installed upon the installation of the shard data.
}

func isSameOp(opX *Op, opY *Op) bool {
	// comparing op types is used to compare two no-ops.
	// comparing clerk id and op id is used to compare two client ops.
	// comparing config num is used to compare two install config ops.
	// comparing shard num is used to compare two install shard ops.
	// it's okay the opX and opY that literally are not the same ops as long as they have the same
	// effect when they're executed.
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
