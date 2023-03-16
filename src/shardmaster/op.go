package shardmaster

import (
	"time"
)

type OpType string

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	ClerkId   int64
	OpId      int
	OpType    OpType   // "Join", "Leave", "Move", "Query", "NoOp".
	GID       int64    // group id. Used by Join, Leave, Move.
	Servers   []string // group server ports. Used by Join.
	Shard     int      // shard id. Used by Move.
	ConfigNum int      // configuration id. Used by Query.
}

func isSameOp(opX *Op, opY *Op) bool {
	// comparing op types is used to compare two no-ops.
	// comparing clerk id and op id is used to compare two client ops.
	// it's okay the opX and opY that literally are not the same ops as long as they have the same
	// effect when they're executed.
	return opX.OpType == opY.OpType && opX.ClerkId == opY.ClerkId && opX.OpId == opY.OpId
}

func (sm *ShardMaster) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (sm *ShardMaster) noOpTicker() {
	for !sm.isdead() {
		op := &Op{OpType: "NoOp"}
		go sm.propose(op)

		time.Sleep(proposeNoOpInterval)
	}
}
