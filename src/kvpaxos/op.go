package kvpaxos

import (
	"time"
)

type Op struct {
	ClerkId int64
	OpId    int
	OpType  string // "Get", "Put", "Append", "NoOp".
	Key     string
	Value   string
}

func isSameOp(opX *Op, opY *Op) bool {
	// comparing op types is used to compare two no-ops.
	// comparing clerk id and op id is used to compare two client ops.
	// it's okay the opX and opY that literally are not the same ops as long as they have the same
	// effect when they're executed.
	return opX.OpType == opY.OpType && opX.ClerkId == opY.ClerkId && opX.OpId == opY.OpId
}

func (kv *KVPaxos) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (kv *KVPaxos) noOpTicker() {
	for !kv.isdead() {
		op := &Op{OpType: "NoOp"}
		go kv.propose(op)

		time.Sleep(proposeNoOpInterval)
	}
}
