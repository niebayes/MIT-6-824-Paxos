package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//

const (
	OK            = "OK"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotApplied = "ErrNotApplied"
)

type Err string

type GetArgs struct {
	ClerkId int64
	OpId    int
	Key     string
}

type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	ClerkId int64
	OpId    int
	OpType  string // "Put" or "Append"
	Key     string
	Value   string
}

type PutAppendReply struct {
	Err Err
}
