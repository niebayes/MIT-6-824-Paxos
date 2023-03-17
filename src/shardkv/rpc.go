package shardkv

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
