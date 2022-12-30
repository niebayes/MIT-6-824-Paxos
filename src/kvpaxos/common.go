package kvpaxos

type Err string

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	ErrRejected = "ErrRejected"
)

// Put or Append
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

type GetArgs struct {
	ClerkId int64
	OpId    int
	Key     string
}

type GetReply struct {
	Err   Err
	Value string
}
