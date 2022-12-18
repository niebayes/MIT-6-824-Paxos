package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrStale       = "ErrStale"
	ErrInternal    = "ErrInternal"
	ErrDuplicate   = "ErrDuplicate"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Me      int64 // clerk id.
	OpId    uint
	Key     string
	Value   string
	Op      string // "Put" or "Append".
	Primary string
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Me      int64 // clerk id.
	OpId    uint
	Key     string
	Primary string
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferArgs struct {
	Me           string // pb server address.
	Db           map[string]string
	LastExecOpId map[int64]uint
}

type TransferReply struct {
	Err Err
}
