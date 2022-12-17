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
	Me      int64
	OpId    uint
	Key     string
	Value   string
	Op      string
	Primary string
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Me      int64
	OpId    uint
	Key     string
	Primary string
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferArgs struct {
	Me string
	// the previous or the current primary's complete database.
	Db           map[string]string
	LastExecOpId map[int64]uint
	CachedReply  map[int64]Reply
}

type TransferReply struct {
	Err Err
}
