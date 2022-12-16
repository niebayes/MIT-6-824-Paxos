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
	Me    string
	OpId  uint
	Key   string
	Value string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Me   string
	OpId uint
	Key  string
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferArgs struct {
	Me         string
	// the previous or the current primary's complete database.
	Db map[string]string
}

type TransferReply struct {
	Err Err
}
