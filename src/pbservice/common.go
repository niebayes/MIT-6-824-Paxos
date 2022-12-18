package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"       // no key-value item found for a given key.
	ErrWrongServer = "ErrWrongServer" // RPC sent to a wrong server.
	ErrInternal    = "ErrInternal"    // any other errors are subsumed to the internal error type.
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Me      int64 // clerk id.
	OpId    uint  // operation id.
	Key     string
	Value   string
	Op      string // "Put" or "Append".
	Primary string // primary in the clerk's view.
}

type PutAppendReply struct {
	Err   Err
	Value string // used to ensure the at-most-once semantics.
}

type GetArgs struct {
	Me      int64 // clerk id.
	OpId    uint  // operation id.
	Key     string
	Primary string // primary in the clerk's view.
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferArgs struct {
	Me       string            // pb server address.
	Db       map[string]string // key-value database copy.
	NextOpId map[int64]uint    // used to ensure the at-most-once semantics.
}

type TransferReply struct {
	Err Err
}
