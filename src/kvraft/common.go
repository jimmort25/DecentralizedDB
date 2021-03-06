package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	DUP      = "DUP"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Ident int64
	Seq   int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key   string
	Ident int64
	Seq   int
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type IsLeaderArgs struct {
	Client int64
}

type IsLeaderReply struct {
	IsLeader bool
	Term     int
}
