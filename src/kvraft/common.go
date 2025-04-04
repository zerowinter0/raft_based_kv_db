package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Put" or "Append"
	SeqId    int    // Sequence ID to detect duplicate requests
	ClientId int64  // Client ID to track requests
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	SeqId    int
	ClientId int64 // Client ID to track requests
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
