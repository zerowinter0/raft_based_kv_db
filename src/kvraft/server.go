package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OperationType int

const (
	Enum_Get    OperationType = iota // Get operation type
	Enum_Put                         // Put operation type
	Enum_Append                      // Append operation type
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationType OperationType // Type of operation (Get/Put/Append)
	Key           string        // Key for the operation
	Value         string        // Value for Put/Append operations
	SeqId         int           // Sequence ID to detect duplicate requests
	ClientId      int64         // Client ID to track requests
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // Raft提交日志的通道
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// Your definitions here.
	kvStore     map[string]string     // The actual key-value store
	lastSeq     map[int64]int         // Last sequence number seen for each client
	waitCh      map[int]chan OpResult // 等待结果的通知通道（按日志索引）
	lastApplied int                   // Last applied index to detect duplicates
}

type OpResult struct {
	Err   Err    // Operation result (OK, ErrNoKey, etc.)
	Value string // Value for Get operations
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OperationType: Enum_Get,
		Key:           args.Key,
		SeqId:         args.SeqId,
		ClientId:      args.ClientId,
	}
	result := kv.HandleOp(op)
	reply.Err = result.Err
	reply.Value = result.Value

}

//func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//}
//
//func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//}

// PutAppend is the handler for Put and Append operations.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Determine operation type
	var opType OperationType
	if args.Op == "Put" {
		opType = Enum_Put
	} else {
		opType = Enum_Append
	}

	op := Op{
		OperationType: opType,
		Key:           args.Key,
		Value:         args.Value,
		SeqId:         args.SeqId,
		ClientId:      args.ClientId,
	}

	result := kv.HandleOp(op)
	reply.Err = result.Err
}

// 快速去重检查：在持有锁的情况下检查是否已处理过该请求，避免不必要的Raft提交。
// Raft提交：只有Leader节点能成功提交操作，否则返回ErrWrongLeader。
// 异步等待：创建与日志索引关联的通道，等待applier协程处理完成后的通知。
// 超时机制：防止因网络分区等原因导致永久阻塞。
func (kv *KVServer) HandleOp(op Op) OpResult {
	kv.mu.Lock()

	// Check for duplicate request
	if seq, ok := kv.lastSeq[op.ClientId]; ok && seq >= op.SeqId {
		// This is a duplicate request, return cached result
		var value string // 如果是Get则返回当前值
		if op.OperationType == Enum_Get {
			value = kv.kvStore[op.Key]
		}
		kv.mu.Unlock()
		return OpResult{OK, value}
	}
	kv.mu.Unlock()

	// 第二阶段：提交到Raft共识
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpResult{ErrWrongLeader, ""}
	}

	// 第三阶段：创建等待通道
	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()

	// 第四阶段：等待结果或超时
	select {
	case result := <-ch:
		return result
	case <-time.After(20 * time.Millisecond):
		return OpResult{ErrTimeout, ""}
	}

}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh { // 持续监听提交的日志
		if !msg.CommandValid { // 忽略非命令消息（如快照）
			continue
		}

		op := msg.Command.(Op)
		kv.mu.Lock()

		// 幂等性检查
		if msg.CommandIndex <= kv.lastApplied {
			kv.mu.Unlock()
			continue
		}

		lastSeq, exists := kv.lastSeq[op.ClientId]
		result := OpResult{Err: OK}

		if !exists || op.SeqId > lastSeq { // 新请求
			switch op.OperationType {
			case Enum_Get: // Get需要特殊处理不存在的情况
				if val, ok := kv.kvStore[op.Key]; ok {
					result.Value = val
				} else {
					result.Err = ErrNoKey
				}
			case Enum_Put:
				kv.kvStore[op.Key] = op.Value
			case Enum_Append:
				kv.kvStore[op.Key] += op.Value
			}
			// 更新序列号
			kv.lastSeq[op.ClientId] = op.SeqId
		} else if op.OperationType == Enum_Get {
			// 返回当前值但不修改状态
			result.Value = kv.kvStore[op.Key]
			//fmt.Printf("Get old value %s\n", result.Value)
		}

		// 通知等待的RPC
		if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
			ch <- result // 非阻塞发送（通道带缓冲）
		}

		kv.lastApplied = msg.CommandIndex
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.lastSeq = make(map[int64]int)
	kv.waitCh = make(map[int]chan OpResult)
	kv.lastApplied = 0

	// Start applier goroutine to process committed entries
	go kv.applier()

	return kv
}
