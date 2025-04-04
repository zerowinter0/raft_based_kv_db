package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64      // 客户端唯一标识
	seqId    int        // 请求序列号（单调递增）
	leaderId int        // 当前认为的Leader服务器索引
	mu       sync.Mutex // 保护共享字段
}

func time_sleep_millsecond(t int) {
	time.Sleep(time.Duration(t) * time.Millisecond)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand() // 生成随机客户端ID
	ck.seqId = 0
	ck.leaderId = 0 // 初始假设Leader是第一个节点
	return ck
}

//func (ck *Clerk) GetSeq() (SendSeq int) {
//	SendSeq = ck.seqId
//	ck.seqId += 1
//	return
//}

func (ck *Clerk) sendRequest(op string, key string, value string) string {
	ck.mu.Lock()
	// 生成固定seqId
	//seq := ck.GetSeq()
	ck.seqId++
	seq := ck.seqId
	clientId := ck.clientId
	leaderId := ck.leaderId
	ck.mu.Unlock()

	for {
		// 尝试当前认为的Leader
		var reply GetReply
		ok := false

		//fmt.Printf("Client [%d] trying leader %d, op=%s key=%s", ck.clientId, leaderId, op, key)

		if op == "Get" {
			args := GetArgs{Key: key, ClientId: clientId, SeqId: seq}
			ok = ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		} else {
			args := PutAppendArgs{
				Key:      key,
				Value:    value,
				Op:       op,
				ClientId: clientId,
				SeqId:    seq,
			}
			var putReply PutAppendReply
			ok = ck.servers[leaderId].Call("KVServer.PutAppend", &args, &putReply)
			reply.Err = putReply.Err
		}

		if ok {
			switch reply.Err {
			case OK:
				ck.mu.Lock()
				ck.leaderId = leaderId // 更新Leader缓存
				ck.mu.Unlock()
				if op == "Get" {
					return reply.Value
				}
				return ""
			case ErrNoKey:
				return ""
			}
		}

		// 失败时尝试下一个节点
		leaderId = (leaderId + 1) % len(ck.servers)
		//fmt.Printf(" - failed, trying next server %d\n", leaderId)

		time_sleep_millsecond(10) // 等待10ms再尝试下一个节点
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ck.sendRequest("Get", key, "")
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sendRequest(op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
