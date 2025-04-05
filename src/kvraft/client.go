package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

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
	defer fmt.Printf("clientid:%d seqid %d finish\n", ck.clientId, ck.seqId)
	defer ck.mu.Unlock()
	// 生成固定seqId
	//seq := ck.GetSeq()

	seq := ck.seqId
	ck.seqId++

	clientId := ck.clientId

	for {
		// 尝试当前认为的Leader
		var reply GetReply
		ok := false

		//fmt.Printf("Client [%d] trying leader %d, op=%s key=%s", ck.clientId, leaderId, op, key)

		if op == "Get" {
			fmt.Printf("clientid:%d seqid: %d, targetKvid: %d, op: Get\n", clientId, seq, ck.leaderId)
			args := GetArgs{Key: key, ClientId: clientId, SeqId: seq}
			ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		} else {
			args := PutAppendArgs{
				Key:      key,
				Value:    value,
				Op:       op,
				ClientId: clientId,
				SeqId:    seq,
			}
			fmt.Printf("clientid:%d seqid: %d, targetKvid: %d, op: PutAppend\n", clientId, seq, ck.leaderId)
			var putReply PutAppendReply
			ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &putReply)
			reply.Err = putReply.Err
		}

		if ok {
			switch reply.Err {
			case OK:
				if op == "Get" {
					return reply.Value
				}
				return ""
			case ErrNoKey:
				return ""
			case ErrTimeout:
				//fmt.Printf("server %d timeout\n", leaderId)
			}
		}

		// 失败时尝试下一个节点
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		//fmt.Printf(" - failed, trying next server %d\n", leaderId)

		time_sleep_millsecond(5) // 等待10ms再尝试下一个节点
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
