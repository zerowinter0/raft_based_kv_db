package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志
type LogEntry struct {
	Index int
	Term  int
	Msg   interface{}
}

// 服务器身份
type Identity int

const (
	Enum_Follower Identity = iota
	Enum_Candidate
	Enum_Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	current_term   int
	voted_for      int
	log            []LogEntry
	identity       Identity
	commit_index   int
	last_applied   int
	applyCh        chan ApplyMsg
	applyCV        sync.Cond
	heartbeatTimer *time.Timer

	//used for time out
	receive_heartbeat bool

	//below is only for leader, is a server becomes a leader, it should initialize these status
	next_indexes    []int
	matches_indexes []int
	votes           int
}

func Assert(tmp bool, msg string) {
	if !tmp {
		fmt.Println(msg)
		os.Exit(1)
	}
}

func (rf *Raft) setHeartBeatTimer(t int) {
	rf.mu.Lock()
	rf.heartbeatTimer.Reset(time.Duration(t) * time.Millisecond)
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.current_term
	isleader = (rf.identity == Enum_Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_term)
	e.Encode(rf.voted_for)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	term := 0
	voted_for := 0
	log := []LogEntry{}
	if d.Decode(&term) != nil ||
		d.Decode(&voted_for) != nil ||
		d.Decode(&log) != nil {
		Assert(false, "read persist decode fail")
	} else {
		rf.current_term = term
		rf.voted_for = voted_for
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term           int
	Candidate_id   int
	Last_log_index int
	Last_log_term  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term           int
	Vote_granted   bool
	Reject_reasion int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//fmt.Printf("%d receive a request vote from %d on term %d!!!\n", rf.me, args.Candidate_id, args.Term)
	rf.mu.Lock()

	defer rf.mu.Unlock()
	if args.Term < rf.current_term {
		//candidate term too old
		reply.Vote_granted = false
		reply.Term = rf.current_term
		reply.Reject_reasion = 1
		return
	}
	if args.Term == rf.current_term {
		if rf.voted_for != -1 && rf.voted_for != args.Candidate_id {
			//already vote in this term
			reply.Vote_granted = false
			reply.Term = rf.current_term
			reply.Reject_reasion = 2
			return
		}
	}
	if args.Term > rf.current_term {
		//如果我的日志更加新，但我现在的term比其他日志很久的服务器都要低很多，那么我可以用这段代码立刻将term与其他服务器追平，以确保我能尽快当选新leader
		rf.current_term = args.Term
		if rf.identity == Enum_Leader {
			rf.identity = Enum_Follower
		}
		rf.persist()
	}
	last_entry := rf.log[len(rf.log)-1]
	if args.Last_log_term < last_entry.Term || (args.Last_log_term == last_entry.Term && args.Last_log_index < last_entry.Index) {
		//candidate log too old
		reply.Vote_granted = false
		reply.Term = rf.current_term
		reply.Reject_reasion = 3
		return
	}

	//accept
	rf.receive_heartbeat = true
	rf.identity = Enum_Follower
	rf.current_term = args.Term
	rf.voted_for = args.Candidate_id
	reply.Vote_granted = true
	reply.Term = rf.current_term
	rf.persist()

}

// AppendEntries RPC参数与返回值
type AppendEntriesArgs struct {
	Term           int
	Leader_id      int
	Prev_log_index int
	Prev_log_term  int
	Log_entries    []LogEntry
	Commit_index   int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//if not success, it can add a expect index
	Index_expect int
}

// 实现AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Log_entries) > 0 {
		Assert(args.Prev_log_index == args.Log_entries[0].Index-1, "append entries args的下标出问题")
	}
	reply.Index_expect = -1
	if args.Term < rf.current_term {
		reply.Term = rf.current_term
		reply.Success = false
		reply.Index_expect = -1
		return
	}

	if args.Term >= rf.current_term {
		if args.Term == rf.current_term && rf.identity == Enum_Leader {
			fmt.Printf("term %d 中,有两个leader:%d %d\n", args.Term, rf.me, args.Leader_id)
			Assert(false, "damn")
		}
		if args.Term > rf.current_term {
			rf.current_term = args.Term
			rf.persist()
		}
		rf.identity = Enum_Follower
		rf.receive_heartbeat = true
	}

	if rf.log[len(rf.log)-1].Index < args.Prev_log_index {
		reply.Term = rf.current_term
		reply.Success = false
		reply.Index_expect = rf.log[len(rf.log)-1].Index + 1

		return
	}

	if rf.log[args.Prev_log_index].Term != args.Prev_log_term {
		reply.Term = rf.current_term
		reply.Success = false
		reply.Index_expect = args.Prev_log_index //上一位冲突了，期望获取上一位的数据
		//这一位的index应该大于commit index
		if args.Prev_log_index <= rf.commit_index {
			fmt.Printf("%d 日志系统异常,已提交日志%d与leader冲突,自己这位的term为%d,leader的term为%d\n", rf.me, args.Prev_log_index, rf.log[args.Prev_log_index].Term, args.Prev_log_term)
			os.Exit(1)
		}
		return
	}

	var rf_log_changed = false
	//接收请求
	for _, log_entry := range args.Log_entries {
		if len(rf.log) <= log_entry.Index {
			if len(rf.log) != log_entry.Index {
				fmt.Printf("日志系统异常,accept了一个超过最新log的log\n")
				os.Exit(1)
			}
			rf.log = append(rf.log, log_entry)
			rf_log_changed = true
		} else {
			if rf.log[log_entry.Index].Term != log_entry.Term {
				//这一位的index应该大于commit index
				if log_entry.Index <= rf.commit_index {
					fmt.Printf("%d 日志系统异常,已提交日志%d被重写\n", rf.me, log_entry.Index)
					os.Exit(1)
				}

				//内容有冲突，删除冲突index及之后的所有内容
				rf.log = rf.log[:log_entry.Index]

				rf.log = append(rf.log, log_entry)

				rf_log_changed = true
			}
		}
	}
	if rf_log_changed {
		rf.persist()
	}
	if args.Commit_index > rf.commit_index {
		new_commit_index := min(args.Commit_index, len(rf.log))
		Assert(rf.commit_index <= new_commit_index, "new_commit_index小于commit_index")
		if rf.commit_index < new_commit_index { //有可能leader的commitindex更后，但这条信息是一个心跳，没有实质的commit index更新
			rf.commit_index = new_commit_index
			//应用于状态机
			rf.applyCV.Signal()
			//fmt.Printf("follower %d commit_index update to %d\n", rf.me, rf.commit_index)
		}
	}
	rf.receive_heartbeat = true
	reply.Term = rf.current_term
	reply.Success = true
	return
}

func (rf *Raft) ServerCommitListener() {
	rf.mu.Lock()
	for !rf.killed() {
		rf.applyCV.Wait()
		for rf.last_applied < rf.commit_index {
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.last_applied+1].Msg,
				CommandIndex: rf.last_applied + 1,
			}
			rf.mu.Unlock()
			rf.applyCh <- *msg
			rf.mu.Lock()
			rf.last_applied++
		}
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (3B).
	rf.mu.Lock()

	isLeader = (rf.identity == Enum_Leader)
	index = len(rf.log)
	term = rf.current_term

	//如果不是leader，直接返回
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	//fmt.Printf("%d leader receive cmd %d\n", rf.me, index)

	log_entry := LogEntry{
		Index: index,
		Term:  term,
		Msg:   command,
	}

	rf.log = append(rf.log, log_entry)
	rf.persist()

	rf.heartbeatTimer.Reset(15)
	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func time_sleep_millsecond(t int) {
	time.Sleep(time.Duration(t) * time.Millisecond)
}
func (rf *Raft) heartbeat_sender() {
	//进入时有着当选时的锁
	cur_term := rf.current_term
	rf.mu.Unlock()
	for {
		//heartbeat需要睡觉，不然疯狂占锁会寄！
		<-rf.heartbeatTimer.C

		//time_sleep_millsecond(50)
		rf.mu.Lock()
		if rf.identity != Enum_Leader || rf.current_term != cur_term {
			rf.mu.Unlock()
			return
		}
		if rf.commit_index < len(rf.log)-1 {
			cnt_for_next_idx := 1
			for idx := range rf.peers {
				if idx == rf.me {
					continue
				}
				if rf.matches_indexes[idx] > rf.commit_index {
					cnt_for_next_idx++
				}
			}
			if cnt_for_next_idx > len(rf.peers)/2 {
				rf.commit_index++ //无论何时更新commit index都要通知applyCV！
				rf.applyCV.Signal()
				//fmt.Printf("leader %d commit_index update to %d\n", rf.me, rf.commit_index)
			}
		}

		rf.mu.Unlock()

		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			go func(idx int) {

				rf.mu.Lock()

				if cur_term != rf.current_term {
					rf.mu.Unlock()
					return
				}

				the_last_idx := rf.log[len(rf.log)-1].Index
				//fmt.Printf("%d\n", rf.next_indexes[idx])
				args := &AppendEntriesArgs{
					Term:           cur_term,
					Leader_id:      rf.me,
					Prev_log_index: rf.log[rf.next_indexes[idx]-1].Index,
					Prev_log_term:  rf.log[rf.next_indexes[idx]-1].Term,
					Log_entries:    rf.log[rf.next_indexes[idx]:len(rf.log)],
					Commit_index:   rf.commit_index,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(idx, args, reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if cur_term != rf.current_term {
					return
				}

				if reply.Term > rf.current_term {
					rf.current_term = reply.Term
					rf.voted_for = -1
					rf.votes = 0
					rf.identity = Enum_Follower
					rf.persist()
					return
				}

				if reply.Success {
					rf.next_indexes[idx] = max(rf.next_indexes[idx], the_last_idx+1)
					rf.matches_indexes[idx] = max(rf.matches_indexes[idx], the_last_idx)
				} else if reply.Index_expect > 0 && reply.Index_expect < rf.next_indexes[idx] {
					rf.next_indexes[idx] = reply.Index_expect
				}
			}(idx)
		}
		rf.setHeartBeatTimer(50)
	}
}

// 一个server尝试进行选举
func (rf *Raft) start_election() {

	rf.mu.Lock()
	//检查是否仍然是Candidate
	if rf.identity != Enum_Candidate {
		rf.mu.Unlock()
		return
	}
	//fmt.Printf("%d 在term %d开始竞选\n", rf.me, rf.current_term)
	var last_log_term int
	if len(rf.log) == 0 {
		last_log_term = -1
	} else {
		last_log_term = rf.log[len(rf.log)-1].Term
	}
	args := &RequestVoteArgs{
		Term:           rf.current_term,
		Candidate_id:   rf.me,
		Last_log_index: len(rf.log) - 1,
		Last_log_term:  last_log_term,
	}
	rf.votes = 1
	rf.mu.Unlock()

	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		//异步尝试获取一台服务器的选票
		go func(idx int) {

			reply := RequestVoteReply{}
			res := rf.sendRequestVote(idx, args, &reply)
			if !res {
				return
			}
			{
				rf.mu.Lock()
				// if reply.Vote_granted {
				// 	fmt.Printf("%d receive vote from %d on term %d\n", rf.me, idx, reply.Term)
				// } else {
				// 	fmt.Printf("%d receive a reject vote from %d on term %d while %d in term %d with reasion %d\n", rf.me, idx, args.Term, idx, reply.Term, reply.Reject_reasion)
				// }

				if rf.current_term > args.Term {
					rf.mu.Unlock()
					return
				}

				if rf.current_term < reply.Term {
					rf.current_term = reply.Term
					rf.voted_for = -1
					rf.identity = Enum_Follower
					rf.persist()
					//有更加新的term，变回follower
					rf.mu.Unlock()
					return
				}

				if rf.current_term != args.Term || rf.identity != Enum_Candidate || !reply.Vote_granted {
					rf.mu.Unlock()
					return
				}

				rf.votes++
				if rf.votes > len(rf.peers)/2 {
					//当选成功
					//fmt.Printf("%d become leader in term %d\n", rf.me, rf.current_term)
					rf.identity = Enum_Leader
					go rf.heartbeat_sender() //内部Unlock
				} else {
					rf.mu.Unlock()
				}
				return
			}
		}(i)

	}
}

func (rf *Raft) ticker() {

	rf.mu.Lock()
	rf.receive_heartbeat = true
	rf.mu.Unlock()

	go rf.ServerCommitListener()

	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		// 判断是否收到心跳
		if !rf.receive_heartbeat && rf.identity != Enum_Leader {
			rf.current_term++            //必须在这里自增任期
			rf.identity = Enum_Candidate //变为候选人
			rf.voted_for = rf.me         //给自己投票
			rf.persist()
			rf.votes = 1
			rf.mu.Unlock()
			rf.start_election()
			rf.mu.Lock()
			rf.receive_heartbeat = true //重置过期时间
			//fmt.Printf("%d start election on term%d \n", rf.me, rf.current_term)
			rf.mu.Unlock()
			time_sleep_millsecond(100)
		} else {
			rf.receive_heartbeat = false
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.commit_index = 0
	rf.current_term = 0
	rf.dead = 0
	rf.identity = Enum_Follower
	rf.last_applied = 0
	rf.receive_heartbeat = true
	rf.voted_for = -1
	rf.votes = 0
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0})
	rf.applyCh = applyCh
	rf.applyCV = *sync.NewCond(&rf.mu)
	rf.next_indexes = make([]int, len(peers))
	for i := range rf.next_indexes {
		rf.next_indexes[i] = 1
	}
	rf.matches_indexes = make([]int, len(peers))
	rf.heartbeatTimer = time.NewTimer(0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
