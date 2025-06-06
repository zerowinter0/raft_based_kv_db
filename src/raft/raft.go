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
	"log"
	"math/rand"
	"os"
	"strconv"
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

	//mine
	CommandTerm int

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
	current_term    int
	voted_for       int
	log             []LogEntry
	identity        Identity
	commit_index    int
	last_applied    int
	applyCh         chan ApplyMsg
	applyCV         sync.Cond
	heartbeatTimer  *time.Timer
	heartbeatSignal chan struct{}
	snapshot        []byte

	//used for time out
	receive_heartbeat bool

	//below is only for leader, is a server becomes a leader, it should initialize these status
	next_indexes      []int
	matches_indexes   []int
	votes             int
	able_commit_index int

	//use for fuzz
	is_fuzzing bool
	fuzz_rand  rand.Source
	rf_logger  *log.Logger
	fuzz_score int
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) get_fuzz_socre() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.fuzz_score

}

func Assert(tmp bool, msg string) {
	if !tmp {
		log.Fatalf(msg)
	}
}

func (rf *Raft) setHeartBeatTimer(t int) {
	rf.mu.Lock()
	rf.heartbeatTimer.Reset(time.Duration(t) * time.Millisecond)
	rf.mu.Unlock()
}

func (rf *Raft) last_log() LogEntry {
	return rf.log[len(rf.log)-1]
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

func (rf *Raft) getLeaderCurrentLog(term int) (bool, interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.current_term || rf.identity != Enum_Leader {
		return false, rf.log
	}
	return true, rf.log
}

func (rf *Raft) getCurrentLog() interface{} {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

// func (rf *Raft)

func (rf *Raft) checkLegalTwo(old_log_data interface{}, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.current_term || rf.identity != Enum_Leader {
		return
	}
	old_log := old_log_data.([]LogEntry)
	for idx := range old_log {
		Assert(rf.log[idx].Index == old_log[idx].Index && rf.log[idx].Term == old_log[idx].Term, "checkLegalTwo False")
	}
}

func (rf *Raft) checkLegalThree(other_log_data interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	other_log := other_log_data.([]LogEntry)
	still_same := true
	for i := 0; i < min(len(other_log), len(rf.log)); i++ {
		if other_log[i].Index == rf.log[i].Index && other_log[i].Term == rf.log[i].Term {
			Assert(other_log[i].Msg == rf.log[i].Msg, "checkLegalThree False 1")
			Assert(still_same, "checkLegalThree False 2")
		} else {
			still_same = false
		}
	}
}

func (rf *Raft) checkLegalFour(other_log_data map[int]interface{}, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity != Enum_Leader || term > rf.current_term {
		return
	}
	for i := 0; i < len(rf.log) && rf.log[i].Index < len(other_log_data); i++ {
		Assert(other_log_data[rf.log[i].Index] == rf.log[i].Msg, "checkLegalFour False 2")
	}
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
	//持久化时要持有锁！
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_term)
	e.Encode(rf.voted_for)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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
		rf.commit_index = rf.log[0].Index
		rf.last_applied = rf.log[0].Index
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	// rf.rf_logger.Printf("%d make Snapshot begin with index %d\n", rf.me, index)
	// defer rf.rf_logger.Printf("%d make Snapshot end with index %d\n", rf.me, index)
	defer rf.mu.Unlock()
	// Your code here (3D).
	if index <= rf.log[0].Index {
		// 这个snapshot太老了
		return
	}

	Assert(index <= rf.commit_index, "snapshot中有没commit的log")
	rf.last_applied = max(rf.last_applied, index)
	rf.log = rf.log[index-rf.log[0].Index:]
	rf.snapshot = snapshot

	rf.persist()
}

// field names must start with capital letters!
type InstallSnapshotArgs struct {
	Term                int
	Leader_id           int
	Last_included_index int
	Last_included_term  int
	Last_included_cmd   interface{}
	Data                []byte
}

// example InstallSnapshot RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	Term int
}

// example InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// rf.rf_logger.Printf("%d InstallSnapshot begin with index %d\n", rf.me, args.Last_included_index)
	// defer rf.rf_logger.Printf("%d InstallSnapshot end and current log[0].index is %d\n", rf.me, rf.log[0].Index)
	reply.Term = rf.current_term
	if args.Term < rf.current_term {
		rf.mu.Unlock()
		return
	}
	if rf.current_term < args.Term {
		rf.current_term = args.Term
		rf.voted_for = -1
		rf.persist()
	}
	rf.identity = Enum_Follower
	rf.receive_heartbeat = true
	Assert(rf.last_log().Index >= rf.commit_index, "node commit something from void?")
	if args.Last_included_index <= rf.log[0].Index {
		// 这个snapshot太老了
		rf.mu.Unlock()
		return
	}

	// if log has 'Last_included_log', then contain logs following it.
	if args.Last_included_index < rf.last_log().Index && rf.log[args.Last_included_index-rf.log[0].Index].Term == args.Last_included_term {
		rf.log = rf.log[args.Last_included_index-rf.log[0].Index:]
	} else {
		rf.log = make([]LogEntry, 0)
		rf.log = append(rf.log, LogEntry{Index: args.Last_included_index, Term: args.Last_included_term, Msg: args.Last_included_cmd})
	}

	rf.commit_index = max(rf.commit_index, args.Last_included_index)
	rf.last_applied = max(rf.last_applied, args.Last_included_index)

	rf.snapshot = args.Data
	msg := &ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.Last_included_term,
		SnapshotIndex: args.Last_included_index,
	}
	rf.persist()
	rf.receive_heartbeat = true
	rf.mu.Unlock()
	rf.applyCh <- *msg
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
	//rf.rf_logger.Printf("%d receive a request vote from %d on term %d!!!\n", rf.me, args.Candidate_id, args.Term)
	rf.mu.Lock()
	// rf.rf_logger.Printf("%d RequestVote from %d begin\n", rf.me, args.Candidate_id)
	// defer rf.rf_logger.Printf("%d RequestVote from %d end\n", rf.me, args.Candidate_id)
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
		rf.voted_for = -1
		rf.identity = Enum_Follower
		rf.persist()
	}
	last_entry := rf.last_log()
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
	Index_expect  int
	Conflict_term int
}

// 实现AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// rf.rf_logger.Printf("%d AppendEntries begin\n", rf.me)
	// defer rf.rf_logger.Printf("%d AppendEntries end\n", rf.me)
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
			rf.rf_logger.Printf("term %d 中,有两个leader:%d %d\n", args.Term, rf.me, args.Leader_id)
			Assert(false, "damn")
		}
		if args.Term > rf.current_term {
			rf.current_term = args.Term
			rf.persist()
		}
		rf.identity = Enum_Follower
		rf.receive_heartbeat = true
	}
	last_log := rf.last_log()

	//leaders' logs are too new or too old
	if last_log.Index < args.Prev_log_index || args.Prev_log_index < rf.log[0].Index {
		reply.Term = rf.current_term
		reply.Success = false
		reply.Index_expect = last_log.Index + 1

		return
	}

	if rf.log[args.Prev_log_index-rf.log[0].Index].Term != args.Prev_log_term {
		reply.Term = rf.current_term
		reply.Success = false
		reply.Index_expect = args.Prev_log_index //上一位冲突了，期望获取上一位的数据
		reply.Conflict_term = rf.log[args.Prev_log_index-rf.log[0].Index].Term
		//这一位的index应该大于commit index
		if args.Prev_log_index <= rf.commit_index {
			rf.rf_logger.Printf("%d 日志系统异常,已提交日志%d与leader冲突,自己这位的term为%d,leader的term为%d\n", rf.me, args.Prev_log_index, rf.log[args.Prev_log_index-rf.log[0].Index].Term, args.Prev_log_term)
			Assert(false, "damn")
		}
		return
	}

	var rf_log_changed = false
	//接收请求
	for _, log_entry := range args.Log_entries {
		if rf.last_log().Index+1 <= log_entry.Index {
			if rf.last_log().Index+1 != log_entry.Index {
				rf.rf_logger.Printf("日志系统异常,accept了一个超过最新log的log\n")
				Assert(false, "damn")
			}
			rf.log = append(rf.log, log_entry)
			rf_log_changed = true
		} else {
			if rf.log[log_entry.Index-rf.log[0].Index].Term != log_entry.Term {
				//这一位的index应该大于commit index
				if log_entry.Index <= rf.commit_index {
					rf.rf_logger.Printf("%d 日志系统异常,已提交日志%d被重写\n", rf.me, log_entry.Index)
					Assert(false, "damn")
				}

				//内容有冲突，删除冲突index及之后的所有内容
				rf.log = rf.log[:log_entry.Index-rf.log[0].Index]

				rf.log = append(rf.log, log_entry)

				rf_log_changed = true

				rf.fuzz_score++
			}
		}
	}
	if rf_log_changed {
		rf.persist()
	}
	if args.Commit_index > rf.commit_index {
		new_commit_index := min(args.Commit_index, rf.last_log().Index)
		Assert(rf.commit_index <= new_commit_index, "new_commit_index小于commit_index")
		if rf.commit_index < new_commit_index { //有可能leader的commitindex更后，但这条信息是一个心跳，没有实质的commit index更新
			//Assert(rf.log[new_commit_index-rf.log[0].Index].Term == rf.current_term, "follower commit an old log!")
			rf.commit_index = new_commit_index
			//应用于状态机
			rf.applyCV.Signal()
			rf.rf_logger.Printf("follower %d commit_index update to %d\n", rf.me, rf.commit_index)
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
		msgs := make([]*ApplyMsg, 0)
		for rf.last_applied < rf.commit_index {
			msg := &ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.last_applied+1-rf.log[0].Index].Msg,
				CommandIndex:  rf.last_applied + 1,
				CommandTerm:   rf.current_term,
				SnapshotValid: false,
			}
			msgs = append(msgs, msg)
			rf.last_applied++
		}
		if len(msgs) > 0 {
			rf.mu.Unlock()
			for _, msg := range msgs {
				rf.applyCh <- *msg
			}
			rf.mu.Lock()
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	index = rf.last_log().Index + 1
	term = rf.current_term

	//如果不是leader，直接返回
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	rf.rf_logger.Printf("%d leader receive cmd %d\n", rf.me, index)

	log_entry := LogEntry{
		Index: index,
		Term:  term,
		Msg:   command,
	}

	rf.log = append(rf.log, log_entry)
	rf.persist()
	select {
	case rf.heartbeatSignal <- struct{}{}:
	default:
	}

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

// require rf.Lock()
func (rf *Raft) check_may_commit() {
	if max(rf.commit_index, rf.able_commit_index-1) < rf.last_log().Index {
		cnt_for_next_idx := 1
		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			if rf.matches_indexes[idx] > max(rf.commit_index, rf.able_commit_index-1) {
				cnt_for_next_idx++
			}
		}
		if cnt_for_next_idx > len(rf.peers)/2 {
			rf.commit_index = max(rf.commit_index, rf.able_commit_index-1) + 1 //无论何时更新commit index都要通知applyCV！
			Assert(rf.log[rf.commit_index-rf.log[0].Index].Term == rf.current_term, "leader commit an old log!")
			rf.applyCV.Signal()
			rf.rf_logger.Printf("leader %d commit_index update to %d\n", rf.me, rf.commit_index)
		}
	}
}
func (rf *Raft) heartbeat_sender() {
	//进入时有着当选时的锁
	cur_term := rf.current_term
	for {
		//heartbeat需要睡觉，不然疯狂占锁会寄！
		if rf.identity != Enum_Leader || rf.current_term != cur_term || rf.killed() {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			go func(idx int) {

				rf.mu.Lock()
				//rf.rf_logger.Printf("%d heartbeat to %d begin in term %d %d\n", rf.me, idx, cur_term, rf.current_term)
				//defer rf.rf_logger.Printf("%d heartbeat to %d end in term %d %d\n", rf.me, idx, cur_term, rf.current_term)
				if cur_term != rf.current_term || rf.identity != Enum_Leader {
					rf.mu.Unlock()
					return
				}

				if rf.next_indexes[idx] <= rf.log[0].Index { //要发Snapshot
					Assert(rf.log[0].Index > 0, "没有snapshot但需求snapshot?")
					args := &InstallSnapshotArgs{
						Term:                rf.current_term,
						Leader_id:           rf.me,
						Last_included_index: rf.log[0].Index,
						Last_included_term:  rf.log[0].Term,
						Last_included_cmd:   rf.log[0].Msg,
						Data:                rf.snapshot,
					}

					reply := &InstallSnapshotReply{}
					rf.mu.Unlock()
					res := rf.sendInstallSnapshot(idx, args, reply)
					if res {
						rf.mu.Lock()
						if cur_term != rf.current_term {
							rf.mu.Unlock()
							return
						}
						if reply.Term > rf.current_term {
							rf.current_term = reply.Term
							rf.identity = Enum_Follower
							rf.voted_for = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}
						rf.matches_indexes[idx] = max(rf.matches_indexes[idx], args.Last_included_index)
						rf.next_indexes[idx] = rf.matches_indexes[idx] + 1
						rf.check_may_commit()
						rf.mu.Unlock()
						return
					}
					return
				}

				the_last_idx := rf.last_log().Index
				args := &AppendEntriesArgs{
					Term:           cur_term,
					Leader_id:      rf.me,
					Prev_log_index: rf.log[rf.next_indexes[idx]-1-rf.log[0].Index].Index,
					Prev_log_term:  rf.log[rf.next_indexes[idx]-1-rf.log[0].Index].Term,
					Log_entries:    rf.log[rf.next_indexes[idx]-rf.log[0].Index:],
					Commit_index:   rf.commit_index,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				res := rf.sendAppendEntries(idx, args, reply)

				if !res {
					return
				}

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
					rf.check_may_commit()
				} else if reply.Index_expect > 0 && reply.Index_expect < rf.next_indexes[idx] {
					//normal back
					//rf.next_indexes[idx] = reply.Index_expect

					//quick back
					tmp := args.Prev_log_index
					for tmp > rf.log[0].Index && rf.log[tmp-rf.log[0].Index].Term >= reply.Conflict_term {
						tmp--
					}
					rf.next_indexes[idx] = tmp
				}
			}(idx)
		}

		select {
		case <-rf.heartbeatTimer.C:
			// 定时器到期，继续下一个循环
			//fmt.Printf("not fast!\n")
		case <-rf.heartbeatSignal:
			//fmt.Printf("fast!\n")
			if !rf.heartbeatTimer.Stop() {
				<-rf.heartbeatTimer.C // 确保定时器通道被清空
			}
			//提前终止
		}

		rf.mu.Lock()
		rf.heartbeatTimer.Reset(time.Duration(50) * time.Millisecond)
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
	rf.rf_logger.Printf("%d 在term %d开始竞选\n", rf.me, rf.current_term)
	args := &RequestVoteArgs{
		Term:           rf.current_term,
		Candidate_id:   rf.me,
		Last_log_index: rf.last_log().Index,
		Last_log_term:  rf.last_log().Term,
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
				if reply.Vote_granted {
					rf.rf_logger.Printf("%d receive vote from %d on term %d\n", rf.me, idx, reply.Term)
				} else {
					rf.rf_logger.Printf("%d receive a reject vote from %d on term %d while %d in term %d with reasion %d\n", rf.me, idx, args.Term, idx, reply.Term, reply.Reject_reasion)
				}

				if rf.current_term > args.Term || rf.identity != Enum_Candidate {
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
					rf.fuzz_score++
					rf.rf_logger.Printf("%d become leader in term %d\n", rf.me, rf.current_term)
					rf.able_commit_index = rf.last_log().Index + 1
					rf.identity = Enum_Leader
					for i := range rf.peers {
						rf.next_indexes[i] = rf.log[0].Index + 1
						rf.matches_indexes[i] = rf.log[0].Index
					}
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
			//rf.rf_logger.Printf("%d start election on term%d \n", rf.me, rf.current_term)
			rf.mu.Unlock()
			time_sleep_millsecond(100)
		} else {
			rf.receive_heartbeat = false
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		var ms int64
		if rf.is_fuzzing {
			ms = 150 + (rf.fuzz_rand.Int63() % 150)
		} else {
			ms = 150 + (rand.Int63() % 150)
		}
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
	persister *Persister, applyCh chan ApplyMsg, seed ...int64) *Raft {
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
	rf.heartbeatSignal = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.snapshot

	if len(seed) > 0 {
		rf.is_fuzzing = true
		rf.fuzz_rand = rand.New(rand.NewSource(seed[0]))
	} else {
		//Assert(false, "?")
	}
	file, err := os.OpenFile("app"+strconv.Itoa(me)+".log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("无法打开日志文件: %v", err)
	}

	// 2. 创建一个新的 Logger，指定输出为文件
	rf.rf_logger = log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	rf.rf_logger.Printf("%d start\n", me)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
