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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
const (
	Follower = iota
	Candidater
	Leader
)

const (
	HeartBeatTimeOut = 75
	ElectTimeOutBase = 500

	ElectTimeOutCheckInterval = time.Duration(300) * time.Millisecond // 检查是否超时的间隔
	CommitCheckTimeInterval   = time.Duration(100) * time.Millisecond // 检查是否可以commit的间隔,,,,
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on server
	currentTerm int        // 记录当前的任期
	votedFor    int        // 投给了谁
	log         []logEntry // 日志条目数组

	//Volatile state on server
	commitIndex int // 已知的被提交的日志索引
	lastApplied int // 已apply的日志索引

	//Volatile state on leader
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	//自定义变量
	timeTick time.Time // 记录收到消息的时间(心跳或append)
	role     int

	muVote    sync.Mutex // 保护投票数据
	voteCount int

	applyCh chan ApplyMsg

	//2D
	snapShot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

type logEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) RealLogIdx(vIdx int) int {
	// 调用该函数需要是加锁的状态
	return vIdx - rf.lastIncludedIndex
}

func (rf *Raft) VirtualLogIdx(rIdx int) int {
	// 调用该函数需要是加锁的状态
	return rIdx + rf.lastIncludedIndex
}

// type timer struct {
// 	timeTicker *time.Ticker
// }

// func (t *timer) reset() {
// 	randomTime := time.Duration(150+rand.Intn(200)) * time.Millisecond // 200~350ms
// 	t.timeTicker.Reset(randomTime)                                     // 重置时间
// }

// func (t *Timer) resetHeartBeat() {
// 	t.timer.Reset(HeartBeatTimeout)
// }

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.role == Leader)

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
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	//2D
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var lastIncludedIndex, lastIncludedTerm int
	var log []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		//   error...
		DPrintf(" server %d decode error!\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("server %d 成功读取持久化 currentTerm %v votedFor %v log %v", rf.me, currentTerm, votedFor, log)
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		// DPrintf("server %d 读取快照失败 nil \n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %d 读取快照成功 \n", rf.me)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	var X = rf.RealLogIdx(index)
	DPrintf("server %d 准备snapshot index = %d, %v\n", rf.me, index, snapshot)
	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapShot = snapshot
	rf.lastIncludedIndex = rf.log[X].Term //lock

	rf.log = rf.log[X:]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.muVote.Lock()
		rf.voteCount = 0
		rf.votedFor = -1
		rf.muVote.Unlock()
		rf.persist()
	}

	//校验PrevLogIndex和PrevLogTerm不合法
	// if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
	// 	(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
	// 	rf.role = Follower
	// 	rf.muVote.Lock()
	// 	rf.voteCount = 0
	// 	rf.votedFor = args.CandidateId
	// 	rf.muVote.Unlock()
	// }

	if rf.votedFor == -1 &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.VirtualLogIdx(len(rf.log)-1))) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.persist()
		rf.timeTick = time.Now()
		DPrintf("server %v 同意向 server %v投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
		return
	}

	DPrintf("server %v 拒绝向 server %v投票: 已投票 %v\n\targs= %+v\n", rf.me, args.CandidateId, rf.votedFor, args)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term             int
	Success          bool
	ReplyCommitIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.timeTick = time.Now()

	rfCurrentTerm := rf.currentTerm
	if args.Term > rfCurrentTerm {
		rf.currentTerm = args.Term
		rf.muVote.Lock()
		rf.votedFor = -1
		rf.muVote.Unlock()
		rf.persist()
		rf.role = Follower
	}

	if args.Entries == nil {
		// 心跳函数
		DPrintf("server %v 接收到 leader %v 的心跳\n", rf.me, args.LeaderId)
	} else {
		DPrintf("server %v 收到 leader %v 的的AppendEntries: %+v \n", rf.me, args.LeaderId, args)
	}
	//to do
	//校验条目d
	virtualLen := rf.VirtualLogIdx(len(rf.log))
	if args.PrevLogIndex >= virtualLen || rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Term = rfCurrentTerm
		if args.PrevLogIndex >= virtualLen {
			DPrintf("server %v 检查到心跳中参数不合法:\n\t args.PrevLogIndex=%v, args.PrevLogTerm=%v, \n\tlen(self.log)=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.VirtualLogIdx(len(rf.log)))
		} else {
			DPrintf("server %v 检查到心跳中参数不合法:\n\t args.PrevLogIndex=%v, args.PrevLogTerm=%v, \n\tlen(self.log)=%v, self此位置term为:%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.VirtualLogIdx(len(rf.log)), rf.log[args.PrevLogIndex].Term)
		}
		reply.Success = false
		reply.ReplyCommitIndex = virtualLen - 1
		if reply.ReplyCommitIndex > args.PrevLogIndex {
			reply.ReplyCommitIndex = args.PrevLogIndex
		}
		if args.PrevLogIndex >= virtualLen {
			return
		}
		realReplyCommitIndex := rf.RealLogIdx(reply.ReplyCommitIndex)
		curTerm := rf.log[realReplyCommitIndex].Term
		for realReplyCommitIndex >= 0 {
			if rf.log[realReplyCommitIndex].Term == curTerm {
				reply.ReplyCommitIndex--
			} else {
				break
			}
		}
		return
	}

	if args.Entries != nil {

		rf.log = rf.log[:rf.RealLogIdx(args.PrevLogIndex)+1]

		rf.log = append(rf.log, args.Entries...)

		rf.persist()
		DPrintf("server %v 成功进行apeend\n", rf.me)
		DPrintf("server %v 现有log\n", rf.log)
	}

	//to do
	//校验条目
	if args.LeaderCommit > rf.commitIndex {
		// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(virtualLen-1)))

	}

	reply.Success = true
	reply.Term = rfCurrentTerm
}

//InstallSnapshot RPC
//2D

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
	LastIncludedCmd   logEntry
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer func() {
		rf.timeTick = time.Now()
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	hasEntry := false
	rIdx := 0
	for ; rIdx < len(rf.log); rIdx++ {
		if rf.VirtualLogIdx(rIdx) == args.LastIncludedIndex && rf.log[rIdx].Term == args.LastIncludedIndex {
			hasEntry = true
			break
		}
	}
	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if hasEntry {
		DPrintf("server %v InstallSnapshot: args.LastIncludedIndex= %v 位置存在, 保留后面的log\n", rf.me, args.LastIncludedIndex)
		rf.log = rf.log[rIdx:]
	} else {
		DPrintf("server %v InstallSnapshot: 清空log\n", rf.me)
		rf.log = make([]logEntry, 0)
		rf.log = append(rf.log, args.LastIncludedCmd)
	}
	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = args.Term
	rf.applyCh <- *msg
	rf.persist()

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.role == Leader)
	term = rf.currentTerm
	if !isLeader {
		return index, term, isLeader
	}
	newEntry := &logEntry{Term: term, Command: command}
	DPrintf("start 命令 %+v", newEntry)
	rf.log = append(rf.log, *newEntry)
	rf.persist()
	virtualLen := rf.VirtualLogIdx(len(rf.log))
	index = virtualLen - 1
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

func (rf *Raft) CommitChecker() {
	for !rf.killed() {
		rf.mu.Lock()
		var msgBuf = make([]*ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.RealLogIdx(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
				SnapshotTerm: rf.log[rf.RealLogIdx(rf.lastApplied)].Term,
			}
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机\n", rf.me, msg.Command, msg.CommandIndex)
			msgBuf = append(msgBuf, &msg)
			DPrintf("server %v 目前log : %v, len: %d\n", rf.me, rf.log, rf.VirtualLogIdx(len(rf.log)))
		}
		rf.mu.Unlock()
		for _, msg := range msgBuf {
			rf.applyCh <- *msg
		}

		time.Sleep(CommitCheckTimeInterval)
	}
}

func (rf *Raft) ticker() {
	random := rand.New(rand.NewSource(int64(rf.me)))
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		randomTime := GetRandomElectTimeOut(random)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.timeTick) > time.Duration(randomTime)*time.Millisecond {
			go func() {
				rf.mu.Lock()

				rf.currentTerm += 1
				rf.role = Candidater
				rf.votedFor = rf.me
				rf.persist()
				rf.voteCount = 1
				rf.timeTick = time.Now()

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.VirtualLogIdx(len(rf.log)) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				rf.mu.Unlock()
				DPrintf("Server %v 参与竞选， args:%+v\n", rf.me, args)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.collectVote(i, &args)
				}

			}()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func GetRandomElectTimeOut(rd *rand.Rand) int {
	plusMs := int(rd.Float64() * 500.0)

	return plusMs + ElectTimeOutBase
}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	DPrintf("Server %v 请求选票 %v\n", rf.me, serverTo)
	var voteGranted bool
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverTo, args, &reply)
	if !ok {
		voteGranted = false
	}

	rf.mu.Lock()

	if args.Term != rf.currentTerm {
		voteGranted = false
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.role = Follower
	}

	rf.mu.Unlock()
	voteGranted = reply.VoteGranted

	if !voteGranted {
		return
	}
	rf.muVote.Lock()
	defer rf.muVote.Unlock()
	if rf.voteCount > len(rf.peers)/2 {
		return
	}

	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role == Follower {
			return
		}
		rf.role = Leader

		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}

		go rf.sendHeartBeats()
	}
}

func (rf *Raft) sendHeartBeats() {
	DPrintf("Server %v 开始发送心跳\n", rf.me)

	for !rf.killed() {
		rf.mu.Lock()

		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			DPrintf("server %d to %d rf.nextIndex[i] : %d", rf.me, i, rf.nextIndex[i])
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}

			virtuallen := rf.VirtualLogIdx(len(rf.log) - 1)

			if args.PrevLogIndex < rf.lastIncludedIndex {
				DPrintf("leader %v 向 server %v 发送sendInstallSnapshot, lastIncludedIndex=%v, nextIndex[%v]=%v, args = %+v \n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args)
				go func(i int) {
					reply := &InstallSnapshotReply{}
					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					args := &InstallSnapshotRequest{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              rf.snapShot,
						LastIncludedCmd:   rf.log[0],
					}
					rf.mu.Unlock()
					ok := rf.sendInstallSnapshot(i, args, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
						rf.timeTick = time.Now()
						rf.persist()
						return
					}
					rf.nextIndex[i] = rf.VirtualLogIdx(1)
				}(i)
				continue
			}
			if virtuallen >= rf.nextIndex[i] {
				args.Entries = rf.log[rf.RealLogIdx(rf.nextIndex[i]):]
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries\n", rf.me, i)
			} else {
				args.Entries = nil
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, i, args)
			}
			args.PrevLogTerm = rf.log[rf.RealLogIdx(rf.nextIndex[i]-1)].Term
			go func(i int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					DPrintf("server %v leader 接收不到 server %v 的消息term: %v\n", rf.me, i, reply)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				virtualLen := rf.VirtualLogIdx(len(rf.log))
				if args.Term != rf.currentTerm {
					return
				}
				DPrintf("server %v leader收到了server %v 心跳函数term: %v\n", rf.me, i, reply)
				if reply.Success {

					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1

					toCommitIndex := virtualLen
					for toCommitIndex > rf.commitIndex {
						count := 1
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								continue
							}
							if rf.matchIndex[i] >= toCommitIndex && rf.log[rf.RealLogIdx(toCommitIndex)].Term == rf.currentTerm {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = toCommitIndex
							break
						}
						toCommitIndex--
					}
					return
				}

				if reply.Term > rf.currentTerm {
					DPrintf("server %v 旧的leader收到了心跳函数中更新的term: %v, 转化为Follower\n", rf.me, reply.Term)
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					rf.persist()
					rf.voteCount = 0
					rf.timeTick = time.Now()
					return
				}

				if reply.Term == rf.currentTerm && rf.role == Leader {
					// term仍然相同, 且自己还是leader, 表名对应的follower在prevLogIndex位置没有与prevLogTerm匹配的项
					// 将nextIndex自减再重试

					// rf.nextIndex[i]--
					// if rf.nextIndex[i] > reply.LastLogIndex+1 && reply.LastLogIndex != 0 {
					// 	rf.nextIndex[i] = reply.LastLogIndex + 1
					// }

					rf.nextIndex[i] = reply.ReplyCommitIndex + 1

					if rf.nextIndex[i] > virtualLen {
						rf.nextIndex[i] = virtualLen
					}

					return
				}

			}(i)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 0)
	rf.log = append(rf.log, logEntry{})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapShot = make([]byte, 0)

	rf.role = Follower
	rf.voteCount = 0
	rf.timeTick = time.Now()

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CommitChecker()

	return rf
}
