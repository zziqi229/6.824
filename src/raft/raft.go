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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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

// type Snapshot struct {
// 	LastIncludeIndex int
// 	LastIncludeTerm  int
// 	Data             []byte
// }

type ServerState int

const (
	followerState  ServerState = 1
	leaderState    ServerState = 2
	candidateState ServerState = 3
)

func (state ServerState) String() string {
	if leaderState == state {
		return "leader"
	} else if followerState == state {
		return "follower"
	} else if candidateState == state {
		return "candidate"
	}
	return "nilstate"
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	serverState ServerState
	currentTerm int
	voteFor     int
	log         *Log

	snapshot                 []byte
	snapshotLastIncludeIndex int
	snapshotLastIncludeTerm  int

	//Volatile state
	commitIndex int
	lastApplied int

	//Volatile state on leader
	nextIndex  []int
	matchIndex []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyHelper *ApplyHelper
	applyCond   *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.serverState == leaderState
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	state := new(bytes.Buffer)
	e := labgob.NewEncoder(state)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIncludeIndex)
	e.Encode(rf.snapshotLastIncludeTerm)

	if rf.snapshotLastIncludeIndex > 0 {
		rf.persister.SaveStateAndSnapshot(state.Bytes(), rf.snapshot)
	} else {
		rf.persister.SaveRaftState(state.Bytes())
	}
	// DPrintf(0, "%v: persist rf.currentTerm=%v rf.voteFor=%v rf.log=%v\n", rf.SayMeL(), rf.currentTerm, rf.voteFor, rf.log)
}

// restore previously persisted state.
func (rf *Raft) readPersist() {
	stateData := rf.persister.ReadRaftState()
	if stateData != nil && len(stateData) > 0 { // bootstrap without any state?
		r := bytes.NewBuffer(stateData)
		d := labgob.NewDecoder(r)
		// var currentTerm int
		// var voteFor int
		// var log *Log
		rf.voteFor = 0 // in case labgob waring
		if d.Decode(&rf.currentTerm) != nil ||
			d.Decode(&rf.voteFor) != nil ||
			d.Decode(&rf.log) != nil ||
			d.Decode(&rf.snapshotLastIncludeIndex) != nil ||
			d.Decode(&rf.snapshotLastIncludeTerm) != nil {
			//   error...
			DPrintf(999, "%v: readPersist decode error\n", rf.SayMeL())
			panic("")
		}
	}
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.commitIndex = rf.snapshotLastIncludeIndex
	rf.lastApplied = rf.snapshotLastIncludeIndex
	// if snapshotData != nil && len(snapshotData) > 0 {
	// 	r := bytes.NewBuffer(snapshotData)
	// 	d := labgob.NewDecoder(r)
	// 	var snapshot Snapshot
	// 	if d.Decode(&snapshot) != nil {
	// 		//   error...
	// 		DPrintf(999, "%v: readPersist decode error\n", rf.SayMeL())
	// 		panic("")
	// 	} else {
	// 		rf.snapshot = snapshot
	// 		// rf.commitIndex = rf.snapshot.LastIncludeIndex
	// 		// rf.lastApplied = rf.snapshot.LastIncludeIndex
	// 	}
	// }
	// Your code here (2C).
	// Example:

	DPrintf(6, "%v: readPersist rf.currentTerm=%v rf.voteFor=%v rf.log=%v\n", rf.SayMeL(), rf.currentTerm, rf.voteFor, rf.log)
}

type RequestInstallSnapShotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}
type RequestInstallSnapShotReply struct {
	Term int
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != leaderState {
		return
	}
	DPrintf(11, "%v: come Snapshot index=%v", rf.SayMeL(), index)
	if rf.log.FirstLogIndex <= index {
		if index > rf.lastApplied {
			panic(fmt.Sprintf("%v: index=%v rf.lastApplied=%v\n", rf.SayMeL(), index, rf.lastApplied))
		}
		rf.snapshot = snapshot
		rf.snapshotLastIncludeIndex = index
		rf.snapshotLastIncludeTerm = rf.getEntryTerm(index)
		// Snapshot{
		// 	LastIncludeIndex: index,
		// 	LastIncludeTerm:  rf.getEntryTerm(index),
		// 	Data:             snapshot,
		// }
		newFirstLogIndex := index + 1
		if newFirstLogIndex <= rf.log.LastLogIndex {
			rf.log.Entries = rf.log.Entries[newFirstLogIndex-rf.log.FirstLogIndex:]
		} else {
			rf.log.LastLogIndex = newFirstLogIndex - 1
			rf.log.Entries = make([]Entry, 0)
		}
		rf.log.FirstLogIndex = newFirstLogIndex
		rf.commitIndex = max(rf.commitIndex, index)
		rf.lastApplied = max(rf.lastApplied, index)
		rf.persist()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.InstallSnapshot(i)
		}
		DPrintf(11, "%v: len(rf.log.Entries)=%v rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v rf.commitIndex=%v  rf.lastApplied=%v\n",
			rf.SayMeL(), len(rf.log.Entries), rf.log.FirstLogIndex, rf.log.LastLogIndex, rf.commitIndex, rf.lastApplied)
	}
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
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.NewTermL(args.Term)
		rf.SetServerStateL(followerState)
		rf.persist()
	}

	DPrintf(5, "%v: reply to %v myLastLogterm=%v myLastLogIndex=%v args.LastLogTerm=%v args.LastLogIndex=%v\n",
		rf.SayMeL(), args.CandidateId, rf.getLastEntryTerm(), rf.log.LastLogIndex, args.LastLogTerm, args.LastLogIndex)
	update := false
	update = update || args.LastLogTerm > rf.getLastEntryTerm()
	update = update || args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.log.LastLogIndex
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && update {
		rf.voteFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.SetServerStateL(followerState)
		rf.electionTimer.Reset(rf.getElectionTime())
		rf.persist()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	DPrintf(1, "%v: reply to %v reply.term=%v reply.VoteGranted=%v rf.voteFor=%v",
		rf.SayMeL(), args.CandidateId, reply.Term, reply.VoteGranted, rf.voteFor)
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
func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendRequestInstallSnapshot(server int, args *RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
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
	term = rf.currentTerm
	if rf.serverState != leaderState {
		isLeader = false
		return index, term, isLeader
	}
	index = rf.log.LastLogIndex + 1
	DPrintf(800, "%v: Come a command index=%v cmd=%T %v", rf.SayMeL(), index, command, command)
	rf.log.appendL(Entry{term, command})
	rf.persist()
	// rf.tryCommitL(rf.log.LastLogIndex)
	go rf.StartAppendEntries(false)
	// rf.heartbeatTimer.Reset(7 * time.Millisecond)
	// if rand.Intn(5) == 0 {
	// time.Sleep(7 * time.Millisecond)
	// }
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
func (rf *Raft) SayMeL() string {
	return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.serverState, rf.currentTerm)
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(100, "%v : is killed", rf.SayMeL())
	rf.applyHelper.Kill()
	rf.SetServerStateL(followerState)
}
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) Killed() bool {
	return rf.killed()
}
func (rf *Raft) NewTermL(term int) {
	DPrintf(850, "%v : from old term %v to new term %v\n", rf.SayMeL(), rf.currentTerm, term)
	rf.currentTerm, rf.voteFor = term, -1
}
func (rf *Raft) becomeLeaderL() {
	rf.SetServerStateL(leaderState)
	DPrintf(100, "%v :becomeleader\n", rf.SayMeL())
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.LastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type RequestAppendEntriesReply struct {
	Term    int
	Success bool

	PrevLogIndex int
	PrevLogTerm  int
}

func (rf *Raft) tryCommitL(index int) {
	if index <= rf.commitIndex {
		return
	}
	if index > rf.log.LastLogIndex {
		return
	}
	if index < rf.log.FirstLogIndex {
		return
	}
	if rf.getEntryTerm(index) != rf.currentTerm {
		return
	}

	cnt := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if index <= rf.matchIndex[i] {
			cnt++
		}
	}
	DPrintf(2, "%v: rf.commitIndex = %v ,trycommitindex=%v,matchindex=%v cnt=%v", rf.SayMeL(), rf.commitIndex, index, rf.matchIndex, cnt)
	if cnt > len(rf.peers)/2 {
		rf.commitIndex = index
		if rf.commitIndex > rf.log.LastLogIndex {
			DPrintf(999, "%v: commitIndex > lastlogindex %v > %v", rf.SayMeL(), rf.commitIndex, rf.log.LastLogIndex)
			panic("")
		}
		// DPrintf(500, "%v: commitIndex = %v ,entries=%v", rf.SayMeL(), rf.commitIndex, rf.log.Entries)
		DPrintf(199, "%v: rf.applyCond.Broadcast(),rf.lastApplied=%v rf.commitIndex=%v", rf.SayMeL(), rf.lastApplied, rf.commitIndex)
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf(11, "%v: RequestInstallSnapshot end  args.LeaderId=%v, args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	DPrintf(11, "%v: RequestInstallSnapshot begin  args.LeaderId=%v, args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.SetServerStateL(followerState)
	rf.electionTimer.Reset(rf.getElectionTime())
	if args.Term > rf.currentTerm {
		rf.NewTermL(args.Term)
	}
	defer rf.persist()
	if args.LastIncludeIndex > rf.snapshotLastIncludeIndex {
		// DPrintf(800, "%v: before install snapshot %s: rf.log.FirstLogIndex=%v, rf.log=%v", rf.SayMeL(), rf.SayMeL(), rf.log.FirstLogIndex, rf.log)
		rf.snapshot = args.Snapshot
		rf.snapshotLastIncludeIndex = args.LastIncludeIndex
		rf.snapshotLastIncludeTerm = args.LastIncludeTerm
		if args.LastIncludeIndex >= rf.log.LastLogIndex {
			rf.log.Entries = make([]Entry, 0)
			rf.log.LastLogIndex = args.LastIncludeIndex
		} else {
			rf.log.Entries = rf.log.Entries[rf.log.getRealIndex(args.LastIncludeIndex+1):]
		}
		rf.log.FirstLogIndex = args.LastIncludeIndex + 1

		// DPrintf(800, "%v: after install snapshot rf.log.FirstLogIndex=%v, rf.log=%v", rf.SayMeL(), rf.log.FirstLogIndex, rf.log)

		if args.LastIncludeIndex > rf.lastApplied {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotLastIncludeTerm,
				SnapshotIndex: rf.snapshotLastIncludeIndex,
			}
			DPrintf(800, "%v: next apply snapshot rf.snapshot.LastIncludeIndex=%v rf.snapshot.LastIncludeTerm=%v\n", rf.SayMeL(), rf.snapshotLastIncludeIndex, rf.snapshotLastIncludeTerm)
			rf.applyHelper.tryApply(&msg)
			rf.lastApplied = args.LastIncludeIndex
		}
		rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	}
}
func (rf *Raft) InstallSnapshot(serverId int) {

	args := RequestInstallSnapShotArgs{}
	reply := RequestInstallSnapShotReply{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(11, "%v: InstallSnapshot begin serverId=%v myinfo:rf.lastApplied=%v, rf.log.FirstLogIndex=%v\n", rf.SayMeL(), serverId, rf.lastApplied, rf.log.FirstLogIndex)
	defer DPrintf(11, "%v: InstallSnapshot end serverId=%v\n", rf.SayMeL(), serverId)
	if rf.serverState != leaderState {
		return
	}
	// if rf.lastApplied < rf.log.FirstLogIndex {
	// 	return
	// }
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludeIndex = rf.snapshotLastIncludeIndex
	args.LastIncludeTerm = rf.snapshotLastIncludeTerm
	args.Snapshot = rf.snapshot
	rf.mu.Unlock()
	ok := rf.sendRequestInstallSnapshot(serverId, &args, &reply)
	rf.mu.Lock()
	if !ok {
		DPrintf(12, "%v: cannot sendRequestInstallSnapshot to  %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
		return
	}
	if rf.serverState != leaderState {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.SetServerStateL(followerState)
		rf.NewTermL(reply.Term)
		rf.persist()
		return
	}
	rf.nextIndex[serverId] = args.LastIncludeIndex + 1
	rf.matchIndex[serverId] = args.LastIncludeIndex
	rf.tryCommitL(rf.matchIndex[serverId])
}
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// DPrintf(3, "%v: before rf.mu.Lock() get RequestAppendEntries args.LeaderId=%v, args.PrevLogIndex=%v, args.PrevLogTerm=%v, args.Entries=%v myentries=%v\n",
	// rf.SayMeL(), args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.log)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(3, "%v: get RequestAppendEntries args.LeaderId=%v, args.PrevLogIndex=%v, args.PrevLogTerm=%v,\n",
		rf.SayMeL(), args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.electionTimer.Reset(rf.getElectionTime())
	rf.SetServerStateL(followerState)
	if args.Term > rf.currentTerm {
		rf.NewTermL(args.Term)
	}
	defer rf.persist()
	// if len(args.Entries) > 0 {
	// 	DPrintf(3, "%v: get append args args.PrevLogIndex=%v args.PrevLogTerm=%v  len=%v args.LeaderId=%v\n",
	// 		rf.SayMeL(), args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderId)
	// }
	if rf.log.empty() {
		if args.PrevLogIndex == rf.snapshotLastIncludeIndex {
			rf.log.appendL(args.Entries...)
			reply.Term = rf.currentTerm
			reply.Success = true
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		}
	}

	if args.PrevLogIndex+1 < rf.log.FirstLogIndex || args.PrevLogIndex > rf.log.LastLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
	} else if rf.getEntryTerm(args.PrevLogIndex) == args.PrevLogTerm {
		//match
		ok := true
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index > rf.log.LastLogIndex {
				rf.log.appendL(entry)
			} else if rf.log.Entry(index).Term != entry.Term {
				ok = false
				*rf.log.Entry(index) = entry
			}
		}
		if !ok {
			rf.log.LastLogIndex = args.PrevLogIndex + len(args.Entries)
		}

		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.log.LastLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log.LastLogIndex
			}
			rf.applyCond.Broadcast()
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
	} else {
		prevIndex := args.PrevLogIndex
		for prevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(prevIndex) == rf.log.Entry(args.PrevLogIndex).Term {
			prevIndex--
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		if prevIndex >= rf.log.FirstLogIndex {
			reply.PrevLogIndex = prevIndex
			reply.PrevLogTerm = rf.getEntryTerm(prevIndex)
		} else {
			reply.PrevLogIndex = rf.snapshotLastIncludeIndex
			reply.PrevLogTerm = rf.snapshotLastIncludeTerm
		}
	}
	DPrintf(1, "%v: reply to %v reply.Term=%v reply.Success=%v reply.PrevLogIndex=%v reply.PrevLogTerm=%v\n", rf.SayMeL(), args.LeaderId, reply.Term, reply.Success,
		reply.PrevLogIndex, reply.PrevLogTerm)
}
func (rf *Raft) AppendEntries(serverId int, heart bool) {
	reply := RequestAppendEntriesReply{}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != leaderState {
		return
	}
	prevLogIndex := rf.nextIndex[serverId] - 1
	if heart {
		prevLogIndex = rf.log.LastLogIndex
	}
	prevLogIndex = min(prevLogIndex, rf.log.LastLogIndex)

	if prevLogIndex+1 < rf.log.FirstLogIndex { //心跳包肯定不会通过快照发
		go rf.InstallSnapshot(serverId)
		return
	}

	Entries_i := rf.log.getAppendEntries(prevLogIndex + 1)
	if len(Entries_i) > 0 {
		DPrintf(5, "%v: send entries [%v,%v] to server %v prevIndex=%v rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v", rf.SayMeL(), Entries_i[0], Entries_i[len(Entries_i)-1], serverId, prevLogIndex, rf.log.FirstLogIndex, rf.log.LastLogIndex)
	} else {
		DPrintf(5, "%v: send entries [] to server %v prevIndex=%v rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v", rf.SayMeL(), serverId, prevLogIndex, rf.log.FirstLogIndex, rf.log.LastLogIndex)
	}
	args := RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getEntryTerm(prevLogIndex),
		Entries:      Entries_i,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	ok := rf.sendRequestAppendEntries(serverId, &args, &reply)
	rf.mu.Lock()

	if rf.serverState != leaderState {
		return
	}
	if !ok {
		DPrintf(1, "%v: cannot request AppendEntries to %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
		return
	}
	DPrintf(11, "%v: get reply from %v reply.Term=%v reply.Success=%v reply.PrevLogTerm=%v reply.PrevLogIndex=%v myinfo:rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n",
		rf.SayMeL(), serverId, reply.Term, reply.Success, reply.PrevLogTerm, reply.PrevLogIndex, rf.log.FirstLogIndex, rf.log.LastLogIndex)

	if reply.Term > rf.currentTerm {
		rf.SetServerStateL(followerState)
		rf.NewTermL(reply.Term)
		rf.persist()
		return
	}
	DPrintf(3, "%v: get append reply reply.PrevLogIndex=%v reply.PrevLogTerm=%v reply.Success=%v heart=%v\n", rf.SayMeL(), reply.PrevLogIndex, reply.PrevLogTerm, reply.Success, heart)
	if reply.Success {
		// reply.PrevLog index term 不可信 may be bug
		//reply.prevlog index可能进快照了
		// rf.nextIndex[serverId] = reply.PrevLogIndex + 1
		// rf.matchIndex[serverId] = reply.PrevLogIndex
		rf.nextIndex[serverId] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
		rf.tryCommitL(rf.matchIndex[serverId])
		return
	}
	//reply.Success is false
	if rf.log.empty() { //判掉为空的情况 方便后面讨论
		go rf.InstallSnapshot(serverId)
		return
	}

	if reply.PrevLogIndex+1 < rf.log.FirstLogIndex {
		go rf.InstallSnapshot(serverId)
		return
	} else if reply.PrevLogIndex > rf.log.LastLogIndex {
		rf.nextIndex[serverId] = rf.log.LastLogIndex + 1
	} else if rf.getEntryTerm(reply.PrevLogIndex) == reply.PrevLogTerm {
		rf.nextIndex[serverId] = reply.PrevLogIndex + 1
	} else {
		PrevIndex := reply.PrevLogIndex
		for PrevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(PrevIndex) == rf.getEntryTerm(reply.PrevLogIndex) {
			PrevIndex--
		}
		if PrevIndex+1 < rf.log.FirstLogIndex {
			if rf.log.FirstLogIndex > 1 {
				go rf.InstallSnapshot(serverId)
				return
			}
		}
		rf.nextIndex[serverId] = PrevIndex + 1
	}
	DPrintf(3, "%v in AppendEntries change nextindex[%v]=%v and retry\n", rf.SayMeL(), serverId, rf.nextIndex[serverId])
	go rf.AppendEntries(serverId, false)

}
func (rf *Raft) StartAppendEntries(heart bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != leaderState {
		return
	}
	rf.heartbeatTimer.Reset(rf.getHeartbeatTime())
	DPrintf(1, "%v: start AppendEntries\n", rf.SayMeL())

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, heart)
	}
}
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Reset(rf.getElectionTime())
	if rf.serverState == leaderState {
		return
	}
	rf.SetServerStateL(candidateState)
	rf.NewTermL(rf.currentTerm + 1)
	DPrintf(1, "%v: start election\n", rf.SayMeL())
	rf.voteFor = rf.me
	cntVoted := 1
	defer rf.persist()

	// BUG 如果不加下面的特判   只有一台的时候会错吧
	if cntVoted > len(rf.peers)/2 {
		rf.becomeLeaderL()
		go rf.StartAppendEntries(true)
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.LastLogIndex,
		LastLogTerm:  rf.getLastEntryTerm(),
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				DPrintf(1, "%v: cannot request Vote to %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
				return
			}
			if reply.Term > rf.currentTerm {
				rf.NewTermL(reply.Term)
				rf.SetServerStateL(followerState)
				rf.persist()
				return
			}
			if reply.Term < rf.currentTerm {
				return
			}
			// next only reply.term==rf.currentTerm
			if rf.serverState != candidateState {
				return
			}
			DPrintf(12, "%v: get vote reply from %v reply.Term=%v reply.VoteGranted=%v cntVoted=%v ", rf.SayMeL(), serverId, reply.Term, reply.VoteGranted, cntVoted)
			if reply.VoteGranted {
				cntVoted++
				if cntVoted > len(rf.peers)/2 {
					rf.becomeLeaderL()
					go rf.StartAppendEntries(true)
				}
			}
		}(i)
	}
}
func (rf *Raft) SetServerStateL(nextState ServerState) {
	DPrintf(1, "%v: change server state from %v to %v\n", rf.SayMeL(), rf.serverState, nextState)
	rf.serverState = nextState
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.heartbeatTimer.C:
			rf.StartAppendEntries(false)
		case <-rf.electionTimer.C:
			rf.StartElection()
		}

	}
	rf.heartbeatTimer.Stop()
	rf.electionTimer.Stop()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		DPrintf(2, "%s: apply wait\n", rf.SayMeL())
		rf.applyCond.Wait()
		DPrintf(100, "%v: try apply rf.lastApplied=%v rf.commitIndex=%v", rf.SayMeL(), rf.lastApplied, rf.commitIndex)
		for rf.lastApplied+1 <= rf.commitIndex {
			i := rf.lastApplied + 1
			// i = 1
			rf.lastApplied++
			// DPrintf(4, "%s: apply index=%v cmd=%v\n", rf.SayMeL(), i, rf.log.Entries[i].Command)
			if i < rf.log.FirstLogIndex {
				DPrintf(999, "%v: apply index=%v but rf.log.FirstLogIndex=%v rf.lastApplied=%v\n",
					rf.SayMeL(), i, rf.log.FirstLogIndex, rf.lastApplied)
				panic("")
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.Entry(i).Command,
				CommandIndex: i,
			}
			DPrintf(500, "%s: next apply index=%v lastApplied=%v len entries=%v LastLogIndex=%v cmd=%v\n", rf.SayMeL(), i, rf.lastApplied, len(rf.log.Entries), rf.log.LastLogIndex, rf.log.Entry(i).Command)
			rf.applyHelper.tryApply(&msg)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.KVserver 0.*alive
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
	rf.voteFor = -1
	rf.log = NewLog()
	rf.SetServerStateL(followerState)
	rf.electionTimer = time.NewTimer(rf.getElectionTime())
	rf.heartbeatTimer = time.NewTimer(rf.getHeartbeatTime())

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.snapshot = nil
	rf.snapshotLastIncludeIndex = 0
	rf.snapshotLastIncludeTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist()
	rf.applyHelper = NewApplyHelper(applyCh, rf.lastApplied)
	DPrintf(100, "%v: Make\n", rf.SayMeL())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	testTimer := time.NewTimer(time.Second * 5)
	go rf.alive(testTimer)
	go func() {
		<-testTimer.C
		if rf.killed() {
			return
		}
		tmpInfo := fmt.Sprintf("%v: bomb", rf.SayMeL())
		DPrintf(999, "%v", tmpInfo)
		panic(tmpInfo)
	}()
	return rf
}

func (rf *Raft) getElectionTime() time.Duration {
	// [250,400) 250+[0,150]
	// return time.Millisecond * time.Duration(250+15*rf.me)
	return time.Millisecond * time.Duration(350+rand.Intn(200))
}
func (rf *Raft) getHeartbeatTime() time.Duration {
	return time.Millisecond * 110
}
func (rf *Raft) getLastEntryTerm() int {
	if rf.log.LastLogIndex >= rf.log.FirstLogIndex {
		return rf.log.Entry(rf.log.LastLogIndex).Term
	} else {
		return rf.snapshotLastIncludeTerm
	}
}

func (rf *Raft) alive(testTimer *time.Timer) {
	for !rf.killed() {
		rf.mu.Lock()
		testTimer.Reset(time.Second * 2)
		DPrintf(100, "%v: I am alive\n", rf.SayMeL())
		rf.mu.Unlock()
		time.Sleep(time.Second)
	}
	testTimer.Stop()
}
