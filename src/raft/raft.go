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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

func shrinkEntriesArray(log []LogEntry) []LogEntry {
	return append(make([]LogEntry, 0), log...)
}

func StableHeartbeatTimeout() time.Duration {
	return time.Millisecond * 100
}

func RandomizedElectionTimeout() time.Duration {
	ms := 500 + (rand.Int63() % 500)
	return time.Duration(ms) * time.Millisecond
}

type NodeState int

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
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

type LogEntry struct {
	Command interface{}
	Term    int
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

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	currentState   NodeState

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	snapshot       []byte
	lastEntryTerm  int
	lastEntryIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.currentState == LEADER

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil || e.Encode(rf.lastEntryTerm) != nil || e.Encode(rf.lastEntryIndex) != nil {
		panic("failed to encode data fields")
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastEntryTerm int
	var lastEntryIndex int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastEntryTerm) != nil || d.Decode(&lastEntryIndex) != nil {
		panic("failed to decode persist data")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastEntryTerm = lastEntryTerm
	rf.lastEntryIndex = lastEntryIndex
	rf.snapshot = rf.persister.ReadSnapshot()

	rf.updateCommitIndex(lastEntryIndex)
	rf.updateLastApplied(lastEntryIndex)
}

func (rf *Raft) updateCommitIndex(commitIndex int) {
	if rf.commitIndex < commitIndex {
		rf.commitIndex = commitIndex
	}
}

func (rf *Raft) updateLastApplied(lastApplied int) {
	if rf.lastApplied < lastApplied {
		rf.lastApplied = lastApplied
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastEntryIndex || rf.commitIndex < index {
		return
	}

	oldlastEntryIndex := rf.lastEntryIndex
	compactIndex := index - oldlastEntryIndex

	rf.snapshot = snapshot
	rf.lastEntryTerm = rf.log[compactIndex].Term
	rf.lastEntryIndex = index

	if compactIndex < len(rf.log) {
		rf.log = shrinkEntriesArray(rf.log[compactIndex:])
	} else {
		rf.log = []LogEntry{{nil, rf.lastEntryTerm}}
	}

	rf.updateCommitIndex(index)
	rf.updateLastApplied(index)
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
	defer rf.persist()
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor, rf.currentState = args.Term, -1, FOLLOWER
	}

	lastLog := rf.log[len(rf.log)-1]
	lastLogIndex := len(rf.log) - 1 + rf.lastEntryIndex
	if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.electionTimer.Reset(RandomizedElectionTimeout())
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm > args.Term {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.currentState = FOLLOWER
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	//should not be possible
	if rf.lastEntryIndex > args.PrevLogIndex {
		reply.Term, reply.Success = 0, false
		return
	}

	compactPrevLogIndex := args.PrevLogIndex - rf.lastEntryIndex
	lastLogIndex := len(rf.log) - 1 + rf.lastEntryIndex
	if args.PrevLogIndex > lastLogIndex {
		reply.Term, reply.Success, reply.ConflictIndex = rf.currentTerm, false, lastLogIndex+1
		return
	}
	if rf.log[compactPrevLogIndex].Term != args.PrevLogTerm {
		conflictIndex := args.PrevLogIndex
		for conflictIndex-1 > rf.commitIndex && rf.log[conflictIndex-1-rf.lastEntryIndex].Term == rf.log[compactPrevLogIndex].Term {
			conflictIndex--
		}
		reply.Term, reply.Success, reply.ConflictIndex = rf.currentTerm, false, conflictIndex
		return
	}

	if len(args.Entries) > 0 && lastLogIndex >= args.PrevLogIndex+1 {
		rf.log = shrinkEntriesArray(rf.log[:args.PrevLogIndex+1-rf.lastEntryIndex])
	}
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		lastIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		rf.applyCond.Signal()
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term          int
	LeaderId      int
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.currentState = FOLLOWER
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.SnapshotIndex <= rf.commitIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	index := args.SnapshotIndex - rf.lastEntryIndex
	if index >= 0 && index < len(rf.log) && rf.log[index].Term == args.SnapshotTerm {
		rf.log = shrinkEntriesArray(rf.log[index:])
	} else {
		rf.log = make([]LogEntry, 1)
		rf.log[0].Term = args.SnapshotTerm
	}

	rf.snapshot = args.Snapshot
	rf.lastEntryTerm = args.SnapshotTerm
	rf.lastEntryIndex = args.SnapshotIndex

	rf.updateLastApplied(args.SnapshotIndex)
	rf.updateCommitIndex(args.SnapshotIndex)

	reply.Term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.SnapshotTerm,
		SnapshotIndex: args.SnapshotIndex,
	}
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != LEADER {
		isLeader = false
	} else {
		index = len(rf.log) + rf.lastEntryIndex
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
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

func (rf *Raft) heartbeater() {
	for rf.killed() == false {
		<-rf.heartbeatTimer.C
		rf.mu.Lock()
		if rf.currentState != LEADER {
			rf.mu.Unlock()
			return
		}
		go rf.sendHeartbeat()
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.lastEntryIndex, rf.commitIndex, rf.lastApplied
		entries := shrinkEntriesArray(rf.log[lastApplied+1-firstIndex : commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
		}
		rf.mu.Lock()
		rf.updateLastApplied(commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		<-rf.electionTimer.C
		rf.mu.Lock()
		// Check if a leader election should be started.
		if rf.currentState != LEADER {
			go rf.StartElection()
		}
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.mu.Unlock()
		// Your code here (2A)

	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.currentState = CANDIDATE
	rf.persist()

	votes := 1

	lastLog := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1 + rf.lastEntryIndex,
		LastLogTerm:  lastLog.Term,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentState != CANDIDATE || rf.currentTerm != reply.Term {
					return
				}
				if reply.VoteGranted {
					votes += 1
					if votes > len(rf.peers)/2 {
						rf.currentState = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						lastLogIndex := len(rf.log) - 1 + rf.lastEntryIndex
						for j := range rf.nextIndex {
							rf.matchIndex[j], rf.nextIndex[j] = 0, lastLogIndex+1
						}
						rf.matchIndex[rf.me] = lastLogIndex

						go rf.sendHeartbeat()
						go rf.heartbeater()
					}
				} else if rf.currentTerm < reply.Term {
					rf.currentTerm, rf.votedFor, rf.currentState = reply.Term, -1, FOLLOWER
					rf.persist()
				}
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	if rf.currentState != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] > rf.lastEntryIndex {
			go rf.handleAppendEntries(i)
		} else {
			go rf.handleInstallSnapshot(i)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleAppendEntries(peer int) {
	rf.mu.Lock()
	if rf.currentState != LEADER {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer]-1 < rf.lastEntryIndex {
		go rf.handleInstallSnapshot(peer)
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peer]-1-rf.lastEntryIndex].Term,
		Entries:      rf.log[rf.nextIndex[peer]-rf.lastEntryIndex:],
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	if rf.sendAppendEntries(peer, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm {
			return
		}
		if reply.Success {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if rf.matchIndex[peer] < newMatchIndex {
				rf.matchIndex[peer] = newMatchIndex
				rf.nextIndex[peer] = newMatchIndex + 1
			}

			rf.checkMajorityMatch()
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm, rf.votedFor, rf.currentState = reply.Term, -1, FOLLOWER
			rf.persist()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
		} else if reply.Term == rf.currentTerm && rf.currentState == LEADER {
			if reply.ConflictIndex > 0 {
				logLength := len(rf.log) + rf.lastEntryIndex
				if reply.ConflictIndex <= logLength {
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					rf.nextIndex[peer] = logLength
				}
				go rf.sendHeartbeat()
			}
		}
	}
}

func (rf *Raft) handleInstallSnapshot(peer int) {
	rf.mu.Lock()
	if rf.currentState != LEADER {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:          rf.currentTerm,
		LeaderId:      rf.me,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastEntryTerm,
		SnapshotIndex: rf.lastEntryIndex,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	if rf.sendInstallSnapshot(peer, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm, rf.votedFor, rf.currentState = reply.Term, -1, FOLLOWER
			rf.persist()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
		} else {
			if rf.matchIndex[peer] < args.SnapshotIndex {
				rf.matchIndex[peer] = args.SnapshotIndex
			}
			if rf.nextIndex[peer] < args.SnapshotIndex+1 {
				rf.nextIndex[peer] = args.SnapshotIndex + 1
			}
			go rf.sendHeartbeat()
		}
	}
}

func (rf *Raft) checkMajorityMatch() {
	sortedMatchIndex := append([]int(nil), rf.matchIndex...)
	sort.Sort(sort.IntSlice(sortedMatchIndex))
	highestCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	if highestCommitIndex < rf.commitIndex {
		return
	}
	highestCommitTerm := rf.log[highestCommitIndex-rf.lastEntryIndex].Term
	if highestCommitTerm == rf.currentTerm {
		rf.updateCommitIndex(highestCommitIndex)
		rf.applyCond.Signal()
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
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		applyCh:        applyCh,
		currentState:   FOLLOWER,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
