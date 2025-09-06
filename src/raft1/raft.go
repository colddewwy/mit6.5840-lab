package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type logEntries struct {
	Command interface{}
	Term    int
}

const (
	Follower  = "Follower"
	Leader    = "Leader"
	Candidate = "Candidate"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *tester.Persister   // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	currentTerm     int
	votedFor        int
	log             []logEntries
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	status          string
	electionTimeout time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

func (rf *Raft) resetElectionTimeout() {
	// 设置一个 350ms 到 500ms 之间的随机超时时间
	timeout := time.Duration(350+rand.Intn(150)) * time.Millisecond
	rf.electionTimeout = time.Now().Add(timeout)
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
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	CandidateTerm int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.CandidateTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.CandidateTerm {
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
		rf.status = Follower
	}
	reply.CurrentTerm = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if args.LastLogIndex >= rf.commitIndex {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.resetElectionTimeout()
		} else {
			reply.VoteGranted = false
		}
	}
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.resetElectionTimeout()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}
	reply.Success = true
	reply.Term = rf.currentTerm
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

func (rf *Raft) sendHeartbeats(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) becomeCandidateAndStartElection() {
	rf.status = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	var voteReceived int32 = 1
	args := RequestVoteArgs{CandidateTerm: rf.currentTerm, CandidateID: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.status != Candidate || rf.currentTerm != args.CandidateTerm {
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&voteReceived, 1)
				if atomic.LoadInt32(&voteReceived) > int32(len(rf.peers)/2) {
					rf.status = Leader
				}
			} else if reply.CurrentTerm > rf.currentTerm {
				rf.currentTerm = reply.CurrentTerm
				rf.status = Follower
				rf.votedFor = -1
			}
		}(i)
	}
}

func (rf *Raft) broadcastHeartbeats() {
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			rf.sendHeartbeats(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.status = Follower
			}
		}(i)
	}
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Check if a leader election should be started.
		rf.mu.Lock()
		status := rf.status

		switch status {
		case Follower, Candidate:
			if time.Now().After(rf.electionTimeout) {
				rf.becomeCandidateAndStartElection()
			}
			time.Sleep(20 * time.Millisecond)
		case Leader:
			rf.broadcastHeartbeats()
			time.Sleep(100 * time.Millisecond)
		}
		rf.mu.Unlock()
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntries, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.status = Follower
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
