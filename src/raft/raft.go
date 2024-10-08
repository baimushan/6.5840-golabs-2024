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

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/util"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
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
	// 3A
	state       int
	currentTerm int
	votedFor    int
	//logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	votesReceived  int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	logger *util.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader
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
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.logger.Debug("%d receive RequestVote state %d term %d  (rf.currentTerm  %d > args.Term %d ) args %v reply %v",
			rf.me, rf.state, rf.commitIndex, rf.commitIndex, args.Term, args, reply)
		return
	}

	if rf.currentTerm == args.Term {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.resetElectionTimer()
			rf.logger.Debug("%d receive RequestVote state %d term %d  support vote  args %v reply %v",
				rf.me, rf.state, rf.commitIndex, args, reply)
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.logger.Debug("%d receive RequestVote state %d term %d dent vote because has vote %d args %v reply %v",
				rf.me, rf.state, rf.commitIndex, rf.votedFor, args, reply)
		}
		return
	}

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.logger.Debug("%d receive RequestVote state %d term %d  (rf.currentTerm  %d < args.Term %d ) args %v reply %v",
			rf.me, rf.state, rf.commitIndex, rf.commitIndex, args.Term, args, reply)

	}

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.becomeFollower(args.Term)
	rf.votedFor = args.LeaderId

	reply.Term = rf.currentTerm
	reply.Success = true

}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) resetElectionTimer() {
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(rf.electionTimeout())
	} else {
		rf.electionTimer.Reset(rf.electionTimeout())
	}
}

func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(500+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) resetHeartbeatTimer() {
	const HeartbeatInterval = 100 * time.Millisecond
	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	} else {
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Debug("%d state %d term %d sendRequestVote args %v reply %v", rf.me, rf.state, rf.commitIndex, args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		}
		if reply.VoteGranted {
			rf.votesReceived++
		}
		if rf.votesReceived > len(rf.peers)/2 {
			rf.startLeader()
			//rf.electionTimer.Stop() //需要这个吗
		}
	}
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

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.logger.Debug("%d start elect term %d ", rf.me, rf.currentTerm)

	// ... start RequestVote RPCs ...
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendRequestVote(peer, &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}, &RequestVoteReply{})
	}
}

func (rf *Raft) ticker() {
	const SleepInterval = 10 * time.Millisecond
	for !rf.killed() {

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.startElection()
			}
			rf.resetElectionTimer()
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.sendHeartbeats()
				rf.resetHeartbeatTimer()
			}
			rf.mu.Unlock()
		case <-time.After(SleepInterval):
			time.Sleep(SleepInterval)
		}

	}
}

func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendAppendEntries(peer)
	}
}

func (rf *Raft) sendAppendEntries(peer int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.sendAppendEntriesRPC(peer, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.logger.Debug("%d sendAppendEntriesRPC peer %d term %d > my term %d ", rf.me, peer, reply.Term, rf.currentTerm)
			rf.becomeFollower(reply.Term)
		} else {
			rf.logger.Debug("%d sendAppendEntriesRPC peer %d term %d <= my term %d ", rf.me, peer, reply.Term, rf.currentTerm)
		}
	} else {
		rf.logger.Debug("%d sendAppendEntriesRPC fail term %d ", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.resetElectionTimer()
	// Cancel any ongoing leader operations if necessary
	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}
	rf.logger.Debug("%d become follower term %d state %d ", rf.me, rf.currentTerm, rf.state)
}

func (rf *Raft) startLeader() {
	rf.state = Leader
	rf.resetHeartbeatTimer()
	rf.sendHeartbeats()
	rf.logger.Debug("%d change leader term %d state %d ", rf.me, rf.currentTerm, rf.state)

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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTimer()
	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)
	//rf.heartbeatTimer.Stop()
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	//rf.logger = util.NewLogger(util.DEBUG)
	rf.logger = util.NewLogger(util.INFO)

	rf.logger.Debug("raft %v\n", rf)
	// start ticker goroutine to start elections

	go rf.ticker()

	return rf
}
