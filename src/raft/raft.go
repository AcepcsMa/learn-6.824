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
	"log"
	"sync"
	"time"
)
import "../labrpc"

// import "bytes"
// import "labgob"

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

// log entry entity <term, cmd>
type LogEntry struct {
	TermId int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state 	  int					  // Candidate, Follower or Leader
	voteFor   int					  // which peer it votes for
	voteCount int				  // how many votes it receives in an election

	electionTimer 	time.Timer	  // recording the election timeout
	electionTimeout time.Duration	// election timeout
	resetElection 	chan struct{}	  // a channel responsible for resetting the election timer
	shutdown 		chan struct{}		  // a channel receiving the shutdown signal

	CurrentTerm int
	LogEntries  []LogEntry
	MatchIndex  []int
	NextIndex   []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TermId 			int	// candidate's term id
	CandidateId 	int	// candidate id
	LastLogIndex 	int	// index of the candidate's last log
	LastLogTerm 	int	// term of the candidate's last log
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TermId int
	VoteFor int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	TermId int
	LeaderId int
	Logs []LogEntry
	PrevLogIndex int
	PrevLogTerm int
	CommitIndex int
}

type AppendEntriesReply struct {
	TermId int
	Success bool
	ConflictTerm int
	ConflictIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) GetLastLogInfo() (int, int) {
	return 0, 0
}

// turn itself to be a follower
func (rf *Raft) TurnToFollowerState(latestTerm int) {
	rf.CurrentTerm = latestTerm
	rf.state = FOLLOWER
	rf.persist()
	rf.resetElection <- struct{}{}
}

// turn itself to be a leader
func (rf *Raft) TurnToLeaderState() {
	rf.state = LEADER
	rf.voteCount = 0

	// initialize matchIndex & nextIndex
	peerCount := len(rf.peers)
	logCount := len(rf.LogEntries)
	for i := 0;i < peerCount;i++ {
		if i == rf.me {
			rf.MatchIndex[i] = logCount - 1
		} else {
			rf.MatchIndex[i] = 0
		}
		rf.NextIndex[i] = logCount
	}

	go rf.StartHeartBeat()
	rf.persist()
	rf.resetElection <- struct{}{}

}

// send heartbeat to the peer whose id is peerId
func (rf *Raft) SendHeartBeat(peerId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO: fill in args
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(peerId, &args, &reply)

	// TODO: deal with the reply
}

// start heartbeat mechanism, signaling each peer periodically
func (rf *Raft) StartHeartBeat() {
	for {
		if rf.state != LEADER {
			return
		}

		select {
		case <-rf.shutdown:
			return
		default:
			rf.resetElection <- struct{}{}	// send heartbeat to itself
			peerCount := len(rf.peers)
			for i := 0;i < peerCount;i++ {
				if i != rf.me {
					go rf.SendHeartBeat(i)	// send heartbeat to other peers
				}
			}
		}
	}
}

func (rf *Raft) DealWithRequestVote(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == CANDIDATE {
		if reply.TermId > rf.CurrentTerm {
			rf.TurnToFollowerState(reply.TermId)
		} else if reply.VoteFor == rf.me {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				rf.TurnToLeaderState()
			}
		}
	}
}

func (rf *Raft) BringUpElection() {
	if rf.state != FOLLOWER {
		return
	}
	rf.mu.Lock()
	rf.CurrentTerm += 1
	rf.state = CANDIDATE
	rf.voteFor = rf.me
	lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
	arg := RequestVoteArgs{
		TermId: rf.CurrentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	rf.mu.Unlock()

	rf.voteCount = 1	// vote for itself
	for i := 0;i < len(rf.peers);i++ {
		if i != rf.me {
			reply := RequestVoteReply{}
			go func(peerId int) {
				rf.sendRequestVote(peerId, &arg, &reply)
				rf.DealWithRequestVote(&reply)
			}(i)
		}
	}
}

func (rf *Raft) ElectionDaemon() {
	for {
		select {
		case <-rf.shutdown:
			log.Println("Shutting down, election daemon ends immediately.")
			return
		case <-rf.resetElection:
			if !rf.electionTimer.Stop() {
				<- rf.electionTimer.C
			}
			rf.electionTimer.Reset(rf.electionTimeout)
		case <-rf.electionTimer.C:
			go rf.BringUpElection()
			rf.electionTimer.Reset(rf.electionTimeout)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
