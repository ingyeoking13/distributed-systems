package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             RaftState //3A
	currentTerm       int       // 3A
	votedFor          *int
	log               []int // 3B
	lastHeartBeatTime time.Time
}

type RaftState int

const (
	FOLLOWER  RaftState = 0
	CANDIDATE           = 1
	LEADER              = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := rf.state == LEADER
	term := rf.currentTerm
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
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int      // leader’s term
	LeaderId     int      // so follower can redirect clients
	PrevLogIndex int      // index of log entry immediately preceding new ones
	PrevLogTerm  int      // term of prevLogIndex entry
	Entries      []string // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int      //leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != nil && args.CandidateId != *rf.votedFor {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	reply.VoteGranted = true
	rf.votedFor = &args.CandidateId
	reply.Term = rf.currentTerm
	rf.state = FOLLOWER

	s := fmt.Sprintf("me %d vote for %d in Term %d", rf.me, *rf.votedFor, rf.currentTerm)
	println(s)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votechan chan int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		curState := rf.updateTermOnlyIf(reply.Term)
		if curState == FOLLOWER {
			votechan <- 0
			return ok
		}
		if reply.VoteGranted {
			votechan <- 1
		}
	}

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// term mismatch
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.lastHeartBeatTime = time.Now()
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, arg *AppendEntriesArgs, reply *AppendEntriesReply, replyChan chan AppendEntriesReply) bool {
	rf.mu.Lock()
	println(fmt.Sprintf("me %d term %d send heart beat at "+time.Now().String(), rf.me, arg.Term))
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", &arg, &reply)
	if ok {
		replyChan <- *reply
	}

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

func (rf *Raft) goCandidate(term int) {
	rf.mu.Lock()
	s := fmt.Sprintf("**me %d think vote in term %d", rf.me, term)
	println(s)

	voteCount := 0
	votechan := make(chan int)

	for idx := range rf.peers {
		if idx == rf.me {
			voteCount++
			continue
		}
		arg := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
		reply := RequestVoteReply{}

		go rf.sendRequestVote(idx, &arg, &reply, votechan)
	}
	rf.mu.Unlock()

L:
	for {
		select {
		case vote := <-votechan:
			if vote == 0 {
				break
			}
			voteCount += vote
		case <-time.After(200 * time.Millisecond):
			break L
		}

		if voteCount > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.state = LEADER
			println("became a leader im " + strconv.Itoa(rf.me) + " in term  " + strconv.Itoa(term))
			rf.mu.Unlock()
			break
		}
	}

}

func (rf *Raft) ticker() {
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if b, term := rf.needVote(); b {
			rf.state = CANDIDATE
			rf.currentTerm++
			rf.votedFor = &rf.me
			rf.mu.Unlock()
			go rf.goCandidate(term + 1)
		} else {
			println("no need vote " + strconv.Itoa(rf.me) + " in " + strconv.Itoa(term) + "state " + strconv.Itoa(int(rf.state)))
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) needVote() (bool, int) {
	if rf.state == LEADER {
		return false, rf.currentTerm
	}

	if rf.lastHeartBeatTime.IsZero() {
		return true, rf.currentTerm
	}
	result := time.Since(rf.lastHeartBeatTime)
	println(result.Milliseconds())
	return result.Milliseconds() >= 300, rf.currentTerm
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
	rf.state = FOLLOWER
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// yo. send heartbeat (master only)
	go rf.heartBeat()

	return rf
}

func (rf *Raft) heartBeat() {
	for {
		term, isLeader := rf.GetState()

		if isLeader {
			rf.mu.Lock()
			replyChan := make(chan AppendEntriesReply)
			for idx := range rf.peers {
				if idx == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []string{},
					LeaderCommit: 0,
				}
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(idx, &args, &reply, replyChan)
			}
			rf.mu.Unlock()

		L:
			for {
				select {
				case reply := <-replyChan:
					if !reply.Success {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.mu.Unlock()
						break L
					}
				case <-time.After(20 * time.Millisecond):
					break L
				}
			}
		}
		ms := 10 + (rand.Int63() % 30)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) updateTermOnlyIf(term int) RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.state = FOLLOWER
	}
	return rf.state
}
