package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"

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

const (
	STATE_FOLLOWER  = iota
	STATE_CANDIDATE
	STATE_LEADER
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	LogIndex   int
	LogCommand interface{}
	LogTerm    int
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state       int // follower、candidate、leader
	currentTerm int
	votedFor    int        // the server's id voted for
	voteCounter int        // count the votes
	logs        []LogEntry // log entries
	commitIndex int        // index of highest entry known to be committed
	lastApplied int        // index of highest entry applied to state machine

	// states that leader maintains
	nextIndex  []int // leader keeps this to record the index of next entry each follower needs
	matchIndex []int // index of highest entry known to be replicated on server for each server

	chanHeartbeat chan bool // keep track of each server's healthy
	chanGrantVote chan bool // record votes during an election
	chanLeader    chan bool // if any server elected to be a leader
	chanCommit    chan bool // if any server commit the entry
	chanApply     chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type AppendEntriesArgs struct {
	Term              int // leader's term
	LeaderId          int // to redirect clients
	PrevLogIndex      int // the index of entry preceding new ones
	PrevLogTerm       int // the term of PrevLogIndex
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term      int  // currentTerm, for leader to update itself
	NextIndex int  // next index the server needs
	Success   bool // true as follower contained entry matching PrevLogIndex and PrevLogTerm
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool // true means vote
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == STATE_LEADER
}

// return the index of latest entry
func (rf *Raft) getLastIndex() int {
	return rf.logs[len(rf.logs)-1].LogIndex
}

// return the term of latest entry
func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].LogTerm
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm { // vote for no
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm { // vote for yes
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	latest := false
	if args.LastLogTerm > term {
		latest = true
	}
	if args.LastLogTerm == term && args.LastLogIndex >= index {
		latest = true
	}
	if (rf.votedFor == -1 || rf.votedFor==args.CandidateId) && latest {
		rf.chanGrantVote <- true
		rf.state = STATE_FOLLOWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}

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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Term > rf.currentTerm { // stale server -> follower
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
		}
		if reply.VoteGranted { // get vote from reply-server
			rf.voteCounter++
			if rf.state == STATE_CANDIDATE && rf.voteCounter > len(rf.peers)/2 {
				rf.chanLeader <- true
			}
		}
	}

	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false

	if args.Term < rf.currentTerm { // stale leader
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	rf.chanHeartbeat <- true
	if args.Term > rf.currentTerm { // healthy follower
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	if args.PrevLogIndex > rf.getLastIndex() { // need for older entry
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	baseIndex := rf.logs[0].LogIndex // may not start from 0
	if args.PrevLogIndex > baseIndex {
		term := rf.logs[args.PrevLogIndex-baseIndex].LogTerm
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1 ; i >= baseIndex; i-- {
				if rf.logs[i-baseIndex].LogTerm != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}
	if args.PrevLogIndex>=baseIndex{
		rf.logs = rf.logs[:args.PrevLogIndex+1-baseIndex]
		rf.logs = append(rf.logs, args.Entries...) // append new entries
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		if args.LeaderCommitIndex > rf.getLastIndex() {
			rf.commitIndex = rf.getLastIndex()
		} else {
			rf.commitIndex = args.LeaderCommitIndex
		}
		//rf.commitIndex = math.MinInt64(args.LeaderCommitIndex, rf.getLastIndex())
		rf.chanCommit <- true
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != STATE_LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm { // stale term
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				// update the nextIndex and matchIndex of "server"
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	if rf.state == STATE_LEADER { // add the command to leader's logs
		index = rf.getLastIndex() - 1
		rf.logs = append(rf.logs, LogEntry{LogTerm: term, LogCommand: command, LogIndex: index})
		rf.persist()
	}

	return index, term, rf.state == STATE_LEADER
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

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.logs[0].LogIndex
	last := rf.getLastIndex()
	newCommitIndex := rf.commitIndex

	// find next entry to be committed
	for i := rf.commitIndex + 1; i <= last; i++ {
		committed := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= i && rf.logs[i-baseIndex].LogTerm == rf.currentTerm {
				committed++
			}
		}
		if committed*2 > len(rf.peers) { // most server has stored the entry -- committed
			newCommitIndex = i
		}
	}

	if newCommitIndex != rf.commitIndex { // to be committed
		rf.commitIndex = last
		rf.chanCommit <- true
	}

	for peer := range rf.peers {
		if peer != rf.me && rf.state == STATE_LEADER {
			if rf.nextIndex[peer] > baseIndex { // new entries needed from leader to followers
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex-baseIndex].LogTerm
				args.Entries = make([]LogEntry, len(rf.logs[args.PrevLogIndex-baseIndex+1:]))
				copy(args.Entries, rf.logs[args.PrevLogIndex-baseIndex+1:])
				args.LeaderCommitIndex = rf.commitIndex
				go func(peer int) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(peer, args, &reply)
				}(peer)
			}
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	var voteArgs RequestVoteArgs
	rf.mu.Lock()
	voteArgs.CandidateId = rf.me
	voteArgs.Term = rf.currentTerm
	voteArgs.LastLogIndex = rf.getLastIndex()
	voteArgs.LastLogTerm = rf.getLastTerm()
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer != rf.me && rf.state == STATE_CANDIDATE {
			go func(peer int) {
				var reply RequestVoteReply
				rf.sendRequestVote(peer, voteArgs, &reply)
			}(peer)
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{LogTerm: 0})
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <-rf.chanHeartbeat: // no timeout, continue
				case <-rf.chanGrantVote: // vote for some candidate, continue
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					rf.state = STATE_CANDIDATE
				}
			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm ++
				rf.votedFor = rf.me
				rf.voteCounter = 1
				rf.mu.Unlock()
				go rf.broadcastRequestVote()
				select {
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.state = STATE_FOLLOWER
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			case STATE_LEADER:
				rf.broadcastAppendEntries()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommit: // entry to commit
				rf.mu.Lock()
				baseIndex := rf.logs[0].LogIndex
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{CommandIndex: i, Command: rf.logs[i-baseIndex].LogCommand}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
