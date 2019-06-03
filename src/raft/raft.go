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
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// log entry struct for Raft peer to persiste.
// contains Command for the state machine && term when entry was received by leader
//
type LogEntry struct {
	Command interface{}
	Term    int
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

	// the commiucation channel
	hearbeat chan int
	timeout  chan int
	voteSig  chan int
	cancel   chan int
	clientCh chan ApplyMsg

	// Persistent state on all servers
	// Updated on stable storage before responding to RPCs
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // -1 means don't vote for anybody
	log         []LogEntry // each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	role        int        // 0 leader, 1 candidate, 2 follower

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	// (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == 0
	rf.mu.Unlock()
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Calling Request Vote %v", rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || uptodate(args, rf) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}
	if !reply.VoteGranted {
		return
	}
	DPrintf("%v Granted true", rf.me)
	rf.votedFor = args.CandidateId
	rf.voteSig <- 1
}

func uptodate(args *RequestVoteArgs, rf *Raft) bool {
	logIndex := len(rf.log) - 1
	logTerm := rf.log[logIndex].Term

	if args.LastLogTerm > logTerm || (args.LastLogTerm == logTerm && args.LastLogIndex >= logIndex) {
		return true
	}

	return false
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

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("append entries args : %v", args)
	DPrintf("raft %v status :\n log: %v\n term: %v", rf.me, rf.log, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		//reply.ConflictTerm = args.PrevLogTerm
		reply.ConflictIndex = args.PrevLogIndex
		if reply.ConflictIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log) - 1
		}
		reply.ConflictTerm = rf.log[reply.ConflictIndex].Term
		for reply.ConflictIndex > 0 && rf.log[reply.ConflictIndex].Term == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		reply.ConflictIndex++
	} else {
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.log = rf.log[:args.PrevLogIndex+1]
		if len(args.Entries) > 0 {
			rf.log = append(rf.log, args.Entries[:]...)
		}
		if args.LeaderCommit > rf.commitIndex {
			//DPrintf("args leader commit index : %v", args.LeaderCommit)
			newCommitIndex := Min(args.LeaderCommit, len(rf.log)-1)
			for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				rf.clientCh <- ApplyMsg{true, rf.log[i].Command, i}
				DPrintf("%v sending committed %v", rf.me, ApplyMsg{true, rf.log[i].Command, i})
			}
			rf.commitIndex = newCommitIndex
		}
		DPrintf("%v's commit index is : %v", rf.me, rf.commitIndex)
		DPrintf("%v's log is current: %v", rf.me, rf.log)
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}
	if args.Term < rf.currentTerm {
		return
	}
	if rf.votedFor != args.LeaderId {
		rf.votedFor = args.LeaderId
	}
	rf.hearbeat <- 1
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.role == 0
	if isLeader {
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		rf.matchIndex[rf.me] = index
		DPrintf("%v accept client command: %v", rf.me, rf.log[len(rf.log)-1])
	}
	rf.mu.Unlock()
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
	rf.cancel <- 1
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}

	rf.role = 2
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.timeout = make(chan int, 1)
	rf.hearbeat = make(chan int, 1)
	rf.voteSig = make(chan int, 1)
	rf.cancel = make(chan int, 1)
	rf.clientCh = applyCh

	// the function to wait for leader election
	go followerTerm(rf)

	return rf
}

func followerTerm(rf *Raft) {
	//rand.Seed(time.Now().Unix())
	t := rand.Uint32()%5e8 + 3e8
	for {
		timeout := make(chan int, 1)
		go ticks(timeout, t)
		select {
		case <-rf.hearbeat:
			// keep follower
			break
		case <-rf.voteSig:
			// keep follower
			break
		case <-rf.cancel:
			return
		case <-timeout:
			// become candidate
			// start candidateTerm
			DPrintf("%v start candidateTerm", rf.me)
			go candidateTerm(rf)
			return
		}
	}
}

func ticks(timeout chan int, t uint32) {
	time.Sleep(time.Duration(t))
	timeout <- 1
}

func candidateTerm(rf *Raft) {
	DPrintf("%v enter candidateTerm", rf.me)
	//rand.Seed(time.Now().Unix())
	rf.role = 1
	t := rand.Uint32()%5e8 + 3e8
	tch := make(chan int, 1)
	for {
		DPrintf("%v candidate loop", rf.me)
		timeout := make(chan int, 1)
		go ticks(timeout, t)
		go election(rf, tch)
		select {
		case <-rf.hearbeat:
			DPrintf("%v go to follower term", rf.me)
			rf.role = 2
			go followerTerm(rf)
			return
		case <-rf.voteSig:
			DPrintf("%v from candidate become follower", rf.me)
			rf.role = 2
			go followerTerm(rf)
			return
		case <-rf.cancel:
			return
		case <-tch:
			// go to leader term
			DPrintf("%v go to leader Term", rf.me)
			rf.role = 0
			go leaderTerm(rf)
			return
		case <-timeout:
			DPrintf("%v timeout candidateTerm", rf.me)
			t = rand.Uint32()%5e8 + 3e8
			break
		}
	}
}

func election(rf *Raft, ch chan int) {
	rf.mu.Lock()
	DPrintf("%v start election term %v", rf.me, rf.currentTerm+1)
	lastLogIndex := len(rf.log) - 1

	// change the rf state
	rf.currentTerm += 1
	rf.votedFor = rf.me
	//rf.mu.Unlock()
	// make the request vote args
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, rf.log[lastLogIndex].Term}
	rf.mu.Unlock()
	// set the voteCount = 1 (voteFor self first)
	voteCount := 1

	old := false

	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(id int, rf *Raft) {
			reply := RequestVoteReply{0, false}
			if ok := rf.sendRequestVote(id, &args, &reply); ok {
				rf.mu.Lock()
				if reply.VoteGranted {
					voteCount++
					if voteCount == len(rf.peers)/2+1 {
						ch <- 1
					}
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					old = true
				}
				rf.mu.Unlock()
			}
			wg.Done()
		}(i, rf)
	}
	wg.Wait()
	if old {
		rf.hearbeat <- 1
		DPrintf("%v is old", rf.me)
		return
	}
	DPrintf("%v gets %v votes", rf.me, voteCount)
	DPrintf("%v end election", rf.me)

}

func leaderTerm(rf *Raft) {
	tick := make(chan int, 1)
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	go sendHeartBeat(rf)
	for {
		DPrintf("%v leadering Term %v", rf.me, rf.currentTerm)
		go ticks(tick, 1e8)
		select {
		case <-rf.hearbeat:
			rf.role = 2
			DPrintf("%v from leader become follower", rf.me)
			go followerTerm(rf)
			return
		case <-rf.voteSig:
			rf.role = 2
			DPrintf("%v from leader become follower", rf.me)
			go followerTerm(rf)
			return
		case <-rf.cancel:
			return
		case <-tick:
			go sendHeartBeat(rf)
		}
	}
}

func confCommit(rf *Raft) {
	rf.mu.Lock()
	mchIndex := make([]int, len(rf.matchIndex))
	DPrintf("match index is : %v", rf.matchIndex)
	copy(mchIndex, rf.matchIndex)
	sort.Ints(mchIndex)
	mid := len(mchIndex) - (len(mchIndex)/2 + 1)
	for i := mid; i >= 0; i-- {
		DPrintf("Commit Index : %v", mchIndex[i])
		if mchIndex[i] <= rf.commitIndex {
			break
		}
		if rf.log[mchIndex[i]].Term == rf.currentTerm {
			for j := rf.commitIndex + 1; j <= mchIndex[i]; j++ {
				rf.clientCh <- ApplyMsg{true, rf.log[j].Command, j}
				DPrintf("%v sending committed %v", rf.me, ApplyMsg{true, rf.log[j].Command, j})
			}
			rf.commitIndex = mchIndex[i]
			DPrintf("update commit index %v", rf.commitIndex)
			break
		}
	}
	rf.mu.Unlock()

}

func sendHeartBeat(rf *Raft) {
	DPrintf("%v's current log is : %v", rf.me, rf.log)
	// pre store instead of get current
	// to make the every heart beat sent to be the same
	rf.mu.Lock()
	rfLog := rf.log
	rfCommitIndex := rf.commitIndex
	rfTerm := rf.currentTerm
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int, irf *Raft) {
			reply := RequestAppendEntriesReply{}
			entries := rfLog[irf.nextIndex[id]:]
			args := RequestAppendEntriesArgs{rfTerm, irf.me, irf.nextIndex[id] - 1,
				rfLog[irf.nextIndex[id]-1].Term, entries, rfCommitIndex}
			if ok := irf.sendRequestAppendEntries(id, &args, &reply); ok {
				DPrintf("%v send heart beat success to %v", irf.me, id)
				irf.mu.Lock()
				if irf.role != 0 {
					irf.mu.Unlock()
					return
				}
				if reply.Term > irf.currentTerm {
					irf.currentTerm = reply.Term
					irf.hearbeat <- 1
				}
				if reply.Success {
					irf.nextIndex[id] = len(irf.log)
					irf.matchIndex[id] = irf.nextIndex[id] - 1
					// check whether have some log could commit
					go confCommit(irf)
				} else {
					irf.nextIndex[id] = reply.ConflictIndex
				}
				irf.mu.Unlock()
			}
		}(i, rf)
	}
}
