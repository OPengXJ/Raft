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

	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm     int
	VoteFor         string //candidateId that received vote in current term
	Logs            []LogEntry
	HasGotHeartBeat bool
	State           int //1.follower,2.candidate,3.leader
	HeartBearCond   *sync.Cond
}

// define the log entry
type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.State == 3 {
		isleader = true
	}
	rf.mu.Unlock()
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //candidate’s term
	CandidateId int //candidate requesting vote
	//make sure choose a right leader
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	// in this term have vote to another
	if args.Term == rf.CurrentTerm && rf.VoteFor != "" {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	//the args' Term maybe newer or same to the currentTerm
	//疑惑：会不会存在可能CurrentTerm变了，但是投票失败(日志不符合)的可能，答案：会，主要还是看日志队列是否相同
	rf.CurrentTerm = args.Term
	reply.Term = rf.CurrentTerm
	// in the 5.3 and 5.4  check logs whether right or not
	if len(rf.Logs) == 0 {
		rf.VoteFor = strconv.Itoa(args.CandidateId)
		reply.VoteGranted = true
	} else if args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term {
		rf.VoteFor = strconv.Itoa(args.CandidateId)
		reply.VoteGranted = true
	} else if args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term {
		if args.LastLogIndex >= len(rf.Logs)-1 {
			rf.VoteFor = strconv.Itoa(args.CandidateId)
			reply.VoteGranted = true
		}
	} else {
		reply.VoteGranted = false
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
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.State != 3 && !rf.HasGotHeartBeat {
			rf.CurrentTerm++
			rf.State = 2
			rf.VoteFor = strconv.Itoa(rf.me)
			HasGotVoteNum := 1
			lastlogIndex := 0
			lastlogTerm := 0
			if len(rf.Logs) == 0 {
				lastlogIndex = -1
				lastlogTerm = -1
			} else {
				lastlogIndex = len(rf.Logs) - 1
				lastlogTerm = rf.Logs[len(rf.Logs)-1].Term
			}
			requestVoteArg := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastlogIndex,
				LastLogTerm:  lastlogTerm,
			}
			rf.mu.Unlock()
			//log.Printf("request vote started, machine :%d\n", rf.me)
			var wg sync.WaitGroup
			wg.Add(len(rf.peers) - 1)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int) {
					defer wg.Done()
					rf.mu.Lock()
					// if has from candidate change to follower
					if rf.State != 2 {
						// rf.HasGotHeartBeat = false
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					requestVoteReply := &RequestVoteReply{}
					if ok := rf.sendRequestVote(i, &requestVoteArg, requestVoteReply); ok {
						rf.mu.Lock()
						if requestVoteReply.VoteGranted {
							HasGotVoteNum++
							//log.Printf("machine %d get vote respond from %d in term: %d has %d votenum \n", rf.me, i, rf.CurrentTerm, HasGotVoteNum)
							if HasGotVoteNum > len(rf.peers)/2 && rf.State == 2 {
								//log.Printf("machine %d has been leader\n", rf.me)
								rf.State = 3

								//let the Heartbeater run
								rf.HeartBearCond.Broadcast()
								rf.mu.Unlock()
								return
							}
						}
						if requestVoteReply.Term >= rf.CurrentTerm {
							rf.CurrentTerm = requestVoteArg.Term
						}
						rf.mu.Unlock()
					}
				}(i)
			}
			wg.Wait()
		} else if rf.State != 3 && rf.HasGotHeartBeat {
			rf.HasGotHeartBeat = false
			rf.State = 1
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			// fmt.Println("other case", rf.State)
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) HeartBeater() {
	rf.mu.Lock()
	cond := sync.NewCond(&rf.mu)
	rf.HeartBearCond = cond
	rf.mu.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State != 3 {
			cond.Wait()
		}
		//if the server is leader
		prelogindex := 0
		prelogterm := 0
		if len(rf.Logs) == 0 {
			prelogindex = -1
			prelogterm = -1
		} else {
			prelogindex = len(rf.peers) - 1
			prelogterm = rf.Logs[len(rf.Logs)-1].Term
		}
		appendEntriesArgs := AppendEntriesArgs{
			Term:        rf.CurrentTerm,
			LeaderId:    rf.me,
			PreLogIndex: prelogindex,
			PreLogTerm:  prelogterm,
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				reply := &AppendEntriesReply{}
				if ok := rf.sendAppendEntries(i, &appendEntriesArgs, reply); ok {
					//log.Printf("machine %d get heartbear respond from %d in term: %d \n", i, rf.me, rf.CurrentTerm)
				}

			}(i)
		}
		//lab say send heartbeat RPCs no more than ten times per second.
		time.Sleep(time.Duration(100) * time.Millisecond)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.HeartBeater()
	go rf.ticker()

	return rf
}

type Entry struct {
}
type AppendEntriesArgs struct {
	Term         int //leader’s term
	LeaderId     int //so follower can redirect clients
	PreLogIndex  int //index of log entry immediately preceding new ones
	PreLogTerm   int //term of prevLogIndex entry
	Entries      []Entry
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	// this is a HeartBeat
	if args.Entries == nil {
		// main for the candidate,let it change back to follower
		rf.State = 1
		rf.CurrentTerm = args.Term
		rf.HasGotHeartBeat = true
		reply.Term = rf.CurrentTerm
		if len(rf.Logs) != 0 {
			if args.PreLogIndex == len(rf.Logs)-1 && args.PreLogTerm == rf.Logs[len(rf.Logs)-1].Term {
				reply.Success = true
			}
		} else {
			if args.PreLogIndex == -1 {
				reply.Success = true
			}
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
