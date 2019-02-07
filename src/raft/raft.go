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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "math"
import "bytes"
import "encoding/gob"

const (
	ms        = time.Millisecond
	FOLLOWER  = "Follower"
	CANDIDATE = "Candidate"
	LEADER    = "Leader"
)

//
// A Go object implementing a log entry
//
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
	Term        int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object holding peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	commitIndex   int
	lastApplied   int
	log           []LogEntry
	state         string
	nextIndex     []int
	matchIndex    []int
	heartbeat     chan int
	wonElection   chan int
	votesReceived int
	applyCh       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.log)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.currentTerm)
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
	VoteGranted bool
	Term        int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false

	// Refuse vote and return immediately if term less than current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// If a term is observed that is greater than current term,
	// revert to follower state
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm

	// Election safety conditions
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && args.LastLogIndex >= len(rf.log)-1) {
			rf.state = FOLLOWER
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			go func() { rf.heartbeat <- 1 }()
			return
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != LEADER {
		return -1, -1, false
	}

	newEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   len(rf.log),
	}
	rf.log = append(rf.log, newEntry)

	DPrintf("[server %v] Added the command %+v", rf.me, newEntry.Command)

	return len(rf.log) - 1, rf.currentTerm, rf.state == LEADER
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	IsHeartbeat  bool
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	FirstBadIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,
	reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FirstBadIndex = len(rf.log)
		return
	}

	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.heartbeat <- 1

	reply.Term = rf.currentTerm

	reply.Success = true
	reply.FirstBadIndex = len(rf.log)

	// log too short
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Success = false
		reply.FirstBadIndex = len(rf.log)
		return
		// entry does not match
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm && args.PrevLogIndex > 0 {

		reply.Success = false
		term := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 1; i-- {
			if rf.log[i].Term != term {
				reply.FirstBadIndex = i
				break
			}
			if i == 1 {
				reply.FirstBadIndex = 1
			}
		}
		// green light to replicate
	} else {
		knownGoodEntries := rf.log[:args.PrevLogIndex+1]
		rf.log = append(knownGoodEntries, args.Entries...)
		reply.Success = true
		reply.FirstBadIndex = len(rf.log)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
			go func() {
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					DPrintf("[Follower %v] Applying command %v", rf.me, rf.log[i].Command)
					rf.applyCh <- ApplyMsg{Index: i, Command: rf.log[i].Command, Term: rf.currentTerm}
					DPrintf("[Follower %v] Applied log entry %+v", rf.me, rf.log[i].Command)
				}
				rf.lastApplied = rf.commitIndex
			}()
		}
	}
}

func (rf *Raft) assertLeadership() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i || rf.state != LEADER {
			continue
		}
		go func(peer int) {

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				LeaderCommit: rf.commitIndex,
				Entries:      rf.log[rf.nextIndex[peer]:],
			}

			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.log[rf.nextIndex[peer]-1].Term
			}

			if len(args.Entries) == 0 {
				args.IsHeartbeat = true
			}

			reply := AppendEntriesReply{}

			go func() {
				ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)

				if args.Term != rf.currentTerm || rf.state != LEADER {
					return
				}

				if ok {
					if reply.Term > rf.currentTerm {
						rf.state = FOLLOWER
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					} else if reply.Success && !args.IsHeartbeat {
						rf.nextIndex[peer] = reply.FirstBadIndex
						rf.matchIndex[peer] = rf.nextIndex[peer] - 1
					} else {
						rf.nextIndex[peer] = reply.FirstBadIndex
					}
				}

				// 5.3 & 5.4 Leader `commitIndex` Update
				for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
					if rf.log[i].Term == rf.currentTerm {
						servers := 1

						for j := 0; j < len(rf.peers); j++ {
							if rf.matchIndex[j] >= i {
								servers++
							}
						}

						if servers > len(rf.peers)/2 {

							rf.commitIndex = i
							go func() {

								for j := rf.lastApplied + 1; j <= rf.commitIndex; j++ {
									msg := ApplyMsg{Index: j, Command: rf.log[j].Command, UseSnapshot: false, Term: rf.currentTerm}
									DPrintf("[Leader %v] Applying log entry %+v", rf.me, msg)
									rf.applyCh <- msg
									DPrintf("[Leader %v] Applied log entry %+v", rf.me, msg)
								}
								rf.lastApplied = rf.commitIndex
							}()
							break
						}
					}
				}
			}()
		}(i)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
		}

		if len(rf.log) > 1 {
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}

		reply := &RequestVoteReply{}

		go func(peer int) {
			if rf.state == CANDIDATE {
				ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)

				if ok {
					if args.Term != rf.currentTerm || rf.state != CANDIDATE {
						return
					}

					if reply.VoteGranted {
						rf.votesReceived++
						if rf.votesReceived > len(rf.peers)/2 {
							rf.wonElection <- 1
						}
					} else {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						return
					}
				}
			}
		}(i)
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
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.votesReceived = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.heartbeat = make(chan int)
	rf.wonElection = make(chan int)
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.heartbeat:
				case <-time.After(time.Duration(rand.Intn(300)+300) * ms):
					DPrintf("[server %v] Timed out after not hearing from leader. Starting election", rf.me)
					rf.state = CANDIDATE
				}
			case LEADER:
				rf.assertLeadership()
				time.Sleep(150 * ms)

			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votesReceived = 1
				rf.votedFor = rf.me
				rf.mu.Unlock()
				rf.persist()

				rf.startElection()

				select {
				case <-rf.wonElection:
					rf.mu.Lock()
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.state = LEADER
					rf.mu.Unlock()
				case <-rf.heartbeat:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.mu.Unlock()
				case <-time.After(time.Duration(rand.Intn(300)+300) * ms):
				}
			}
		}

	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	return rf
}
