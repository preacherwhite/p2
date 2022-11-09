//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me", its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"github.com/cmu440/rpc"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = true

// Set to true to log to stdout instead of file
const kLogToStdout = false

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

//
// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
//
type ApplyCommand struct {
	Index   int
	Command interface{}
}

type GetInfo struct {
	Me       int
	Term     int
	IsLeader bool
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// Your data here (2A, 2B).

	// Basic Raft variables that needs protection
	currentTerm   int
	votedFor      int
	state         string
	votesReceived int
	// Basic Raft immutable variables
	electionTimeoutWindow time.Duration
	beatInterval          time.Duration
	// Timer communication variables
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
	// vote communication variables
	voteChannel chan bool
	// handler communication
	receivedRequestsChannel chan *RequestVoteArgs
	receivedAppendChannel   chan *AppendEntriesArgs
	resultRequestsChannel   chan *RequestVoteReply
	resultAppendChannel     chan *AppendEntriesReply
	requestFeedbackChannel  chan *RequestVoteReply
	appendFeedbackChannel   chan *AppendEntriesReply
	getCallChannel          chan bool
	getResultChannel        chan *GetInfo
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	rf.getCallChannel <- true
	result := <-rf.getResultChannel
	return result.Me, result.Term, result.IsLeader
}

//
// RequestVoteArgs
// ===============

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

//
// RequestVoteReply
// ================

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.logger.Printf("new request received from server %d, term %d \n", args.CandidateId, args.Term)
	rf.receivedRequestsChannel <- args
	rf.logger.Printf("new request processed from server %d, term %d \n", args.CandidateId, args.Term)
	replyValue := <-rf.resultRequestsChannel
	reply.Term = replyValue.Term
	reply.VoteGranted = replyValue.VoteGranted
	rf.logger.Printf("new request delivered from server %d, term %d \n", args.CandidateId, args.Term)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//source := args.LeaderId
	rf.logger.Printf("new append received from server %d, term %d \n", args.LeaderId, args.Term)
	rf.receivedAppendChannel <- args
	rf.logger.Printf("new append processed from server %d, term %d \n", args.LeaderId, args.Term)
	replyValue := <-rf.resultAppendChannel
	reply.Term = replyValue.Term
	reply.Success = replyValue.Success
	rf.logger.Printf("new append delivered from server %d, term %d \n", args.LeaderId, args.Term)
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.logger.Printf("sending vote request to server %d\n", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.logger.Printf("vote reply received from server %d\n", server)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.logger.Printf("sending append to server %d\n", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.logger.Printf("append reply received from server %d\n", server)
	return ok
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

	return index, term, isLeader
}

//
// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Stop() {
	// Your code here, if desired
}

//
// NewPeer
// ====
//
// The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
//
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.mux = sync.Mutex{}

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesReceived = 0
	rf.electionTimeoutWindow = time.Duration(rand.Intn(1000) + 1000)
	rf.beatInterval = 200
	rf.state = "follower"
	rf.voteChannel = make(chan bool)
	rf.receivedAppendChannel = make(chan *AppendEntriesArgs)
	rf.receivedRequestsChannel = make(chan *RequestVoteArgs)
	rf.resultRequestsChannel = make(chan *RequestVoteReply)
	rf.resultAppendChannel = make(chan *AppendEntriesReply)
	rf.requestFeedbackChannel = make(chan *RequestVoteReply)
	rf.appendFeedbackChannel = make(chan *AppendEntriesReply)
	rf.getResultChannel = make(chan *GetInfo)
	rf.getCallChannel = make(chan bool)
	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Printf("logger initialized, %d server, timeout window %d \n", me, rf.electionTimeoutWindow)
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (2A, 2B)
	rf.electionTimer = time.NewTimer(rf.electionTimeoutWindow * time.Millisecond)
	go rf.serverRoutine()

	return rf
}

func (rf *Raft) serverRoutine() {
	for {
		rf.mux.Lock()
		if rf.state == "follower" {
			rf.followerRoutine()
		} else if rf.state == "leader" {
			rf.leaderRoutine()
		} else {
			rf.candidateRoutine()
		}
		rf.mux.Unlock()
	}
}

func (rf *Raft) followerRoutine() {
	rf.logger.Printf("follower routine, term %d\n", rf.currentTerm)
	select {
	case <-rf.getCallChannel:
		rf.getResultChannel <- &GetInfo{
			Me:       rf.me,
			Term:     rf.currentTerm,
			IsLeader: false,
		}
	case <-rf.electionTimer.C:
		rf.logger.Println("election timeout, starting next election")
		rf.electionSetup()
		rf.state = "candidate"
	case args := <-rf.receivedRequestsChannel:
		rf.logger.Println("processing request")
		requestCandidate := args.CandidateId
		requestTerm := args.Term
		reply := &RequestVoteReply{}
		if requestTerm > rf.currentTerm {
			rf.logger.Printf("term outdated given new request, updating to %d \n", requestTerm)
			rf.currentTerm = requestTerm
			rf.votedFor = -1
			rf.resetElectionTimer()
		}
		if requestTerm == rf.currentTerm && rf.votedFor == -1 {
			rf.logger.Println("voted")
			rf.votedFor = requestCandidate
			reply.VoteGranted = true
			rf.resetElectionTimer()
		} else {
			rf.logger.Println("discarding vote")
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm
		rf.resultRequestsChannel <- reply
	case args := <-rf.receivedAppendChannel:
		appendTerm := args.Term
		reply := &AppendEntriesReply{}
		if appendTerm >= rf.currentTerm {
			rf.logger.Printf("term outdated given new append, updating to %d \n", appendTerm)
			rf.currentTerm = appendTerm
			rf.resetElectionTimer()
			reply.Success = true
		} else if appendTerm < rf.currentTerm {
			rf.logger.Println("discarding append")
			reply.Success = false
		}
		reply.Term = rf.currentTerm
		rf.resultAppendChannel <- reply
	case args := <-rf.appendFeedbackChannel:
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.resetElectionTimer()
		}
	case args := <-rf.requestFeedbackChannel:
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.resetElectionTimer()
		}

	}
	rf.logger.Printf("follower routine end\n\n")
}

func (rf *Raft) voteRequestRoutine(serverId int, me int, currentTerm int) {
	newReply := &RequestVoteReply{}
	newRequest := &RequestVoteArgs{
		CandidateId: me,
		Term:        currentTerm,
	}
	ok := rf.sendRequestVote(serverId, newRequest, newReply)
	if ok {
		rf.logger.Printf("vote from %d, term %d, grated %t\n", serverId, newRequest.Term, newReply.VoteGranted)
		rf.requestFeedbackChannel <- newReply
	}

}

func (rf *Raft) leaderRoutine() {
	rf.logger.Printf("leader routine, term %d\n", rf.currentTerm)
	select {
	// get function call
	case <-rf.getCallChannel:
		rf.getResultChannel <- &GetInfo{
			Me:       rf.me,
			Term:     rf.currentTerm,
			IsLeader: true,
		}
	// send heartbeat
	case <-rf.heartBeatTimer.C:
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me {
				go rf.appendEntriesRoutine(serverId, rf.me, rf.currentTerm)
			}
		}
		rf.heartBeatTimer.Reset(rf.beatInterval * time.Millisecond)
	// new request
	case args := <-rf.receivedRequestsChannel:
		rf.logger.Println("processing request")
		requestCandidate := args.CandidateId
		requestTerm := args.Term
		reply := &RequestVoteReply{}
		if requestTerm > rf.currentTerm {
			rf.leaderToFollower(requestTerm)
			rf.votedFor = requestCandidate
			reply.VoteGranted = true
		} else {
			rf.logger.Println("discarding vote")
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm
		rf.resultRequestsChannel <- reply
	// new append
	case args := <-rf.receivedAppendChannel:
		appendTerm := args.Term
		reply := &AppendEntriesReply{}
		if appendTerm > rf.currentTerm {
			rf.logger.Printf("term outdated, updating to %d \n", appendTerm)
			rf.leaderToFollower(appendTerm)
			reply.Success = true
		} else {
			rf.logger.Println("discarding append")
			reply.Success = false
		}
		reply.Term = rf.currentTerm
		rf.resultAppendChannel <- reply
	// append replied
	case args := <-rf.appendFeedbackChannel:
		if args.Term > rf.currentTerm {
			rf.leaderToFollower(args.Term)
		}
	// request replied
	case args := <-rf.requestFeedbackChannel:
		if args.Term > rf.currentTerm {
			rf.leaderToFollower(args.Term)
		}
	}
	rf.logger.Printf("leader routine end\n\n")
}

func (rf *Raft) appendEntriesRoutine(serverId int, me int, currentTerm int) {
	newReply := &AppendEntriesReply{}
	newAppend := &AppendEntriesArgs{
		Term:     currentTerm,
		LeaderId: me,
	}
	ok := rf.sendAppendEntries(serverId, newAppend, newReply)
	if ok {
		rf.appendFeedbackChannel <- newReply
	}
}

func (rf *Raft) candidateRoutine() {
	rf.logger.Printf("candidate routine, term %d\n", rf.currentTerm)
	select {
	case <-rf.getCallChannel:
		rf.getResultChannel <- &GetInfo{
			Me:       rf.me,
			Term:     rf.currentTerm,
			IsLeader: false,
		}
	case <-rf.electionTimer.C:
		rf.logger.Println("election timeout, restarting election")
		rf.electionSetup()
	case args := <-rf.receivedRequestsChannel:
		rf.logger.Println("processing request")
		requestCandidate := args.CandidateId
		requestTerm := args.Term
		reply := &RequestVoteReply{}
		if requestTerm > rf.currentTerm {
			rf.logger.Printf("term outdated given new request, updating to %d and voting\n", requestTerm)
			rf.candidateToFollower(args.Term)
			rf.votedFor = requestCandidate
			reply.VoteGranted = true
			rf.resetElectionTimer()
		} else {
			rf.logger.Println("discarding vote")
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm
		rf.resultRequestsChannel <- reply
	case args := <-rf.receivedAppendChannel:
		appendTerm := args.Term
		reply := &AppendEntriesReply{}
		if appendTerm < rf.currentTerm {
			rf.logger.Println("discarding append")
			reply.Success = false
		} else {
			rf.logger.Println("resetting election timer")
			rf.candidateToFollower(args.Term)
			reply.Success = true
		}
		reply.Term = rf.currentTerm
		rf.resultAppendChannel <- reply
	case args := <-rf.appendFeedbackChannel:
		if args.Term > rf.currentTerm {
			rf.candidateToFollower(args.Term)
		}
	case args := <-rf.requestFeedbackChannel:
		if args.VoteGranted {
			rf.logger.Printf("vote added, now %d votes\n", rf.votesReceived)
			rf.votesReceived += 1
			if rf.votesReceived > len(rf.peers)/2 {
				rf.logger.Println("received enough votes, becoming leader")
				rf.state = "leader"
				rf.votedFor = -1
				if !rf.electionTimer.Stop() {
					<-rf.electionTimer.C
				}
				rf.heartBeatTimer = time.NewTimer(rf.beatInterval * time.Millisecond)
			}
		} else if args.Term > rf.currentTerm {
			rf.candidateToFollower(args.Term)
		}
	}
	rf.logger.Printf("candidate routine end\n\n")
}

func (rf *Raft) leaderToFollower(term int) {
	rf.logger.Println("reverting leader to follower")
	rf.currentTerm = term
	rf.state = "follower"
	rf.votedFor = -1
	if !rf.heartBeatTimer.Stop() {
		<-rf.heartBeatTimer.C
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
}

func (rf *Raft) candidateToFollower(term int) {
	rf.currentTerm = term
	rf.state = "follower"
	rf.votesReceived = 0
	rf.votedFor = -1
	rf.resetElectionTimer()
}

func (rf *Raft) resetElectionTimer() {
	rf.logger.Println("resetting election timer")
	if !rf.electionTimer.Stop() {
		<-rf.electionTimer.C
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
}
func (rf *Raft) electionSetup() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived = 1
	//rf.voteChannel = make(chan bool, 500)
	select {
	case <-rf.voteChannel:
	default:
	}
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId != rf.me {
			go rf.voteRequestRoutine(serverId, rf.me, rf.currentTerm)
		}
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
}
