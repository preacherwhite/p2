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
// rf.PutCommand(Command interface{}) (index, Term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, Term, isLeader)
//   Ask a Raft peer for "me", its current Term, and whether it thinks it
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
// As each Raft peer becomes aware that successive log Entries are
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

type LogInfo struct {
	Command interface{}
	Term    int
}

type putCommandFeedback struct {
	index    int
	term     int
	isLeader bool
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
	log           []LogInfo
	// Basic Raft immutable variables
	electionTimeoutWindow time.Duration
	beatInterval          time.Duration
	// Timer communication variables
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
	// vote communication variables
	voteChannel chan bool
	// channels
	receivedRequestsChannel chan *RequestVoteArgs
	receivedAppendChannel   chan *AppendEntriesArgs
	resultRequestsChannel   chan *RequestVoteReply
	resultAppendChannel     chan *AppendEntriesReply
	requestFeedbackChannel  chan *RequestVoteReply
	appendFeedbackChannel   chan *appendFeedback

	getCallChannel            chan bool
	getResultChannel          chan *GetInfo
	applyCh                   chan ApplyCommand
	putCommandChannel         chan interface{}
	putCommandFeedbackChannel chan *putCommandFeedback
	// leader specific slices
	nextIndex  []int
	matchIndex []int
	// values for log tracking
	commitIndex int
	lastApplied int
}

//
// GetState()
// ==========
//
// Return "me", current Term and whether this peer
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply
// ================

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogInfo
	LeaderCommit int
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
	//rf.logger.Printf("new request received from server %d, Term %d \n", args.CandidateId, args.Term)
	rf.receivedRequestsChannel <- args
	replyValue := <-rf.resultRequestsChannel
	reply.Term = replyValue.Term
	reply.VoteGranted = replyValue.VoteGranted
	//rf.logger.Printf("new request replied to server %d, Term %d \n", args.CandidateId, args.Term)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//source := args.LeaderId
	//rf.logger.Printf("new append received from server %d, Term %d \n", args.LeaderId, args.Term)
	rf.receivedAppendChannel <- args
	replyValue := <-rf.resultAppendChannel
	reply.Term = replyValue.Term
	reply.Success = replyValue.Success
	//rf.logger.Printf("new append replied to server %d, Term %d \n", args.LeaderId, args.Term)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this Command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the Command will appear at
// if it is ever committed
//
// The second return value is the current Term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.putCommandChannel <- command
	reply := <-rf.putCommandFeedbackChannel
	return reply.index, reply.term, reply.isLeader
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
	rf.state = "follower"
	rf.votesReceived = 0
	rf.log = make([]LogInfo, 1)

	rf.electionTimeoutWindow = time.Duration(rand.Intn(1000) + 1000)
	rf.beatInterval = 500

	rf.voteChannel = make(chan bool)

	rf.receivedRequestsChannel = make(chan *RequestVoteArgs)
	rf.receivedAppendChannel = make(chan *AppendEntriesArgs)
	rf.resultRequestsChannel = make(chan *RequestVoteReply)
	rf.resultAppendChannel = make(chan *AppendEntriesReply)
	rf.requestFeedbackChannel = make(chan *RequestVoteReply)
	rf.appendFeedbackChannel = make(chan *appendFeedback)

	rf.getCallChannel = make(chan bool)
	rf.getResultChannel = make(chan *GetInfo)
	rf.applyCh = applyCh
	rf.putCommandChannel = make(chan interface{})
	rf.putCommandFeedbackChannel = make(chan *putCommandFeedback)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.commitIndex = 0
	rf.lastApplied = 0

	// insert dummy into 0 index
	rf.log[0] = LogInfo{
		Command: nil,
		Term:    -1,
	}

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
		//rf.mux.Lock()
		if rf.state == "follower" {
			rf.followerRoutine()
		} else if rf.state == "leader" {
			rf.leaderRoutine()
		} else {
			rf.candidateRoutine()
		}
		//rf.mux.Unlock()
	}
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		<-rf.electionTimer.C
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
}

func (rf *Raft) checkApply() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		newApply := ApplyCommand{
			Index:   rf.lastApplied,
			Command: rf.log[rf.lastApplied].Command,
		}
		rf.logger.Printf("applying Command index %d, Command = %v \n", rf.lastApplied, rf.log[rf.lastApplied].Command.(int))
		rf.applyCh <- newApply
	}
}

func (rf *Raft) casePutCommand(command interface{}) {
	logEntry := LogInfo{
		Command: command,
		Term:    rf.currentTerm,
	}
	putFeedback := &putCommandFeedback{
		index:    len(rf.log),
		term:     rf.currentTerm,
		isLeader: rf.state == "leader",
	}
	if rf.state == "leader" {
		rf.logger.Printf("leader, appending new Command, term %d, command %v\n", logEntry.Term, logEntry.Command.(int))
		rf.log = append(rf.log, logEntry)
	}
	rf.putCommandFeedbackChannel <- putFeedback
}

func (rf *Raft) checkRequestValid(request *RequestVoteArgs) bool {
	rf.logger.Printf("checking request, lastlogterm %d, lastindex %d. my last log Term %d, my last log index %d \n",
		request.LastLogTerm, request.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)
	if request.LastLogTerm > rf.log[len(rf.log)-1].Term {
		return true
	} else if request.LastLogTerm < rf.log[len(rf.log)-1].Term {
		return false
	} else {
		if request.LastLogIndex >= len(rf.log)-1 {
			return true
		} else {
			return false
		}
	}
}

func (rf *Raft) processAppend(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := &AppendEntriesReply{}
	reply.Term = rf.currentTerm
	appendTerm := args.Term
	if appendTerm < rf.currentTerm {
		rf.logger.Printf("append outdated, my Term %d, append Term %d \n", rf.currentTerm, appendTerm)
		reply.Success = false
		return reply
	}

	if args.Entries == nil {
		//new heartbeat
		//rf.logger.Printf("heartbeat received \n")
		rf.resetElectionTimer()
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logger.Printf("Prev log does not match \n")
		reply.Success = false
		return reply
	}

	// update commit index
	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	if args.Entries == nil {
		reply.Success = false
		return reply
	}

	rf.logger.Println("append successful")
	// get rid of all Entries beyond PrevLogIndex and append new Entries
	rf.log = rf.log[:args.PrevLogIndex+1]
	for i := range args.Entries {
		rf.logger.Printf("appending Command %v \n", args.Entries[i].Command.(int))
		rf.log = append(rf.log, args.Entries[i])
	}

	reply.Success = true
	rf.resetElectionTimer()
	return reply
}
