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
const kEnableDebugLogs = false

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

// GetInfo
// Struct used for communicating with main routine when server needs to return its information
type GetInfo struct {
	Me       int
	Term     int
	IsLeader bool
}

// LogInfo
// Struct that the log slice stores, includes term on top of command entry
type LogInfo struct {
	Command interface{}
	Term    int
}

// putCommandFeedback
// Struct containing info on the result of a putCommand call
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

type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state, isn't used in this implementation
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]

	logger *log.Logger // We provide you with a separate logger per peer.

	currentTerm   int       // current term of the server
	votedFor      int       // candidate voted this term
	state         string    // the role of the server, either "follower", "candidate", or "leader
	votesReceived int       // total vote received from other severs
	log           []LogInfo // logs stored
	// Basic Raft immutable variables
	electionTimeoutWindow time.Duration // time window before election timeout
	beatInterval          time.Duration // time window before heartbeat/send append
	// Timer communication variables
	heartBeatTimer *time.Timer // timer for append entry
	electionTimer  *time.Timer // timer for election timeout
	// vote communication variables
	voteChannel chan bool
	// channels
	receivedRequestsChannel chan *RequestVoteArgs    // channel for all received voting requests
	receivedAppendChannel   chan *AppendEntriesArgs  // channel for all received heartbeat/entries
	resultRequestsChannel   chan *RequestVoteReply   // channel for returning vote request result
	resultAppendChannel     chan *AppendEntriesReply // channel for returning append result
	requestFeedbackChannel  chan *RequestVoteReply   // channel for received result of vote from other servers
	appendFeedbackChannel   chan *appendFeedback     // channel for received result of append from other servers

	getCallChannel            chan bool                // channel to inform main routine GetState is called
	getResultChannel          chan *GetInfo            // channel to return GetState the information
	applyCh                   chan ApplyCommand        // channel for applying entries, accessed only by the applyRoutine
	putCommandChannel         chan interface{}         // channel to inform main routine PutCommand is called
	putCommandFeedbackChannel chan *putCommandFeedback // channel to return PutCommand the information

	sendApplyChannel chan ApplyCommand // channel to send applyRoutine entries to apply
	// leader specific slices
	nextIndex  []int // next index info for other servers
	matchIndex []int // match index info for other servers
	// values for log tracking
	commitIndex int // latest index committed
	lastApplied int // latest index applied
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

// Standard RPC structs as according to the handout

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

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

// end of standard structs

//
// RequestVote
// ===========
//
// sends the arguments to main routine and returns the reply from main routine
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.receivedRequestsChannel <- args
	replyValue := <-rf.resultRequestsChannel
	reply.Term = replyValue.Term
	reply.VoteGranted = replyValue.VoteGranted
}

//
// AppendEntries
// ===========
//
// sends the arguments to main routine and returns the reply from main routine
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.receivedAppendChannel <- args
	replyValue := <-rf.resultAppendChannel
	reply.Term = replyValue.Term
	reply.Success = replyValue.Success
}

//
// sendRequestVote
// ===============
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// sendAppendEntries
// ===============
// server int -- index of the target server in
// rf.peers[]
//
// args *AppendEntriesArgs -- RPC arguments in args
//
// reply *AppendEntriesReply -- RPC reply
//
//
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
// Initializes everything and kicks off the main routine and apply routine
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
	rf.beatInterval = 300

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

	rf.sendApplyChannel = make(chan ApplyCommand, 1000000)
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

	rf.electionTimer = time.NewTimer(rf.electionTimeoutWindow * time.Millisecond)

	go rf.applyRoutine()
	go rf.serverRoutine()
	return rf
}

// serverRoutine
// =====
// the main routine, partitions into three cases, executes based on its current role
//
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

// applyRoutine
// =====
// routine that sends the committed applies to be actually applied, listens to the main routine's apply
// messages
//
func (rf *Raft) applyRoutine() {
	for {
		select {
		case newApply := <-rf.sendApplyChannel:
			rf.logger.Printf("apply executed \n")
			rf.applyCh <- newApply
		}
	}
}

// resetElectionTimer
// =====
// resets the election timer
//
func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		<-rf.electionTimer.C
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
}

// resetElectionTimer
// =====
// checks if any entry is committed and can be applied. If so, send the message to applyRoutine via channel
//
func (rf *Raft) checkApply() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		newApply := ApplyCommand{
			Index:   rf.lastApplied,
			Command: rf.log[rf.lastApplied].Command,
		}
		rf.logger.Printf("applying Command index %d, Command = %v \n", rf.lastApplied, rf.log[rf.lastApplied].Command.(int))
		rf.sendApplyChannel <- newApply
	}
}

// casePutCommand
// =====
// processes PutCommand requests in the main routine, only append if role is leader
//
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

// checkRequestValid
// =====
// checks if a given request if valid: check if the term is at least as new and log length at least as long
//
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

// processAppend
// =====
// processes append messages including heartbeats, goes with paper instructions
// checks term and PrevLogIndex's term
// updates self commit index if needed
// if append is valid, updates self log
// used only by candidate and followers
//
func (rf *Raft) processAppend(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := &AppendEntriesReply{}
	reply.Term = rf.currentTerm
	appendTerm := args.Term
	if appendTerm < rf.currentTerm {
		rf.logger.Printf("append outdated, my Term %d, append Term %d \n", rf.currentTerm, appendTerm)
		reply.Success = false
		return reply
	}

	rf.resetElectionTimer()

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logger.Printf("Prev log does not match \n")
		reply.Success = false
		return reply
	}

	if args.Entries == nil {
		reply.Success = false
		if args.LeaderCommit > rf.commitIndex {

			if len(rf.log)-1 < args.LeaderCommit {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.logger.Printf("follower commit index changed to %d\n", rf.commitIndex)
		}
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
	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.logger.Printf("follower commit index changed to %d\n", rf.commitIndex)
	}
	return reply
}
