package raft

import (
	"time"
)

// followerRoutine
// =====
// routine for follower, cased on each different channel signals, runs checkApply on default
//
func (rf *Raft) followerRoutine() {
	select {
	case <-rf.getCallChannel:
		rf.followerCaseGetState()
	case <-rf.electionTimer.C:
		rf.followerCaseElection()
	case args := <-rf.putCommandChannel:
		rf.casePutCommand(args)
	case args := <-rf.receivedRequestsChannel:
		rf.followerCaseReceiveRequest(args)
	case args := <-rf.receivedAppendChannel:
		rf.followerCaseReceiveAppend(args)
	case args := <-rf.appendFeedbackChannel:
		rf.followerCaseFeedbackAppend(args)
	case args := <-rf.requestFeedbackChannel:
		rf.followerCaseFeedbackRequest(args)
	default:
		rf.checkApply()
	}
}

// followerCaseGetState
// =====
// returns self information
//
func (rf *Raft) followerCaseGetState() {
	rf.getResultChannel <- &GetInfo{
		Me:       rf.me,
		Term:     rf.currentTerm,
		IsLeader: false,
	}
}

// followerCaseElection
// =====
// starts off election after election timeout, turn into candidate
//
func (rf *Raft) followerCaseElection() {
	rf.logger.Println("election timeout, starting next election")
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived = 1
	// emptying the vote channel
	select {
	case <-rf.voteChannel:
	default:
	}
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId != rf.me {
			newRequest := &RequestVoteArgs{
				CandidateId:  rf.me,
				Term:         rf.currentTerm,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			go rf.voteRequestRoutine(serverId, newRequest)
		}
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
	rf.state = "candidate"
}

// followerCaseReceiveRequest
// =====
// processes vote requests from other candidates, checks validation based on paper
//
func (rf *Raft) followerCaseReceiveRequest(args *RequestVoteArgs) {
	rf.logger.Println("processing request")
	requestCandidate := args.CandidateId
	requestTerm := args.Term
	reply := &RequestVoteReply{}
	if requestTerm > rf.currentTerm {
		rf.logger.Printf("Term outdated given new request, updating to %d \n", requestTerm)
		rf.currentTerm = requestTerm
		rf.votedFor = -1
	}

	if requestTerm < rf.currentTerm {
		rf.logger.Printf("request outdated, discarding \n")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.resultRequestsChannel <- reply
		return
	}

	if rf.votedFor == -1 {

		if rf.checkRequestValid(args) {
			rf.logger.Println("voted")
			rf.votedFor = requestCandidate
			reply.VoteGranted = true
			rf.resetElectionTimer()
		} else {
			rf.logger.Println("request not valid")
			reply.VoteGranted = false
		}
	} else {
		rf.logger.Println("discarding vote")
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.resultRequestsChannel <- reply
}

// followerCaseReceiveAppend
// =====
// processes append requests, calls on processAppend, also checks if term need updating
//
func (rf *Raft) followerCaseReceiveAppend(args *AppendEntriesArgs) {
	//rf.logger.Printf("processing append, Term %d, leader %d, previdx %d, prevterm %d, entry len %d\n",
	//	args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	appendTerm := args.Term
	if appendTerm > rf.currentTerm {
		rf.logger.Printf("Term outdated given new append, updating to %d \n", appendTerm)
		rf.currentTerm = appendTerm
	}
	rf.resultAppendChannel <- rf.processAppend(args)
}

// followerCaseFeedbackAppend
// =====
// previous append's result, only relevant if term needs updating
//
func (rf *Raft) followerCaseFeedbackAppend(feedback *appendFeedback) {
	args := feedback.reply
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}
}

// followerCaseFeedbackRequest
// =====
// previous request's result, only relevant if term needs updating
//
func (rf *Raft) followerCaseFeedbackRequest(args *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}
}

// voteRequestRoutine
// =====
// routine for voting, sends requests to others
//
func (rf *Raft) voteRequestRoutine(serverId int, newRequest *RequestVoteArgs) {
	newReply := &RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, newRequest, newReply)
	if ok {
		rf.logger.Printf("vote from %d, Term %d, grated %t\n", serverId, newRequest.Term, newReply.VoteGranted)
		rf.requestFeedbackChannel <- newReply
	}

}
