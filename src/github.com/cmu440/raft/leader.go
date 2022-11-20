package raft

import (
	"time"
)

// appendFeedback
// struct that stores extra info needed for handling returned append to other servers
//
type appendFeedback struct {
	reply         *AppendEntriesReply
	serverId      int
	lastSentIndex int
}

// leaderRoutine
// =====
// routine for leader, cased on each different channel signals, runs checkApply and leaderCaseCheckCommit on default
//
func (rf *Raft) leaderRoutine() {
	select {
	// get function call
	case <-rf.getCallChannel:
		rf.leaderCaseGetState()
	// send heartbeat
	case args := <-rf.putCommandChannel:
		rf.casePutCommand(args)
	case <-rf.heartBeatTimer.C:
		rf.leaderCaseCheckFollower()
		rf.leaderCaseHeartBeat()
	// new request
	case args := <-rf.receivedRequestsChannel:
		rf.leaderCaseReceiveRequest(args)
	// new append
	case args := <-rf.receivedAppendChannel:
		rf.leaderCaseReceiveAppend(args)
	// append replied
	case args := <-rf.appendFeedbackChannel:
		rf.leaderCaseFeedbackAppend(args)
	// request replied
	case args := <-rf.requestFeedbackChannel:
		rf.leaderCaseFeedbackRequest(args)
	default:
		rf.leaderCaseCheckCommit()
		rf.checkApply()
	}
}

// leaderToFollower
// =====
// change leader to follower
//
func (rf *Raft) leaderToFollower(term int) {
	rf.logger.Println("reverting leader to follower")
	rf.currentTerm = term
	rf.state = "follower"
	rf.votedFor = -1
	if !rf.heartBeatTimer.Stop() {
		<-rf.heartBeatTimer.C
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		// zero out all leader specific variables
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 0
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
}

// appendEntriesRoutine
// =====
// routine for appending entries, sends requests to others
//
func (rf *Raft) appendEntriesRoutine(serverId int, newAppend *AppendEntriesArgs, lastSentIndex int) {
	newReply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(serverId, newAppend, newReply)
	if ok {
		rf.appendFeedbackChannel <- &appendFeedback{
			reply:         newReply,
			serverId:      serverId,
			lastSentIndex: lastSentIndex,
		}
	}
}

// leaderCaseGetState
// =====
// returns self information
//
func (rf *Raft) leaderCaseGetState() {
	rf.getResultChannel <- &GetInfo{
		Me:       rf.me,
		Term:     rf.currentTerm,
		IsLeader: true,
	}
}

// leaderCaseCheckFollower
// =====
// checks if any follower can be updated with new entries
//
func (rf *Raft) leaderCaseCheckFollower() {
	lastLogIndex := len(rf.log) - 1
	for server := 0; server < len(rf.nextIndex); server++ {
		if server != rf.me && lastLogIndex >= rf.nextIndex[server] {
			// server index outdated, sending logs
			//entries := rf.createEntries(rf.nextIndex[server], lastLogIndex)
			newAppendLog := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
				Entries:      rf.log[rf.nextIndex[server] : lastLogIndex+1],
				LeaderCommit: rf.commitIndex,
			}
			rf.logger.Printf("sending Entries to server %d, len %d \n", server, len(newAppendLog.Entries))
			go rf.appendEntriesRoutine(server, newAppendLog, lastLogIndex)
		}

	}
}

// leaderCaseCheckCommit
// =====
// checks agreement is made and update commit index
//
func (rf *Raft) leaderCaseCheckCommit() {
	resultN := rf.commitIndex
	for N := rf.commitIndex + 1; N < len(rf.log); N++ {
		agreeCount := 1
		for i := 0; i < len(rf.matchIndex); i++ {
			if rf.matchIndex[i] >= N {
				agreeCount += 1
			}
		}
		if agreeCount > len(rf.peers)/2 {
			if rf.log[N].Term == rf.currentTerm {
				//rf.logger.Printf("commit term dont match, my term %d, log term %d \n", rf.currentTerm, rf.log[N].Term)
				resultN = N
			}
		}

	}
	if resultN > rf.commitIndex {
		rf.logger.Printf("changing commit index to  %d \n", resultN)
		rf.commitIndex = resultN
	}
}

// leaderCaseHeartBeat
// =====
// sends heartbeat to servers if needed
//
func (rf *Raft) leaderCaseHeartBeat() {
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId != rf.me {
			if rf.nextIndex[serverId] >= len(rf.log) {
				newAppend := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[serverId] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[serverId]-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				go rf.appendEntriesRoutine(serverId, newAppend, 0)
			}
		}
	}
	rf.heartBeatTimer.Reset(rf.beatInterval * time.Millisecond)
}

// leaderCaseReceiveRequest
// =====
// processes vote requests, only relevant if term outdated
//
func (rf *Raft) leaderCaseReceiveRequest(args *RequestVoteArgs) {
	rf.logger.Println("processing request")
	requestCandidate := args.CandidateId
	requestTerm := args.Term
	reply := &RequestVoteReply{}
	if requestTerm > rf.currentTerm {
		rf.leaderToFollower(requestTerm)
		if rf.checkRequestValid(args) {
			rf.votedFor = requestCandidate
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		rf.logger.Println("discarding vote")
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.resultRequestsChannel <- reply
}

// leaderCaseReceiveAppend
// =====
// processes append requests, only relevant if term outdated
//
func (rf *Raft) leaderCaseReceiveAppend(args *AppendEntriesArgs) {
	rf.logger.Println("(leader) received new append")
	appendTerm := args.Term
	reply := &AppendEntriesReply{}
	if appendTerm > rf.currentTerm {
		rf.logger.Printf("Term outdated, updating to %d \n", appendTerm)
		rf.leaderToFollower(appendTerm)
		reply.Success = false
	} else {
		rf.logger.Println("discarding append")
		reply.Success = false
	}
	reply.Term = rf.currentTerm
	rf.resultAppendChannel <- reply
}

// leaderCaseFeedbackAppend
// =====
// processes resulted appends, decides what to do with server based on success or failure
//
func (rf *Raft) leaderCaseFeedbackAppend(feedback *appendFeedback) {
	reply := feedback.reply
	server := feedback.serverId
	if reply.Term > rf.currentTerm {
		rf.logger.Println("reverting to follower")
		rf.leaderToFollower(reply.Term)
	}

	if feedback.lastSentIndex == 0 {
		// heartbeat
		return
	}

	if reply.Success {
		rf.logger.Println("append successful")
		rf.matchIndex[server] = feedback.lastSentIndex
		rf.nextIndex[server] = feedback.lastSentIndex + 1
	} else {
		rf.logger.Printf("append not successful, updating nextindex to %d", rf.nextIndex[server]-1)
		rf.nextIndex[server] -= 1
	}

}

// leaderCaseFeedbackRequest
// =====
// processes previous vote results, not relevant unless term outdated
//
func (rf *Raft) leaderCaseFeedbackRequest(args *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.leaderToFollower(args.Term)
	}
}
