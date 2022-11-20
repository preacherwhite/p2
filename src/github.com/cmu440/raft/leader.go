package raft

import (
	"time"
)

type appendFeedback struct {
	reply         *AppendEntriesReply
	serverId      int
	lastSentIndex int
}

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

func (rf *Raft) leaderCaseGetState() {
	rf.getResultChannel <- &GetInfo{
		Me:       rf.me,
		Term:     rf.currentTerm,
		IsLeader: true,
	}
}

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

func (rf *Raft) leaderCaseHeartBeat() {
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId != rf.me {
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
	rf.heartBeatTimer.Reset(rf.beatInterval * time.Millisecond)
}

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

func (rf *Raft) leaderCaseFeedbackAppend(feedback *appendFeedback) {
	//rf.logger.Println("append feedback")
	reply := feedback.reply
	server := feedback.serverId
	if reply.Term > rf.currentTerm {
		rf.logger.Println("reverting to follower")
		rf.leaderToFollower(reply.Term)
	}

	if feedback.lastSentIndex == 0 {
		//rf.logger.Println("heartbeat feedback")
		return
	}

	if reply.Success {
		rf.logger.Println("append successful")
		rf.matchIndex[server] = feedback.lastSentIndex
		rf.nextIndex[server] = feedback.lastSentIndex + 1
	} else {
		rf.logger.Printf("append not successful, updating nextindex to %d", rf.nextIndex[server]-1)
		rf.nextIndex[server] -= 1
		// TODO: design decision, don't retry here and leave it to next server loop
	}

}

func (rf *Raft) leaderCaseFeedbackRequest(args *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.leaderToFollower(args.Term)
	}
}

//start and end inclusive
//func (rf *Raft) createEntries(start int, end int) []interface{} {
//	entries := make([]interface{}, 0)
//	for i := start; i <= end; i++ {
//		entry := rf.log[i].Command
//		entries = append(entries, entry)
//		rf.logger.Printf("creating entry with entry %v \n", entry.(int))
//	}
//	return entries
//}
