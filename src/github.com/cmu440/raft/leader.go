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
	rf.logger.Printf("leader routine, term %d\n", rf.currentTerm)
	select {
	// get function call
	case <-rf.getCallChannel:
		rf.leaderCaseGetState()
	// send heartbeat
	case args := <-rf.putCommandChannel:
		rf.leaderCasePutCommand(args)
	case <-rf.heartBeatTimer.C:
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
		rf.leaderCaseCheckFollower()
		rf.leaderCaseCheckCommit()
		rf.leaderCaseCheckApply()
	}
	rf.logger.Printf("leader routine end\n\n")
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
		if lastLogIndex >= rf.nextIndex[server] {
			// server index outdated, sending logs
			newAppendLog := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				prevLogIndex: rf.nextIndex[server] - 1,
				prevLogTerm:  rf.log[rf.nextIndex[server]-1].term,
				entries:      rf.createEntries(rf.nextIndex[server], lastLogIndex),
				leaderCommit: rf.commitIndex,
			}
			rf.appendEntriesRoutine(server, newAppendLog, lastLogIndex)
		}

	}
}

func (rf *Raft) leaderCaseCheckCommit() {
	N := rf.commitIndex + 1
	for ; N < len(rf.log); N++ {
		agreeCount := 0
		for i := 0; i < len(rf.matchIndex); i++ {
			if rf.matchIndex[i] >= N {
				agreeCount += 1
			}
		}
		if agreeCount <= len(rf.peers)/2 || rf.log[N].term != rf.currentTerm {
			break
		}
	}
	N -= 1
	if N > rf.commitIndex {
		rf.commitIndex = N
	}
}

func (rf *Raft) leaderCaseCheckApply() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		newApply := ApplyCommand{
			Index:   rf.lastApplied,
			Command: rf.log[rf.lastApplied].command,
		}
		rf.applyCh <- newApply
	}
}

func (rf *Raft) leaderCasePutCommand(command interface{}) {
	logEntry := &logInfo{
		command: command,
		term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	putFeedback := &putCommandFeedback{
		index:    len(rf.log) - 1,
		term:     rf.currentTerm,
		isLeader: true,
	}
	rf.putCommandFeedbackChannel <- putFeedback
}

func (rf *Raft) leaderCaseHeartBeat() {
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId != rf.me {
			newAppend := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				prevLogIndex: -1,
				prevLogTerm:  -1,
				entries:      nil,
				leaderCommit: rf.commitIndex,
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
		rf.votedFor = requestCandidate
		reply.VoteGranted = true
	} else {
		rf.logger.Println("discarding vote")
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.resultRequestsChannel <- reply
}

func (rf *Raft) leaderCaseReceiveAppend(args *AppendEntriesArgs) {
	appendTerm := args.Term
	reply := &AppendEntriesReply{}
	if appendTerm > rf.currentTerm {
		rf.logger.Printf("term outdated, updating to %d \n", appendTerm)
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
	reply := feedback.reply
	server := feedback.serverId
	if reply.Term > rf.currentTerm {
		rf.leaderToFollower(reply.Term)
	}

	if feedback.lastSentIndex == 0 {
		// heartbeat
		return
	}

	if reply.Success {
		rf.matchIndex[server] = feedback.lastSentIndex
		rf.nextIndex[server] = feedback.lastSentIndex + 1
	} else {
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
func (rf *Raft) createEntries(start int, end int) []interface{} {
	entries := make([]interface{}, 0)
	for i := start; i <= end; i++ {
		entry := rf.log[i].command
		entries = append(entries, entry)
	}
	return entries
}
