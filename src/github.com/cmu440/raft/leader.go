package raft

import "time"

func (rf *Raft) leaderRoutine() {
	rf.logger.Printf("leader routine, term %d\n", rf.currentTerm)
	select {
	// get function call
	case <-rf.getCallChannel:
		rf.leaderCaseGetState()
	// send heartbeat
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
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
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

func (rf *Raft) leaderCaseGetState() {
	rf.getResultChannel <- &GetInfo{
		Me:       rf.me,
		Term:     rf.currentTerm,
		IsLeader: true,
	}
}

func (rf *Raft) leaderCaseHeartBeat() {
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId != rf.me {
			go rf.appendEntriesRoutine(serverId, rf.me, rf.currentTerm)
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
		reply.Success = true
	} else {
		rf.logger.Println("discarding append")
		reply.Success = false
	}
	reply.Term = rf.currentTerm
	rf.resultAppendChannel <- reply
}

func (rf *Raft) leaderCaseFeedbackAppend(args *AppendEntriesReply) {
	if args.Term > rf.currentTerm {
		rf.leaderToFollower(args.Term)
	}
}

func (rf *Raft) leaderCaseFeedbackRequest(args *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.leaderToFollower(args.Term)
	}
}
