package raft

import "time"

func (rf *Raft) candidateRoutine() {
	rf.logger.Printf("candidate routine, term %d\n", rf.currentTerm)
	select {
	case <-rf.getCallChannel:
		rf.candidateCaseGetState()
	case <-rf.electionTimer.C:
		rf.candidateCaseElection()
	case args := <-rf.putCommandChannel:
		rf.casePutCommand(args)
	case args := <-rf.receivedRequestsChannel:
		rf.candidateCaseReceiveRequest(args)
	case args := <-rf.receivedAppendChannel:
		rf.candidateCaseReceiveAppend(args)
	case args := <-rf.appendFeedbackChannel:
		rf.candidateCaseFeedbackAppend(args)
	case args := <-rf.requestFeedbackChannel:
		rf.candidateCaseFeedbackRequest(args)
	default:
		rf.checkApply()
	}
	rf.logger.Printf("candidate routine end\n\n")
}

func (rf *Raft) candidateCaseGetState() {
	rf.getResultChannel <- &GetInfo{
		Me:       rf.me,
		Term:     rf.currentTerm,
		IsLeader: false,
	}
}

func (rf *Raft) candidateCaseElection() {
	rf.logger.Println("election timeout, restarting election")
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
				lastLogIndex: len(rf.log) - 1,
				lastLogTerm:  rf.log[len(rf.log)-1].term,
			}
			go rf.voteRequestRoutine(serverId, newRequest)
		}
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
}

func (rf *Raft) candidateCaseReceiveRequest(args *RequestVoteArgs) {
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
}

func (rf *Raft) candidateCaseReceiveAppend(args *AppendEntriesArgs) {
	appendTerm := args.Term
	reply := &AppendEntriesReply{}

	if appendTerm >= rf.currentTerm {
		rf.logger.Printf("term outdated given new append, updating to %d \n", appendTerm)
		rf.currentTerm = appendTerm
		rf.candidateToFollower(args.Term)
	}
	if appendTerm < rf.currentTerm || len(rf.log) <= args.prevLogIndex || rf.log[args.prevLogIndex].term != args.prevLogTerm {
		rf.logger.Println("discarding append")
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.resultAppendChannel <- reply
		return
	}
	// get rid of all entries beyond prevLogIndex and append new entries
	rf.log = rf.log[:args.prevLogIndex+1]
	for entry := range args.entries {
		newLog := &logInfo{
			command: entry,
			term:    rf.currentTerm,
		}
		rf.log = append(rf.log, newLog)
	}
	// update commit index
	if args.leaderCommit > rf.commitIndex {
		if len(rf.log)-1 < args.leaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.leaderCommit
		}
	}
	// reset timer
	rf.resetElectionTimer()
}

func (rf *Raft) candidateCaseFeedbackAppend(feedback *appendFeedback) {
	args := feedback.reply
	if args.Term > rf.currentTerm {
		rf.candidateToFollower(args.Term)
	}
}

func (rf *Raft) candidateCaseFeedbackRequest(args *RequestVoteReply) {
	if args.VoteGranted {
		rf.logger.Printf("vote added, now %d votes\n", rf.votesReceived)
		rf.votesReceived += 1
		if rf.votesReceived > len(rf.peers)/2 {
			rf.candidateToLeader()
		}
	} else if args.Term > rf.currentTerm {
		rf.candidateToFollower(args.Term)
	}
}

func (rf *Raft) candidateToFollower(term int) {
	rf.currentTerm = term
	rf.state = "follower"
	rf.votesReceived = 0
	rf.votedFor = -1
	rf.resetElectionTimer()
}

func (rf *Raft) candidateToLeader() {
	rf.logger.Println("received enough votes, becoming leader")
	rf.state = "leader"
	rf.votedFor = -1
	if !rf.electionTimer.Stop() {
		<-rf.electionTimer.C
	}
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
	rf.heartBeatTimer = time.NewTimer(rf.beatInterval * time.Millisecond)
}
