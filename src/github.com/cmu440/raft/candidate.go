package raft

import "time"

func (rf *Raft) candidateRoutine() {
	rf.logger.Printf("candidate routine, term %d\n", rf.currentTerm)
	select {
	case <-rf.getCallChannel:
		rf.candidateCaseGetState()
	case <-rf.electionTimer.C:
		rf.candidateCaseElection()
	case args := <-rf.receivedRequestsChannel:
		rf.candidateCaseReceiveRequest(args)
	case args := <-rf.receivedAppendChannel:
		rf.candidateCaseReceiveAppend(args)
	case args := <-rf.appendFeedbackChannel:
		rf.candidateCaseFeedbackAppend(args)
	case args := <-rf.requestFeedbackChannel:
		rf.candidateCaseFeedbackRequest(args)
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
	rf.electionSetup()
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
}

func (rf *Raft) candidateCaseFeedbackAppend(args *AppendEntriesReply) {
	if args.Term > rf.currentTerm {
		rf.candidateToFollower(args.Term)
	}
}

func (rf *Raft) candidateCaseFeedbackRequest(args *RequestVoteReply) {
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

func (rf *Raft) candidateToFollower(term int) {
	rf.currentTerm = term
	rf.state = "follower"
	rf.votesReceived = 0
	rf.votedFor = -1
	rf.resetElectionTimer()
}
