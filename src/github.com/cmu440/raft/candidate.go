package raft

import "time"

func (rf *Raft) candidateRoutine() {
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
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
		rf.logger.Printf("Term outdated given new request, updating to %d and voting\n", requestTerm)
		rf.candidateToFollower(args.Term)
		if rf.checkRequestValid(args) {
			rf.votedFor = requestCandidate
			reply.VoteGranted = true
			rf.resetElectionTimer()
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

func (rf *Raft) candidateCaseReceiveAppend(args *AppendEntriesArgs) {
	appendTerm := args.Term
	if appendTerm > rf.currentTerm {
		rf.logger.Printf("Term outdated given new append, updating to %d \n", appendTerm)
		rf.currentTerm = appendTerm
		rf.candidateToFollower(args.Term)
	}

	if appendTerm == rf.currentTerm {
		rf.logger.Printf("leader position taken, stepping back \n")
		rf.candidateToFollower(args.Term)
	}

	rf.resultAppendChannel <- rf.processAppend(args)
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
