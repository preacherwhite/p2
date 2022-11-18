package raft

import "time"

func (rf *Raft) followerRoutine() {
	rf.logger.Printf("follower routine, term %d\n", rf.currentTerm)
	select {
	case <-rf.getCallChannel:
		rf.followerCaseGetState()
	case <-rf.electionTimer.C:
		rf.followerCaseElection()
	case args := <-rf.receivedRequestsChannel:
		rf.followerCaseReceiveRequest(args)
	case args := <-rf.receivedAppendChannel:
		rf.followerCaseReceiveAppend(args)
	case args := <-rf.appendFeedbackChannel:
		rf.followerCaseFeedbackAppend(args)
	case args := <-rf.requestFeedbackChannel:
		rf.followerCaseFeedbackRequest(args)
	}
	rf.logger.Printf("follower routine end\n\n")
}

func (rf *Raft) followerCaseGetState() {
	rf.getResultChannel <- &GetInfo{
		Me:       rf.me,
		Term:     rf.currentTerm,
		IsLeader: false,
	}
}

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
				lastLogIndex: len(rf.log) - 1,
				lastLogTerm:  rf.log[len(rf.log)-1].term,
			}
			go rf.voteRequestRoutine(serverId, newRequest)
		}
	}
	rf.electionTimer.Reset(rf.electionTimeoutWindow * time.Millisecond)
	rf.state = "candidate"
}

func (rf *Raft) followerCaseReceiveRequest(args *RequestVoteArgs) {
	rf.logger.Println("processing request")
	requestCandidate := args.CandidateId
	requestTerm := args.Term
	reply := &RequestVoteReply{}
	if requestTerm > rf.currentTerm {
		rf.logger.Printf("term outdated given new request, updating to %d \n", requestTerm)
		rf.currentTerm = requestTerm
		rf.votedFor = -1
		rf.resetElectionTimer()
	}
	if requestTerm == rf.currentTerm && rf.votedFor == -1 {
		rf.logger.Println("voted")
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

func (rf *Raft) followerCaseReceiveAppend(args *AppendEntriesArgs) {
	appendTerm := args.Term
	reply := &AppendEntriesReply{}
	if appendTerm >= rf.currentTerm {
		rf.logger.Printf("term outdated given new append, updating to %d \n", appendTerm)
		rf.currentTerm = appendTerm
		rf.resetElectionTimer()
		reply.Success = false
	} else if appendTerm < rf.currentTerm {
		rf.logger.Println("discarding append")
		reply.Success = false
	}
	reply.Term = rf.currentTerm
	reply.serverId = rf.me
	rf.resultAppendChannel <- reply
}

func (rf *Raft) followerCaseFeedbackAppend(args *AppendEntriesReply) {
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}
}

func (rf *Raft) followerCaseFeedbackRequest(args *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}
}

func (rf *Raft) voteRequestRoutine(serverId int, newRequest *RequestVoteArgs) {
	newReply := &RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, newRequest, newReply)
	if ok {
		rf.logger.Printf("vote from %d, term %d, grated %t\n", serverId, newRequest.Term, newReply.VoteGranted)
		rf.requestFeedbackChannel <- newReply
	}

}
