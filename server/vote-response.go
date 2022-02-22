package server

type VoteResponse struct {
	nodeId      string
	currentTerm int
	voteInFavor bool
}
