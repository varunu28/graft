package messages

type VoteResponse struct {
	nodeId      string
	currentTerm int
	voteInFavor bool
}
