package server

type VoteRequest struct {
	candidateId        string
	candidateTerm      int
	candidateLogLength int
	candidateLogTerm   int
}
