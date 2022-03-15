package model

import (
	"strconv"
	"strings"
)

type VoteRequest struct {
	CandidateId        string
	CandidateTerm      int
	CandidateLogLength int
	CandidateLogTerm   int
}

func (vr *VoteRequest) String() string {
	return "VoteRequest" + "|" + vr.CandidateId + "|" + strconv.Itoa(vr.CandidateTerm) + "|" + strconv.Itoa(vr.CandidateLogLength) + "|" + strconv.Itoa(vr.CandidateLogTerm)
}

func ParseVoteRequest(message string) (*VoteRequest, error) {
	splits := strings.Split(message, "|")
	var err error
	_, err = strconv.Atoi(splits[2])
	if err != nil {
		return nil, err
	}
	candidateTerm, _ := strconv.Atoi(splits[2])
	_, err = strconv.Atoi(splits[3])
	if err != nil {
		return nil, err
	}
	candidateLogLength, _ := strconv.Atoi(splits[3])
	_, err = strconv.Atoi(splits[4])
	if err != nil {
		return nil, err
	}
	candidateLogTerm, _ := strconv.Atoi(splits[4])
	return NewVoteRequest(splits[1], candidateTerm, candidateLogLength, candidateLogTerm), nil
}

func NewVoteRequest(candidateId string, candidateTerm int, candidateLogLength int, candidateLogTerm int) *VoteRequest {
	return &VoteRequest{
		CandidateId:        candidateId,
		CandidateTerm:      candidateTerm,
		CandidateLogLength: candidateLogLength,
		CandidateLogTerm:   candidateLogTerm,
	}
}
