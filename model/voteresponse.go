package model

import (
	"strconv"
	"strings"
)

type VoteResponse struct {
	NodeId      string
	CurrentTerm int
	VoteInFavor bool
}

func (vr *VoteResponse) String() string {
	return "VoteResponse" + "|" + vr.NodeId + "|" + strconv.Itoa(vr.CurrentTerm) + "|" + strconv.FormatBool(vr.VoteInFavor)
}

func ParseVoteResponse(message string) (*VoteResponse, error) {
	splits := strings.Split(message, "|")
	var err error
	_, err = strconv.Atoi(splits[2])
	if err != nil {
		return nil, err
	}
	currentTerm, _ := strconv.Atoi(splits[2])
	_, err = strconv.ParseBool(splits[3])
	if err != nil {
		return nil, err
	}
	voteInFavor, _ := strconv.ParseBool(splits[3])
	return NewVoteResponse(splits[1], currentTerm, voteInFavor), nil
}

func NewVoteResponse(nodeId string, currentTerm int, voteInFavor bool) *VoteResponse {
	return &VoteResponse{
		NodeId:      nodeId,
		CurrentTerm: currentTerm,
		VoteInFavor: voteInFavor,
	}
}
