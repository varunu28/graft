package model

import (
	"graft/logging"
	"strconv"
	"strings"
)

type ServerState struct {
	Name         string
	CurrentTerm  int
	VotedFor     string
	CommitLength int
}

func GetExistingServerStateOrCreateNew(name string) *ServerState {
	log, err := logging.GetLatestServerStateIfPresent(name)
	if err != nil {
		return newServerState(name)
	}
	return parseServerStateLog(log)
}

func newServerState(name string) *ServerState {
	return &ServerState{
		Name:         name,
		CurrentTerm:  0,
		VotedFor:     "",
		CommitLength: 0,
	}
}

func parseServerStateLog(log string) *ServerState {
	splits := strings.Split(log, ",")
	name := splits[0]
	currentTerm, _ := strconv.Atoi(splits[1])
	votedFor := splits[2]
	commitLength, _ := strconv.Atoi(splits[3])
	return &ServerState{
		Name:         name,
		CurrentTerm:  currentTerm,
		VotedFor:     votedFor,
		CommitLength: commitLength,
	}
}
