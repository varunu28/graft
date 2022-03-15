package model

import "time"

type ElectionModule struct {
	ElectionTimeout         *time.Ticker
	ResetElectionTimer      chan struct{}
	ElectionTimeoutInterval int
}

func NewElectionModule(electionTimeoutInterval int) *ElectionModule {
	return &ElectionModule{
		ElectionTimeout:         time.NewTicker(time.Duration(electionTimeoutInterval) * time.Millisecond),
		ResetElectionTimer:      make(chan struct{}),
		ElectionTimeoutInterval: electionTimeoutInterval,
	}
}
