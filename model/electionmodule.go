package model

import "time"

type ElectionModule struct {
	ElectionTimeout         *time.Ticker
	ResetElectionTimer      chan struct{}
	ElectionTimeoutInterval int
}

func NewElectionModule(ticker *time.Ticker, resetChan chan struct{}, electionTimeoutInterval int) *ElectionModule {
	return &ElectionModule{
		ElectionTimeout:         ticker,
		ResetElectionTimer:      resetChan,
		ElectionTimeoutInterval: electionTimeoutInterval,
	}
}
