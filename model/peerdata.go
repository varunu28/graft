package model

type PeerData struct {
	VotesReceived  map[string]bool
	AckedLength    map[string]int
	SentLength     map[string]int
	SuspectedNodes map[int]bool
}

func NewPeerData() *PeerData {
	return &PeerData{
		VotesReceived:  make(map[string]bool),
		AckedLength:    make(map[string]int),
		SentLength:     make(map[string]int),
		SuspectedNodes: make(map[int]bool),
	}
}
