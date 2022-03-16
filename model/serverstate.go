package model

type ServerState struct {
	Name         string
	CurrentTerm  int
	VotedFor     string
	CommitLength int
}

func NewServerState(name string) *ServerState {
	return &ServerState{
		Name:         name,
		CurrentTerm:  0,
		VotedFor:     "",
		CommitLength: 0,
	}
}
