package messages

type ServerState struct {
	currentTerm   int
	votedFor      string
	logs          []string
	commitLength  int
	currentState  string
	leaderNodeId  string
	votesReceived map[string]bool
	sentLength    map[string]int
	ackedLength   map[string]int
}
