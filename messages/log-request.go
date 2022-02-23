package messages

type LogRequest struct {
	leaderId     string
	currentTerm  int
	prefixLength int
	prefixTerm   int
	commitLength int
	suffix       []string
}
