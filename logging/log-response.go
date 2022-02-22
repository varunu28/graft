package logging

type LogResponse struct {
	nodeId                string
	currentTerm           int
	ackLength             int
	replicationSuccessful bool
}
