package messages

import (
	"strconv"
	"strings"
)

type LogRequest struct {
	leaderId     string
	currentTerm  int
	prefixLength int
	prefixTerm   int
	commitLength int
	suffix       []string
}

func NewLogRequest(leaderId string, currentTerm int, prefixLength int, prefixTerm int, commitLength int, suffix []string) *LogRequest {
	return &LogRequest{
		leaderId:     leaderId,
		currentTerm:  currentTerm,
		prefixLength: prefixLength,
		prefixTerm:   prefixTerm,
		commitLength: commitLength,
		suffix:       suffix,
	}
}

func (l LogRequest) String() string {
	return l.leaderId + "|" + strconv.Itoa(l.currentTerm) + "|" + strconv.Itoa(l.prefixLength) + "|" + strconv.Itoa(l.prefixTerm) + "|" + strconv.Itoa(l.commitLength) + "|" + strings.Join(l.suffix, ",")
}
