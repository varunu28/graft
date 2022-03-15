package model

import (
	"strconv"
	"strings"
)

type LogRequest struct {
	LeaderId     string
	CurrentTerm  int
	PrefixLength int
	PrefixTerm   int
	CommitLength int
	Suffix       []string
}

func (l *LogRequest) String() string {
	return "LogRequest" + "|" + l.LeaderId + "|" + strconv.Itoa(l.CurrentTerm) + "|" + strconv.Itoa(l.PrefixLength) + "|" + strconv.Itoa(l.PrefixTerm) + "|" + strconv.Itoa(l.CommitLength) + "|" + strings.Join(l.Suffix, ",")
}

func ParseLogRequest(message string) (*LogRequest, error) {
	splits := strings.Split(message, "|")
	leaderId := splits[1]
	var err error
	_, err = strconv.Atoi(splits[2])
	if err != nil {
		return nil, err
	}
	currentTerm, _ := strconv.Atoi(splits[2])
	_, err = strconv.Atoi(splits[2])
	if err != nil {
		return nil, err
	}
	prefixLength, _ := strconv.Atoi(splits[3])
	_, err = strconv.Atoi(splits[4])
	if err != nil {
		return nil, err
	}
	prefixTerm, _ := strconv.Atoi(splits[4])
	_, err = strconv.Atoi(splits[5])
	if err != nil {
		return nil, err
	}
	commitLength, _ := strconv.Atoi(splits[5])
	var suffix = strings.Split(splits[6], ",")
	if len(suffix) > 0 && len(suffix[0]) == 0 {
		suffix = make([]string, 0)
	}
	return NewLogRequest(leaderId, currentTerm, prefixLength, prefixTerm, commitLength, suffix), nil
}

func NewLogRequest(leaderId string, currentTerm int, prefixLength int, prefixTerm int, commitLength int, suffix []string) *LogRequest {
	return &LogRequest{
		LeaderId:     leaderId,
		CurrentTerm:  currentTerm,
		PrefixLength: prefixLength,
		PrefixTerm:   prefixTerm,
		CommitLength: commitLength,
		Suffix:       suffix,
	}
}
