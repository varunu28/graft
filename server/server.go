package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"multiclient-server/db"
	"multiclient-server/logging"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	serverName  = flag.String("server-name", "", "name for the server")
	port        = flag.String("port", "", "port for running the server")
	currentRole = flag.String("current-role", "follower", "current role for server")
)

type Server struct {
	port                    string
	name                    string
	db                      *db.Database
	currentTerm             int
	votedFor                string
	Logs                    []string
	commitLength            int
	currentRole             string
	leaderNodeId            string
	votesReceived           map[string]bool
	ackedLength             map[string]int
	sentLength              map[string]int
	electionTimeout         *time.Ticker
	resetElectionTimer      chan struct{}
	electionTimeoutInterval int
}

type LogRequest struct {
	leaderId     string
	currentTerm  int
	prefixLength int
	prefixTerm   int
	commitLength int
	suffix       []string
}

type LogResponse struct {
	nodeId                string
	port                  int
	currentTerm           int
	ackLength             int
	replicationSuccessful bool
}

type VoteRequest struct {
	candidateId        string
	candidateTerm      int
	candidateLogLength int
	candidateLogTerm   int
}

type VoteResponse struct {
	nodeId      string
	currentTerm int
	voteInFavor bool
}

func parseLogRequest(message string) (*LogRequest, error) {
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
	return &LogRequest{
		leaderId:     leaderId,
		currentTerm:  currentTerm,
		prefixLength: prefixLength,
		prefixTerm:   prefixTerm,
		commitLength: commitLength,
		suffix:       suffix,
	}, nil
}

func parseLogResponse(message string) (*LogResponse, error) {
	splits := strings.Split(message, "|")
	var err error
	_, err = strconv.Atoi(splits[2])
	if err != nil {
		return nil, err
	}
	port, _ := strconv.Atoi(splits[2])
	_, err = strconv.Atoi(splits[3])
	if err != nil {
		return nil, err
	}
	currentTerm, _ := strconv.Atoi(splits[3])
	_, err = strconv.Atoi(splits[4])
	if err != nil {
		return nil, err
	}
	ackLength, _ := strconv.Atoi(splits[4])
	_, err = strconv.ParseBool(splits[5])
	if err != nil {
		return nil, err
	}
	replicationSuccessful, _ := strconv.ParseBool(splits[5])
	return &LogResponse{
		nodeId:                splits[1],
		port:                  port,
		currentTerm:           currentTerm,
		ackLength:             ackLength,
		replicationSuccessful: replicationSuccessful,
	}, nil
}

func parseVoteRequest(message string) (*VoteRequest, error) {
	splits := strings.Split(message, "|")
	var err error
	_, err = strconv.Atoi(splits[2])
	if err != nil {
		return nil, err
	}
	candidateTerm, _ := strconv.Atoi(splits[2])
	_, err = strconv.Atoi(splits[3])
	if err != nil {
		return nil, err
	}
	candidateLogLength, _ := strconv.Atoi(splits[3])
	_, err = strconv.Atoi(splits[4])
	if err != nil {
		return nil, err
	}
	candidateLogTerm, _ := strconv.Atoi(splits[4])
	return &VoteRequest{
		candidateId:        splits[1],
		candidateTerm:      candidateTerm,
		candidateLogLength: candidateLogLength,
		candidateLogTerm:   candidateLogTerm,
	}, nil
}

func parseVoteResponse(message string) (*VoteResponse, error) {
	splits := strings.Split(message, "|")
	var err error
	_, err = strconv.Atoi(splits[2])
	if err != nil {
		return nil, err
	}
	currentTerm, _ := strconv.Atoi(splits[2])
	_, err = strconv.ParseBool(splits[3])
	if err != nil {
		return nil, err
	}
	voteInFavor, _ := strconv.ParseBool(splits[3])
	return &VoteResponse{
		nodeId:      splits[1],
		currentTerm: currentTerm,
		voteInFavor: voteInFavor,
	}, nil
}

func (l *LogRequest) String() string {
	return "LogRequest" + "|" + l.leaderId + "|" + strconv.Itoa(l.currentTerm) + "|" + strconv.Itoa(l.prefixLength) + "|" + strconv.Itoa(l.prefixTerm) + "|" + strconv.Itoa(l.commitLength) + "|" + strings.Join(l.suffix, ",")
}

func (l LogResponse) String() string {
	return "LogResponse" + "|" + l.nodeId + "|" + strconv.Itoa(l.port) + "|" + strconv.Itoa(l.currentTerm) + "|" + strconv.Itoa(l.ackLength) + "|" + strconv.FormatBool(l.replicationSuccessful)
}

func (vr *VoteRequest) String() string {
	return "VoteRequest" + "|" + vr.candidateId + "|" + strconv.Itoa(vr.candidateTerm) + "|" + strconv.Itoa(vr.candidateLogLength) + "|" + strconv.Itoa(vr.candidateLogTerm)
}

func (vr *VoteResponse) String() string {
	return "VoteResponse" + "|" + vr.nodeId + "|" + strconv.Itoa(vr.currentTerm) + "|" + strconv.FormatBool(vr.voteInFavor)
}

func (s *Server) logServerPersistedState() {
	persistenceLog := s.name + "," + strconv.Itoa(s.currentTerm) + "," + s.votedFor + "," + strconv.Itoa(s.commitLength)
	err := logging.PersistServerState(persistenceLog)
	if err != nil {
		fmt.Println(err)
	}
}

func parseFlags() {
	flag.Parse()

	if *serverName == "" {
		log.Fatalf("Must provide serverName for the server")
	}

	if *port == "" {
		log.Fatalf("Must provide a port number for server to run")
	}
}

func (s *Server) sendMessageToFollowerNode(message string, port int) {
	c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Fprintf(c, message+"\n")
	go s.handleConnection(c)
}

func (s *Server) replicateLog(followerName string, followerPort int) {
	var prefixTerm = 0
	prefixLength := s.sentLength[followerName]
	if prefixLength > 0 {
		logSplit := strings.Split(s.Logs[prefixLength-1], "#")
		prefixTerm, _ = strconv.Atoi(logSplit[1])
	}
	logRequest := LogRequest{
		leaderId:     s.name,
		currentTerm:  s.currentTerm,
		prefixLength: prefixLength,
		prefixTerm:   prefixTerm,
		commitLength: s.commitLength,
		suffix:       s.Logs[s.sentLength[followerName]:],
	}
	s.sendMessageToFollowerNode(logRequest.String(), followerPort)
}

func parseLogTerm(message string) int {
	split := strings.Split(message, "#")
	pTerm, _ := strconv.Atoi(split[1])
	return pTerm
}

func (s *Server) AddLog(log string) []string {
	s.Logs = append(s.Logs, log)
	return s.Logs
}

func (s *Server) appendEntries(prefixLength int, commitLength int, suffix []string) {
	if len(suffix) > 0 && len(s.Logs) > prefixLength {
		var index int
		if len(s.Logs) > (prefixLength + len(suffix)) {
			index = prefixLength + len(suffix) - 1
		} else {
			index = len(s.Logs) - 1
		}
		if parseLogTerm(s.Logs[index]) != parseLogTerm(suffix[index-prefixLength]) {
			s.Logs = s.Logs[:prefixLength]
		}
	}
	if prefixLength+len(suffix) > len(s.Logs) {
		for i := (len(s.Logs) - prefixLength); i < len(suffix); i++ {
			s.AddLog(suffix[i])
			err := s.db.LogCommand(suffix[i], s.name)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	if commitLength > s.commitLength {
		for i := s.commitLength; i < commitLength; i++ {
			s.db.PerformDbOperations(strings.Split(s.Logs[i], "#")[0])
		}
		s.commitLength = commitLength
		s.logServerPersistedState()
	}
}

func (s *Server) handleLogResponse(message string) string {
	lr, _ := parseLogResponse(message)
	if lr.currentTerm > s.currentTerm {
		s.currentTerm = lr.currentTerm
		s.currentRole = "follower"
		s.votedFor = ""
		go s.electionTimer()
	}
	if lr.currentTerm == s.currentTerm && s.currentRole == "leader" {
		if lr.replicationSuccessful && lr.ackLength >= s.ackedLength[lr.nodeId] {
			s.sentLength[lr.nodeId] = lr.ackLength
			s.ackedLength[lr.nodeId] = lr.ackLength
			s.commitLogEntries()
		} else {
			s.sentLength[lr.nodeId] = s.sentLength[lr.nodeId] - 1
			s.replicateLog(lr.nodeId, lr.port)
		}
	}
	return "replication successful"
}

func (s *Server) handleLogRequest(message string) string {
	s.resetElectionTimer <- struct{}{}
	logRequest, _ := parseLogRequest(message)
	if logRequest.currentTerm > s.currentTerm {
		s.currentTerm = logRequest.currentTerm
		s.votedFor = ""
	}
	if logRequest.currentTerm == s.currentTerm {
		if s.currentRole == "leader" {
			go s.electionTimer()
		}
		s.currentRole = "follower"
		s.leaderNodeId = logRequest.leaderId
	}
	var logOk bool = false
	if len(s.Logs) >= logRequest.prefixLength &&
		(logRequest.prefixLength == 0 ||
			parseLogTerm(s.Logs[logRequest.prefixLength-1]) == logRequest.prefixTerm) {
		logOk = true
	}
	port, _ := strconv.Atoi(s.port)
	if s.currentTerm == logRequest.currentTerm && logOk {
		s.appendEntries(logRequest.prefixLength, logRequest.commitLength, logRequest.suffix)
		ack := logRequest.prefixLength + len(logRequest.suffix)
		return LogResponse{
			nodeId:                s.name,
			port:                  port,
			currentTerm:           s.currentTerm,
			ackLength:             ack,
			replicationSuccessful: true,
		}.String()
	} else {
		return LogResponse{
			nodeId:                s.name,
			port:                  port,
			currentTerm:           s.currentTerm,
			ackLength:             0,
			replicationSuccessful: false,
		}.String()
	}
}

func (s *Server) commitLogEntries() {
	allNodes, _ := logging.ListRegisteredServer()
	for i := s.commitLength; i < len(s.Logs); i++ {
		var acks = 0
		for node := range allNodes {
			if node != s.name && s.ackedLength[node] > s.commitLength {
				acks = acks + 1
			}
		}
		if acks >= (len(allNodes)+1)/2 {
			log := s.Logs[i]
			command := strings.Split(log, "#")[0]
			s.db.PerformDbOperations(command)
			s.commitLength = s.commitLength + 1
			s.logServerPersistedState()
		} else {
			break
		}
	}
}

func (s *Server) handleVoteRequest(message string) string {
	voteRequest, _ := parseVoteRequest(message)
	if voteRequest.candidateTerm > s.currentTerm {
		s.currentTerm = voteRequest.candidateTerm
		s.currentRole = "follower"
		s.votedFor = ""
	}
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}
	var logOk = false
	if voteRequest.candidateLogTerm > lastTerm ||
		(voteRequest.candidateLogTerm == lastTerm && voteRequest.candidateLogLength >= len(s.Logs)) {
		logOk = true
	}
	var vr VoteResponse
	if voteRequest.candidateTerm == s.currentTerm && logOk && (s.votedFor == "" || s.votedFor == voteRequest.candidateId) {
		s.votedFor = voteRequest.candidateId
		vr = VoteResponse{
			nodeId:      s.name,
			currentTerm: s.currentTerm,
			voteInFavor: true,
		}
	} else {
		vr = VoteResponse{
			nodeId:      s.name,
			currentTerm: s.currentTerm,
			voteInFavor: false,
		}
	}
	return vr.String()
}

func (s *Server) checkForElectionResult() {
	var totalVotes = 0
	for server := range s.votesReceived {
		if s.votesReceived[server] {
			totalVotes += 1
		}
	}
	allNodes, _ := logging.ListRegisteredServer()
	if totalVotes >= (len(allNodes)+1)/2 {
		s.currentRole = "leader"
		s.leaderNodeId = s.name
		s.electionTimeout.Stop()
		s.syncUp()
	}
}

func (s *Server) handleVoteResponse(message string) {
	voteResponse, _ := parseVoteResponse(message)
	if voteResponse.currentTerm > s.currentTerm {
		if s.currentRole != "leader" {
			s.resetElectionTimer <- struct{}{}
		}
		s.currentTerm = voteResponse.currentTerm
		s.currentRole = "follower"
		s.votedFor = ""
	}
	if s.currentRole == "candidate" && voteResponse.currentTerm == s.currentTerm && voteResponse.voteInFavor {
		s.votesReceived[voteResponse.nodeId] = true
		s.checkForElectionResult()
	}
}

func (s *Server) handleConnection(c net.Conn) {
	defer c.Close()
	for {
		data, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			continue
		}
		message := strings.TrimSpace(string(data))
		if message == "invalid command" || message == "replication successful" {
			continue
		}
		fmt.Println(">", string(message))
		var response string = ""
		if strings.HasPrefix(message, "LogRequest") {
			response = s.handleLogRequest(message)
		}
		if strings.HasPrefix(message, "LogResponse") {
			response = s.handleLogResponse(message)
		}
		if strings.HasPrefix(message, "VoteRequest") {
			response = s.handleVoteRequest(message)
		}
		if strings.HasPrefix(message, "VoteResponse") {
			s.handleVoteResponse(message)
		}
		if s.currentRole == "leader" && response == "" {
			var err = s.db.ValidateCommand(message)
			if err != nil {
				response = err.Error()
			}
			if strings.HasPrefix(message, "GET") {
				response = s.db.PerformDbOperations(message)
			}
			if response == "" {
				logMessage := message + "#" + strconv.Itoa(s.currentTerm)
				s.ackedLength[s.name] = len(s.Logs)
				s.Logs = append(s.Logs, logMessage)
				currLogIdx := len(s.Logs) - 1
				err = s.db.LogCommand(logMessage, s.name)
				if err != nil {
					response = "error while logging command"
				}
				allServers, _ := logging.ListRegisteredServer()
				for sname, sport := range allServers {
					if sname != s.name {
						s.replicateLog(sname, sport)
					}
				}
				for s.commitLength < currLogIdx {
					fmt.Println("Waiting for consensus")
				}
				response = "operation sucessful"
			}
		}
		if response != "" {
			c.Write([]byte(response + "\n"))
		}
	}
}

func (s *Server) startElection() {
	s.currentTerm = s.currentTerm + 1
	s.currentRole = "candidate"
	s.votedFor = s.name
	s.votesReceived = map[string]bool{}
	s.votesReceived[s.name] = true
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}
	voteRequest := VoteRequest{
		candidateId:        s.name,
		candidateTerm:      s.currentTerm,
		candidateLogLength: len(s.Logs),
		candidateLogTerm:   lastTerm,
	}
	allNodes, _ := logging.ListRegisteredServer()
	for node, port := range allNodes {
		if node != s.name {
			s.sendMessageToFollowerNode(voteRequest.String(), port)
		}
	}
}

func (s *Server) electionTimer() {
	for {
		select {
		case <-s.electionTimeout.C:
			fmt.Println("Timed out")
			if s.currentRole != "candidate" {
				go s.startElection()
			}
		case <-s.resetElectionTimer:
			fmt.Println("Resetting election timer")
			s.electionTimeout.Reset(time.Duration(s.electionTimeoutInterval) * time.Second)
		}
	}
}

func (s *Server) syncUp() {
	ticker := time.NewTicker(3 * time.Second)
	for t := range ticker.C {
		fmt.Println("sending heartbeat at: ", t)
		allServers, _ := logging.ListRegisteredServer()
		for sname, sport := range allServers {
			if sname != s.name {
				s.replicateLog(sname, sport)
			}
		}
	}
}

func main() {
	parseFlags()
	l, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	db, err := db.NewDatabase()
	if err != nil {
		fmt.Println("Error while creating db")
		return
	}

	err = logging.RegisterServer(*serverName, *port)
	if err != nil {
		fmt.Println(err)
		return
	}
	rand.Seed(time.Now().UnixNano())
	minTimeout := 4
	maxTimeout := 10

	s := Server{
		port:                    *port,
		name:                    *serverName,
		db:                      db,
		currentTerm:             0,
		votedFor:                "",
		Logs:                    make([]string, 0),
		commitLength:            0,
		currentRole:             *currentRole,
		leaderNodeId:            "",
		votesReceived:           map[string]bool{},
		ackedLength:             map[string]int{},
		sentLength:              map[string]int{},
		electionTimeout:         time.NewTicker(5 * time.Second),
		resetElectionTimer:      make(chan struct{}),
		electionTimeoutInterval: rand.Intn(maxTimeout-minTimeout+1) + minTimeout,
	}
	s.logServerPersistedState()
	if s.currentRole == "leader" {
		go s.syncUp()
	} else if s.currentRole == "follower" {
		go s.electionTimer()
	}
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go s.handleConnection(c)
	}
}
