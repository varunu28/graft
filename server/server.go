package main

import (
	"bufio"
	"flag"
	"fmt"
	"graft/db"
	"graft/logging"
	"graft/model"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	serverName = flag.String("server-name", "", "name for the server")
	port       = flag.String("port", "", "port for running the server")
)

const (
	BroadcastPeriod    = 3000
	ElectionMinTimeout = 3001
	ElectionMaxTimeout = 10000
)

type Server struct {
	port           string
	name           string
	db             *db.Database
	currentTerm    int
	votedFor       string
	Logs           []string
	commitLength   int
	currentRole    string
	leaderNodeId   string
	peerdata       *model.PeerData
	electionModule *model.ElectionModule
}

func (s *Server) logServerPersistedState() {
	persistenceLog := s.name + "," + strconv.Itoa(s.currentTerm) + "," + s.votedFor + "," + strconv.Itoa(s.commitLength)
	err := logging.PersistServerState(persistenceLog)
	if err != nil {
		fmt.Println(err)
	}
}

func (s *Server) sendMessageToFollowerNode(message string, port int) {
	c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		s.peerdata.SuspectedNodes[port] = true
		return
	}
	_, ok := s.peerdata.SuspectedNodes[port]
	if ok {
		delete(s.peerdata.SuspectedNodes, port)
	}
	fmt.Fprintf(c, message+"\n")
	go s.handleConnection(c)
}

func (s *Server) replicateLog(followerName string, followerPort int) {
	if followerName == s.name {
		go s.commitLogEntries()
		return
	}
	var prefixTerm = 0
	prefixLength := s.peerdata.SentLength[followerName]
	if prefixLength > 0 {
		logSplit := strings.Split(s.Logs[prefixLength-1], "#")
		prefixTerm, _ = strconv.Atoi(logSplit[1])
	}
	logRequest := model.NewLogRequest(s.name, s.currentTerm, prefixLength, prefixTerm, s.commitLength, s.Logs[s.peerdata.SentLength[followerName]:])
	s.sendMessageToFollowerNode(logRequest.String(), followerPort)
}

func (s *Server) addLogs(log string) []string {
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
			s.addLogs(suffix[i])
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
	lr, _ := model.ParseLogResponse(message)
	if lr.CurrentTerm > s.currentTerm {
		s.currentTerm = lr.CurrentTerm
		s.currentRole = "follower"
		s.votedFor = ""
		go s.electionTimer()
	}
	if lr.CurrentTerm == s.currentTerm && s.currentRole == "leader" {
		if lr.ReplicationSuccessful && lr.AckLength >= s.peerdata.AckedLength[lr.NodeId] {
			s.peerdata.SentLength[lr.NodeId] = lr.AckLength
			s.peerdata.AckedLength[lr.NodeId] = lr.AckLength
			s.commitLogEntries()
		} else {
			s.peerdata.SentLength[lr.NodeId] = s.peerdata.SentLength[lr.NodeId] - 1
			s.replicateLog(lr.NodeId, lr.Port)
		}
	}
	return "replication successful"
}

func (s *Server) handleLogRequest(message string) string {
	s.electionModule.ResetElectionTimer <- struct{}{}
	logRequest, _ := model.ParseLogRequest(message)
	if logRequest.CurrentTerm > s.currentTerm {
		s.currentTerm = logRequest.CurrentTerm
		s.votedFor = ""
	}
	if logRequest.CurrentTerm == s.currentTerm {
		if s.currentRole == "leader" {
			go s.electionTimer()
		}
		s.currentRole = "follower"
		s.leaderNodeId = logRequest.LeaderId
	}
	var logOk bool = false
	if len(s.Logs) >= logRequest.PrefixLength &&
		(logRequest.PrefixLength == 0 ||
			parseLogTerm(s.Logs[logRequest.PrefixLength-1]) == logRequest.PrefixTerm) {
		logOk = true
	}
	port, _ := strconv.Atoi(s.port)
	if s.currentTerm == logRequest.CurrentTerm && logOk {
		s.appendEntries(logRequest.PrefixLength, logRequest.CommitLength, logRequest.Suffix)
		ack := logRequest.PrefixLength + len(logRequest.Suffix)
		return model.NewLogResponse(s.name, port, s.currentTerm, ack, true).String()
	} else {
		return model.NewLogResponse(s.name, port, s.currentTerm, 0, false).String()
	}
}

func (s *Server) commitLogEntries() {
	allNodes, _ := logging.ListRegisteredServer()
	eligbleNodeCount := len(allNodes) - len(s.peerdata.SuspectedNodes)
	for i := s.commitLength; i < len(s.Logs); i++ {
		var acks = 0
		for node := range allNodes {
			if s.peerdata.AckedLength[node] > s.commitLength {
				acks = acks + 1
			}
		}
		if acks >= (eligbleNodeCount+1)/2 || eligbleNodeCount == 1 {
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
	voteRequest, _ := model.ParseVoteRequest(message)
	if voteRequest.CandidateTerm > s.currentTerm {
		s.currentTerm = voteRequest.CandidateTerm
		s.currentRole = "follower"
		s.votedFor = ""
		s.electionModule.ResetElectionTimer <- struct{}{}
	}
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}
	var logOk = false
	if voteRequest.CandidateLogTerm > lastTerm ||
		(voteRequest.CandidateLogTerm == lastTerm && voteRequest.CandidateLogLength >= len(s.Logs)) {
		logOk = true
	}
	if voteRequest.CandidateTerm == s.currentTerm && logOk && (s.votedFor == "" || s.votedFor == voteRequest.CandidateId) {
		s.votedFor = voteRequest.CandidateId
		s.logServerPersistedState()
		return model.NewVoteResponse(
			s.name,
			s.currentTerm,
			true,
		).String()
	} else {
		return model.NewVoteResponse(s.name, s.currentTerm, false).String()
	}
}

func (s *Server) handleVoteResponse(message string) {
	voteResponse, _ := model.ParseVoteResponse(message)
	if voteResponse.CurrentTerm > s.currentTerm {
		if s.currentRole != "leader" {
			s.electionModule.ResetElectionTimer <- struct{}{}
		}
		s.currentTerm = voteResponse.CurrentTerm
		s.currentRole = "follower"
		s.votedFor = ""
	}
	if s.currentRole == "candidate" && voteResponse.CurrentTerm == s.currentTerm && voteResponse.VoteInFavor {
		s.peerdata.VotesReceived[voteResponse.NodeId] = true
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
				s.peerdata.AckedLength[s.name] = len(s.Logs)
				s.Logs = append(s.Logs, logMessage)
				currLogIdx := len(s.Logs) - 1
				err = s.db.LogCommand(logMessage, s.name)
				if err != nil {
					response = "error while logging command"
				}
				allServers, _ := logging.ListRegisteredServer()
				for sname, sport := range allServers {
					s.replicateLog(sname, sport)
				}
				for s.commitLength <= currLogIdx {
					fmt.Println("Waiting for consensus: ")
				}
				response = "operation sucessful"
			}
		}
		if response != "" {
			c.Write([]byte(response + "\n"))
		}
	}
}

func (s *Server) checkForElectionResult() {
	if s.currentRole == "leader" {
		return
	}
	var totalVotes = 0
	for server := range s.peerdata.VotesReceived {
		if s.peerdata.VotesReceived[server] {
			totalVotes += 1
		}
	}
	allNodes, _ := logging.ListRegisteredServer()
	if totalVotes >= (len(allNodes)+1)/2 {
		fmt.Println("I won the election. New leader: ", s.name, " Votes received: ", totalVotes)
		s.currentRole = "leader"
		s.leaderNodeId = s.name
		s.peerdata.VotesReceived = make(map[string]bool)
		s.electionModule.ElectionTimeout.Stop()
		s.syncUp()
	}
}

func (s *Server) startElection() {
	s.currentTerm = s.currentTerm + 1
	s.currentRole = "candidate"
	s.votedFor = s.name
	s.peerdata.VotesReceived = map[string]bool{}
	s.peerdata.VotesReceived[s.name] = true
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}
	voteRequest := model.NewVoteRequest(s.name, s.currentTerm, len(s.Logs), lastTerm)
	allNodes, _ := logging.ListRegisteredServer()
	for node, port := range allNodes {
		if node != s.name {
			s.sendMessageToFollowerNode(voteRequest.String(), port)
		}
	}
	s.checkForElectionResult()
}

func (s *Server) electionTimer() {
	for {
		select {
		case <-s.electionModule.ElectionTimeout.C:
			fmt.Println("Timed out")
			if s.currentRole == "follower" {
				go s.startElection()
			} else {
				s.currentRole = "follower"
				s.electionModule.ResetElectionTimer <- struct{}{}
			}
		case <-s.electionModule.ResetElectionTimer:
			fmt.Println("Resetting election timer")
			s.electionModule.ElectionTimeout.Reset(time.Duration(s.electionModule.ElectionTimeoutInterval) * time.Millisecond)
		}
	}
}

func (s *Server) syncUp() {
	ticker := time.NewTicker(BroadcastPeriod * time.Millisecond)
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

	rand.Seed(time.Now().UnixNano())
	electionTimeoutInterval := rand.Intn(int(ElectionMaxTimeout)-int(ElectionMinTimeout)) + int(ElectionMinTimeout)
	electionModule := model.NewElectionModule(electionTimeoutInterval)

	err = logging.RegisterServer(*serverName, *port)
	if err != nil {
		fmt.Println(err)
		return
	}

	s := Server{
		port:           *port,
		name:           *serverName,
		db:             db,
		currentTerm:    0,
		votedFor:       "",
		Logs:           make([]string, 0),
		commitLength:   0,
		currentRole:    "follower",
		leaderNodeId:   "",
		peerdata:       model.NewPeerData(),
		electionModule: electionModule,
	}
	s.logServerPersistedState()
	go s.electionTimer()
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go s.handleConnection(c)
	}
}

func parseLogTerm(message string) int {
	split := strings.Split(message, "#")
	pTerm, _ := strconv.Atoi(split[1])
	return pTerm
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
