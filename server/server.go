package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"multiclient-server/db"
	"multiclient-server/logging"
	"net"
	"strconv"
	"strings"
)

var (
	serverName  = flag.String("server-name", "", "name for the server")
	port        = flag.String("port", "", "port for running the server")
	currentRole = flag.String("current-role", "follower", "current role for server")
)

type Server struct {
	port          string
	name          string
	db            *db.Database
	currentTerm   int
	votedFor      string
	Logs          []string
	commitLength  int
	currentRole   string
	leaderNodeId  string
	votesReceived map[string]bool
	ackedLength   map[string]int
	sentLength    map[string]int
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

func NewLogResponse(nodeId string, port int, currentTerm int, ackLength int, replicationSuccessful bool) *LogResponse {
	return &LogResponse{
		nodeId:                nodeId,
		port:                  port,
		currentTerm:           currentTerm,
		ackLength:             ackLength,
		replicationSuccessful: replicationSuccessful,
	}
}

func (l LogResponse) String() string {
	return "LogResponse" + "|" + l.nodeId + "|" + strconv.Itoa(l.port) + "|" + strconv.Itoa(l.currentTerm) + "|" + strconv.Itoa(l.ackLength) + "|" + strconv.FormatBool(l.replicationSuccessful)
}

func NewLogResponseFromString(message string) (*LogResponse, error) {
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
	return NewLogResponse(splits[1], port, currentTerm, ackLength, replicationSuccessful), nil
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

func (l *LogRequest) String() string {
	return "LogRequest" + "|" + l.leaderId + "|" + strconv.Itoa(l.currentTerm) + "|" + strconv.Itoa(l.prefixLength) + "|" + strconv.Itoa(l.prefixTerm) + "|" + strconv.Itoa(l.commitLength) + "|" + strings.Join(l.suffix, ",")
}

func NewLogRequestFromString(message string) (*LogRequest, error) {
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
	suffix := strings.Split(splits[6], ",")
	return NewLogRequest(leaderId, currentTerm, prefixLength, prefixTerm, commitLength, suffix), nil
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
	logRequest := NewLogRequest(
		s.name,
		s.currentTerm,
		prefixLength,
		prefixTerm,
		s.commitLength,
		s.Logs[s.sentLength[followerName]:],
	)
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

func (s *Server) handleLogRequest(message string) string {
	logRequest, _ := NewLogRequestFromString(message)
	if logRequest.currentTerm > s.currentTerm {
		s.currentTerm = logRequest.currentTerm
		s.votedFor = ""
		// TODO: Cancel election time
	}
	if logRequest.currentTerm == s.currentTerm {
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
		return NewLogResponse(
			s.name, port, s.currentTerm, ack, true,
		).String()
	} else {
		return NewLogResponse(
			s.name, port, s.currentTerm, 0, false,
		).String()
	}
}

func (s *Server) commitLogEntries() {
	all_nodes, _ := logging.ListRegisteredServer()
	for i := s.commitLength; i < len(s.Logs); i++ {
		var acks = 0
		for node := range all_nodes {
			if node != s.name && s.ackedLength[node] > s.commitLength {
				acks = acks + 1
			}
		}
		if acks >= (len(all_nodes)+1)/2 {
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

func (s *Server) handleLogResponse(message string) string {
	lr, _ := NewLogResponseFromString(message)
	if lr.currentTerm > s.currentTerm {
		s.currentTerm = lr.currentTerm
		s.currentRole = "follower"
		s.votedFor = ""
		// TODO: Cancel election timer
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

func (s *Server) handleConnection(c net.Conn) {
	defer c.Close()
	for {
		data, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
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

	s := Server{
		port:          *port,
		name:          *serverName,
		db:            db,
		currentTerm:   0,
		votedFor:      "",
		Logs:          make([]string, 0),
		commitLength:  0,
		currentRole:   *currentRole,
		leaderNodeId:  "",
		votesReceived: map[string]bool{},
		ackedLength:   map[string]int{},
		sentLength:    map[string]int{},
	}
	s.logServerPersistedState()
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go s.handleConnection(c)
	}
}
