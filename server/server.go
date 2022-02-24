package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"multiclient-server/db"
	"multiclient-server/logging"
	"multiclient-server/messages"
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
	logs          []string
	commitLength  int
	currentRole   string
	leaderNodeId  string
	votesReceived map[string]bool
	ackedLength   map[string]int
	sentLength    map[string]int
}

func (s Server) logServerPersistedState() {
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

func (s Server) sendMessageToFollowerNode(message string, port int) {
	c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Fprintf(c, message+"\n")
	go s.handleConnection(c)
}

func (s Server) replicateLog(followerName string, followerPort int) {
	var prefixTerm = 0
	prefixLength := s.sentLength[followerName]
	if prefixLength > 0 {
		logSplit := strings.Split(s.logs[prefixLength-1], "#")
		prefixTerm, _ = strconv.Atoi(logSplit[1])
	}
	logRequest := messages.NewLogRequest(
		s.name,
		s.currentTerm,
		prefixLength,
		prefixTerm,
		s.commitLength,
		s.logs[s.sentLength[followerName]:],
	)
	s.sendMessageToFollowerNode(logRequest.String(), followerPort)
}

func (s Server) handleConnection(c net.Conn) {
	defer c.Close()
	for {
		data, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		message := strings.TrimSpace(string(data))
		if message == "invalid command" {
			continue
		}
		fmt.Println(">", string(message))
		var response string = ""
		if s.currentRole == "leader" {
			var err = s.db.ValidateCommand(message)
			if err != nil {
				response = err.Error()
			}
			if response == "" {
				logMessage := message + "#" + strconv.Itoa(s.currentTerm)
				s.ackedLength[s.name] = len(s.logs)
				s.logs = append(s.logs, logMessage)
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
			}
			if response == "" {
				response = s.db.PerformDbOperations(message)
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
		logs:          make([]string, 0),
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
