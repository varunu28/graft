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
	serverName = flag.String("server-name", "", "name for the server")
	port       = flag.String("port", "", "port for running the server")
)

type Server struct {
	port string
	name string
	db   *db.Database
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

func (s Server) broadcast(message string) {
	if s.port == "8000" {
		serverToPortMapping, err := logging.ListRegisteredServer()
		if err != nil {
			fmt.Println(err)
			return
		}
		for _, port := range serverToPortMapping {
			if port != 8000 {
				c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Fprintf(c, message+"\n")
				go s.handleConnection(c)
			}
		}
	}
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
		if message == "Invalid command" {
			continue
		}
		fmt.Println(">", string(message))
		go s.broadcast(message)
		var response string = ""
		if s.port == "8000" {
			var err = s.db.ValidateCommand(message)
			if err != nil {
				response = err.Error()
			}
			if response == "" {
				err = s.db.LogCommand(message, s.name)
				if err != nil {
					response = "error while logging command"
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

	s := Server{port: *port, name: *serverName, db: db}
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go s.handleConnection(c)
	}
}
