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

func parseFlags() {
	flag.Parse()

	if *serverName == "" {
		log.Fatalf("Must provide serverName for the server")
	}

	if *port == "" {
		log.Fatalf("Must provide a port number for server to run")
	}
}

func handleConnection(c net.Conn, db *db.Database) {
	for {
		data, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		message := strings.TrimSpace(string(data))
		if message == "STOP" {
			break
		}
		fmt.Println(">", string(message))
		splits := strings.Split(message, " ")
		operation := splits[0]
		var response string
		if operation == "GET" {
			key := splits[1]
			val, err := db.GetKey(key)
			if err != nil {
				response = "Key not found error"
			} else {
				response = "Value for key (" + key + ") is: " + strconv.Itoa(val)
			}
		} else if operation == "SET" {
			key := splits[1]
			val, err := strconv.Atoi(splits[2])
			if err != nil {
				response = "Not a valid integer value"
			}
			if response == "" {
				if err := db.SetKey(key, val); err != nil {
					response = "Error inserting key in DB"
				}
			}
			if response == "" {
				response = "Key set successfully"
			}
		} else if operation == "DELETE" {
			key := splits[1]
			if err := db.DeleteKey(key); err != nil {
				response = "Key not found"
			}
			if response == "" {
				response = "Key deleted successfully"
			}
		} else {
			response = "Invalid command"
		}
		c.Write([]byte(response + "\n"))
	}
	c.Close()
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

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c, db)
	}
}
