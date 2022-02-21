package main

import (
	"bufio"
	"fmt"
	"multiclient-server/db"
	"net"
	"os"
	"strconv"
	"strings"
)

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
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Port required for server")
		return
	}
	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp", PORT)
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

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c, db)
	}
}
