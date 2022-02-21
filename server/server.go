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

var count = 0

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
		if strings.HasPrefix(message, "GET") {
			splits := strings.Split(message, " ")
			val, err := db.GetKey(splits[1])
			if err != nil {
				c.Write([]byte("Key not found error \n"))
			} else {
				c.Write([]byte("Value for key (" + splits[1] + ") is: " + strconv.Itoa(val) + "\n"))
			}
		} else if strings.HasPrefix(message, "SET") {
			splits := strings.Split(message, " ")
			key := splits[1]
			val, _ := strconv.Atoi(splits[2])
			if err := db.SetKey(key, val); err != nil {
				fmt.Println("Error inserting key in DB")
			}
			c.Write([]byte("Key set successfully \n"))
		} else {
			c.Write([]byte("Invalid command \n"))
		}
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
		count++
		fmt.Printf("Number of active client connections: (%d)", count)
	}
}
