package db

import (
	"errors"
	"graft/fileutils"
	"strconv"
	"strings"
)

type Database struct {
	db map[string]int
}

func NewDatabase() (db *Database, err error) {
	keyValueStore := make(map[string]int)
	db = &Database{db: keyValueStore}
	return db, nil
}

func (d *Database) setKey(key string, value int) error {
	d.db[key] = value
	return nil
}

func (d *Database) getKey(key string) (int, error) {
	val, exists := d.db[key]
	if !exists {
		return -1, errors.New("key not found")
	}
	return val, nil
}

func (d *Database) deleteKey(key string) error {
	_, exists := d.db[key]
	if !exists {
		return errors.New("key not found")
	}
	delete(d.db, key)
	return nil
}

// ValidateCommand performs validation for commands received from client for DB operations
func (d *Database) ValidateCommand(command string) error {
	splits := strings.Split(command, " ")
	operation := splits[0]
	if operation == "GET" || operation == "DELETE" {
		if len(splits) != 2 {
			return errors.New("need a key for GET/DELETE operation")
		}
	} else if operation == "SET" {
		if len(splits) != 3 {
			return errors.New("need a key and a value for SET operation")
		}
		_, err := strconv.Atoi(splits[2])
		if err != nil {
			return errors.New("not a valid integer value")
		}
	} else {
		return errors.New("invalid command")
	}
	return nil
}

// PerformOperations updates the storage by processing given operation
func (d *Database) PerformDbOperations(command string) string {
	splits := strings.Split(command, " ")
	operation := splits[0]
	var response string = ""
	if operation == "GET" {
		key := splits[1]
		val, err := d.getKey(key)
		if err != nil {
			response = "Key not found error"
		} else {
			response = "Value for key (" + key + ") is: " + strconv.Itoa(val)
		}
	} else if operation == "SET" {
		key := splits[1]
		val, _ := strconv.Atoi(splits[2])
		if response == "" {
			if err := d.setKey(key, val); err != nil {
				response = "Error inserting key in DB"
			}
		}
		if response == "" {
			response = "Key set successfully"
		}
	} else if operation == "DELETE" {
		key := splits[1]
		if err := d.deleteKey(key); err != nil {
			response = "Key not found"
		}
		if response == "" {
			response = "Key deleted successfully"
		}
	}
	return response
}

// LogDbCommand logs the database command to log file
func (d *Database) LogCommand(command string, serverName string) error {
	fileName := serverName + ".txt"
	var err = fileutils.CreateFileIfNotExists(fileName)
	if err != nil {
		return err
	}
	err = fileutils.WriteToFile(fileName, serverName+","+command+"\n")
	if err != nil {
		return err
	}
	return nil
}

func (d *Database) RebuildLogIfExists(serverName string) []string {
	logs := make([]string, 0)
	fileName := serverName + ".txt"
	fileutils.CreateFileIfNotExists(fileName)
	lines, _ := fileutils.ReadFile(fileName)
	for _, line := range lines {
		splits := strings.Split(line, ",")
		logs = append(logs, splits[1])
	}
	return logs
}
