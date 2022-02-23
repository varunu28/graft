package db

import (
	"errors"
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

func (d *Database) PerformDbOperations(message string) string {
	splits := strings.Split(message, " ")
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
		val, err := strconv.Atoi(splits[2])
		if err != nil {
			response = "Not a valid integer value"
		}
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
	} else {
		response = "Invalid command"
	}
	return response
}
