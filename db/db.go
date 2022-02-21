package db

import "errors"

type Database struct {
	db map[string]int
}

func NewDatabase() (db *Database, err error) {
	keyValueStore := make(map[string]int)
	db = &Database{db: keyValueStore}
	return db, nil
}

func (d *Database) SetKey(key string, value int) error {
	d.db[key] = value
	return nil
}

func (d *Database) GetKey(key string) (int, error) {
	val, exists := d.db[key]
	if !exists {
		return -1, errors.New("Key not found")
	}
	return val, nil
}
