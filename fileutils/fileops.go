package fileutils

import (
	"bufio"
	"os"
)

// CreateFileIfNotExists creates a file with given fileName if it doesn't exists
func CreateFileIfNotExists(fileName string) error {
	var _, err = os.Stat(fileName)
	if os.IsNotExist(err) {
		var file, err = os.Create(fileName)
		if err != nil {
			return err
		}
		file.Close()
	}
	return nil
}

// WriteToFile apppends the given message to a file
func WriteToFile(fileName string, message string) error {
	var file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = file.WriteString(message)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

// ReadFile reads contents of file into an array of strings
func ReadFile(fileName string) ([]string, error) {
	var lines []string
	file, err := os.Open(fileName)
	if err != nil {
		return lines, err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}
