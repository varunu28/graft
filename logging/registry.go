package logging

import (
	"multiclient-server/fileutils"
	"strconv"
	"strings"
)

const registryFileName string = "registry.txt"

// RegisterServer appends a registry log into registry for server
func RegisterServer(serverName string, port string) error {
	var err = fileutils.CreateFileIfNotExists(registryFileName)
	if err != nil {
		return err
	}
	registryLog := serverName + "," + port + "\n"
	err = fileutils.WriteToFile(registryFileName, registryLog)
	if err != nil {
		return err
	}
	return nil
}

// ListRegisteredServer returns a mapping of serverName to corresponding port
func ListRegisteredServer() (map[string]int, error) {
	m := make(map[string]int)
	registeryLines, err := fileutils.ReadFile(registryFileName)
	if err != nil {
		return m, err
	}
	for _, line := range registeryLines {
		splits := strings.Split(line, ",")
		port, _ := strconv.Atoi(splits[1])
		m[splits[0]] = port
	}
	return m, nil
}
