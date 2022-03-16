package logging

import (
	"errors"
	"graft/fileutils"
	"strings"
)

const serverStateFileName string = "server-state.txt"

func PersistServerState(serverStateLog string) error {
	var err = fileutils.CreateFileIfNotExists(serverStateFileName)
	if err != nil {
		return err
	}
	err = fileutils.WriteToFile(serverStateFileName, serverStateLog+"\n")
	if err != nil {
		return err
	}
	return nil
}

func GetLatestServerStateIfPresent(serverName string) (string, error) {
	serverStateLogs, err := fileutils.ReadFile(serverStateFileName)
	var serverStateLog = ""
	if err != nil {
		return "", err
	}
	for _, line := range serverStateLogs {
		splits := strings.Split(line, ",")
		if splits[0] == serverName {
			serverStateLog = line
		}
	}
	if serverStateLog == "" {
		return "", errors.New("no existing server state found")
	}
	return serverStateLog, nil
}
