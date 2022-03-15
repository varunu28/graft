package logging

import "graft/fileutils"

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
