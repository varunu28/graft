package logging

import "os"

const fileName string = "registry.txt"

func createFile() {
	var _, err = os.Stat(fileName)
	if os.IsNotExist(err) {
		var file, err = os.Create(fileName)
		if err != nil {
			return
		}
		file.Close()
	}
}

func RegisterServer(serverName string, port string) error {
	createFile()
	var file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	registryLog := serverName + "," + port + "\n"
	_, err = file.WriteString(registryLog)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}
