package main

import (
	"encoding/json"
	"io"  // чтение файлов и вывод
	"log" // log.Fatalf
	"os"  // открытие фалов
	//"time" чтобы спать
	"fmt"
)

const constConfigFilePath string = "./config.json"

type ConfigWorkerJSON struct {
	Username   string
	Password   string
	Host       string
	Port       uint16
	MaxRequest uint32
}

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("error: %s\tmsg: %s", err, msg)
	}
}


func main() {
	configFilePath := constConfigFilePath
	if (len(os.Args) > 1) {
		configFilePath = os.Args[1]
	}
	// reading config.json
	file, err := os.Open(configFilePath)
	if err != nil {
		panic("Failed to open file")
	}
	defer file.Close()
	var configData ConfigWorkerJSON = ConfigWorkerJSON{}
	dataFromFile, err := io.ReadAll(file) // dataFromfile has type of []byte
	handleError(err, "Failed to read from file")
	if !json.Valid(dataFromFile) {
		handleError(nil, "Wrong json format")
	}
	jsonErr := json.Unmarshal(dataFromFile, &configData)
	handleError(jsonErr, "Failed to decode json file")

	fmt.Println(configData)
}
