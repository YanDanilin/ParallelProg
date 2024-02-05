package main

import (
	"encoding/json"
	"io"
	"os"

	//"strconv"
	// "fmt"
	"log"
)

type ConfigOperatorJSON struct {
	Host string
	Port uint64
}

const constConfigFilePath string = "./config.json"

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("error: %s\tmsg: %s", err, msg)
	}
}

type operator struct {
	pb.U
}

func main() {
	configFilePath := constConfigFilePath
	if len(os.Args) > 1 {
		configFilePath = os.Args[1]
	}

	// reading config.json
	file, err := os.Open(configFilePath)
	if err != nil {
		panic("Failed to open file")
	}
	defer file.Close()
	var configData ConfigOperatorJSON = ConfigOperatorJSON{}
	dataFromFile, err := io.ReadAll(file) // dataFromfile has type of []byte
	handleError(err, "Failed to read from file")
	if !json.Valid(dataFromFile) {
		handleError(nil, "Wrong json format")
	}
	jsonErr := json.Unmarshal(dataFromFile, &configData)
	handleError(jsonErr, "Failed to decode json file")

}
