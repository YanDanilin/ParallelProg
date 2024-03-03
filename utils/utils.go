package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("error: %s\tmsg: %s", err, msg)
	}
}

func DecodeConfigJSON(configFilePath string, val interface{}) error {
	// reading config.json
	file, err := os.Open(configFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	dataFromFile, err := io.ReadAll(file) // dataFromfile has type of []byte
	if err != nil {
		return err
	}
	if !json.Valid(dataFromFile) {
		return fmt.Errorf("file %s has wrong json format", configFilePath)
	}
	err = json.Unmarshal(dataFromFile, val)
	return err
}

func Equal(a, b []int32) bool {
    if len(a) != len(b) {
        return false
    }
    for i, v := range a {
        if v != b[i] {
            return false
        }
    }
    return true
}