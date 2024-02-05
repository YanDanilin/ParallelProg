package main

import (
	"os"
	"io"
	"encoding/json"
	"strconv"
	// "fmt"
	"log"
	"github.com/streadway/amqp"
)

type ConfigOperatorJSON struct {
	Username string
	Password string
	Host string
	Port uint64
}

const constConfigFilePath string = "./config.json"

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
	var configData ConfigOperatorJSON = ConfigOperatorJSON{}
	dataFromFile, err := io.ReadAll(file) // dataFromfile has type of []byte
	handleError(err, "Failed to read from file")
	if !json.Valid(dataFromFile) {
		handleError(nil, "Wrong json format")
	}
	jsonErr := json.Unmarshal(dataFromFile, &configData)
	handleError(jsonErr, "Failed to decode json file")

	// connecting to RabbitMQ
	connectionURL := "ampq: //" + configData.Username + ":" + configData.Password + "@" + configData.Host + ":" + strconv.FormatUint(configData.Port, 10) + "/"
	conn, err := amqp.Dial(connectionURL)
	handleError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// openning a channel
	channel, err := conn.Channel()
	handleError(err, "Failled to open a channel")
	defer channel.Close()

	taskQueue, err := channel.QueueDeclare(configData.TaskQueue, true, false, false, false, nil)
	handleError(err, "Failed to declare the \"tasks\" queue")

	tasks, err := channel.Consume(taskQueue.Name, "", true, false, false, false, nil)

	for task := range tasks {
		task.ConsumerTag
	}

}