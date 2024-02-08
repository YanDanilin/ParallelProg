package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"time"

	// "os"
	// "strings"
	"log"

	"github.com/YanDanilin/ParallelProg/utils"
)

var configFilePathFlag = flag.String("configPath", "./src/worker/config.json", "path to configuration file for worker")
var roleFlag = flag.String("role", "worker", "set role of worker ['worker' | 'manager']")

type ConfigWorker struct {
	OperatorHost     string // worker will connect these host and port to send a request to connect ot server
	OperatorPort     string
	Host             string // where the worker works (figures out in main function)
	ListenOperatorOn string // worker will get info from operator on this port
	ListenManagerOn  string // worker will get tasks from manager on this port
	ManagerPort      string // worker will send response to the manager on this port
	IsManager        bool
}

type ConfigStruct struct {
	ConfigWorker
}

func main() {
	flag.Parse()
	var configData ConfigStruct
	err := utils.DecodeConfigJSON(*configFilePathFlag, &configData)
	utils.HandleError(err, "Failed to get config parametrs")

	if *roleFlag == "manager" {
		configData.IsManager = true
	}

	operatorListener, err := net.Listen("tcp", "localhost:"+configData.ListenOperatorOn)
	utils.HandleError(err, "Failed to listen port "+configData.ListenOperatorOn)
	defer operatorListener.Close()
	var conn net.Conn
	for {
		conn, err = net.Dial("tcp", configData.OperatorHost+":"+configData.OperatorPort)
		if err != nil {
			log.Println("Failed to connect. Trying again")
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}
	defer conn.Close()
	fmt.Println("Connect to operator!")

	message, err := json.Marshal(configData)
	_, err = conn.Write(message)
	utils.HandleError(err, "Failed to send message")

	connToOp, err := operatorListener.Accept()
	if err != nil {
		fmt.Println("Failed to read response from operator")
	}
	buffer := make([]byte, 1024)
	bytesRead, err := connToOp.Read(buffer)
	fmt.Println(string(buffer[:bytesRead]))
}
