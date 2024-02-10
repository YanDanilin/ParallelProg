package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	// "os"
	"log"
	"strings"

	"github.com/YanDanilin/ParallelProg/utils"
	"github.com/google/uuid"
	cmpb "github.com/YanDanilin/ParallelProg/communication"

	"google.golang.org/protobuf/proto"
)

var configFilePathFlag = flag.String("configPath", "./src/worker/config.json", "path to configuration file for worker")
var roleFlag = flag.String("role", "worker", "set role of worker ['worker' | 'manager']")

type WorkerID uuid.UUID

type ConfigWorker struct {
	ID               WorkerID // unique id of worker made by operator
	OperatorHost     string   // worker will connect these host and port to send a request to connect ot server
	OperatorPort     string
	Host             string // where the worker works (figures out in main function)
	ListenOperatorOn string // worker will get info from operator on this port
	// ListenManagerOn  string // worker will get tasks from manager on this port
	ManagerHost string
	ManagerPort      string // worker will send response to the manager on this port
	IsManager        bool
	IsBusy           bool
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
	message, err := json.Marshal(configData)
	var conn net.Conn
	for {
		conn, err = net.Dial("tcp", configData.OperatorHost+":"+configData.OperatorPort)
		if err != nil {
			log.Println("Failed to connect. Trying again")
			time.Sleep(time.Second * 5)
			conn, err = net.Dial("tcp", configData.OperatorHost+":"+configData.OperatorPort)
			utils.HandleError(err, "Failed to connect after waiting")
		} // } else {
		// 	break
		// }
		// }

		fmt.Println("Connected to operator!")

		_, err = conn.Write(message)
		utils.HandleError(err, "Failed to send message")
		// for {
		connToOper, err := operatorListener.Accept()
		if err != nil {
			fmt.Println("Failed to read response from operator")
		}
		buffer := make([]byte, 1024)
		bytesRead, err := connToOper.Read(buffer)
		utils.HandleError(err, "here")
		fmt.Println(string(buffer[:bytesRead]))
		if string(buffer[:6]) == "[WAIT]" {
			time.Sleep(time.Second * 2)
			fmt.Println(string(buffer[7:bytesRead]))
			conn.Close()
			continue
		}
		if string(buffer[:6]) == "[INFO]" {
			configData.ManagerHost = strings.Split(string(buffer[7:]), ":")[0]
			configData.ManagerPort = strings.Split(string(buffer[7:]), ":")[1]
			break
		}
	}
	defer conn.Close()
	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
}
