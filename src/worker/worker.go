package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	"github.com/YanDanilin/ParallelProg/utils"
	"github.com/google/uuid"

	"google.golang.org/protobuf/proto"
)

var configFilePathFlag = flag.String("configPath", "./src/worker/config.json", "path to configuration file for worker")
var roleFlag = flag.String("role", "worker", "set role of worker ['worker' | 'manager']")

type WorkerID uuid.UUID

type ConfigWorker struct {
	// ID               WorkerID // unique id of worker made by operator
	OperatorHost     string // worker will connect these host and port to send a request to connect ot server
	OperatorPort     string
	Host             string // where the worker works
	ListenOperatorOn string // worker will get info from operator on this port
	ListenOn         string // worker gets tasks from manager on this port
	//ManagerHost      string
	//ManagerPort      string // worker dials and sends responses to the manager on this port
	IsManager bool // if true, ManagerPort is where manager gets responses and ListenManagerOn is where to send tasks
}

type ConfigStruct struct {
	ConfigWorker
}

type Worker struct {
	Config          ConfigWorker
	ManagerHost     string
	ManagerPort     string // worker dials and sends responses to the manager on this port
	operListener    net.Listener
	ConnToOper      net.Conn
	TasksCount      int32
	MyID            WorkerID
	mutex           sync.Mutex
	managerListener net.Listener
}

func (worker *Worker) Exec(stopCtx context.Context) {
	fmt.Println(worker.Config.IsManager)
	if worker.Config.IsManager {
		worker.ExecManager(stopCtx)
	} else {
		fmt.Println("exec func")
		changeToManagerCtx, changeToManagerCancel := context.WithCancel(context.Background())
		worker.ExecWorker(stopCtx, changeToManagerCtx, changeToManagerCancel)
		if stopCtx.Err() != context.Canceled {
			if changeToManagerCtx.Err() == context.Canceled {
				fmt.Println("switching to manager")
				worker.ExecManager(stopCtx)
			}
		}
	}
}

func main() {
	flag.Parse()
	var configData ConfigStruct
	err := utils.DecodeConfigJSON(*configFilePathFlag, &configData)
	utils.HandleError(err, "Failed to get config parametrs")

	if *roleFlag == "manager" {
		configData.IsManager = true
	}
	var worker Worker = Worker{}
	worker.operListener, err = net.Listen("tcp", configData.Host+":"+configData.ListenOperatorOn)
	utils.HandleError(err, "Failed to listen port "+configData.ListenOperatorOn)
	defer worker.operListener.Close()
	message, _ := proto.Marshal(&cmpb.RequestToConnect{
		ListenOperatorOn: configData.ListenOperatorOn,
		IsManager:        configData.IsManager,
		ListenOn:         configData.ListenOn,
	})
	worker.Config = configData.ConfigWorker
	var conn net.Conn

	for {
		conn, err = net.Dial("tcp", configData.OperatorHost+":"+configData.OperatorPort)
		if err != nil {
			log.Println("Failed to connect. Trying again")
			time.Sleep(time.Second * 5)
			conn, err = net.Dial("tcp", configData.OperatorHost+":"+configData.OperatorPort)
			utils.HandleError(err, "Failed to connect after waiting")
		}
		fmt.Println("Connected to operator!")

		_, err = conn.Write(message)
		fmt.Println("msg written")
		utils.HandleError(err, "Failed to send message")
		buffer := make([]byte, 1024)
		worker.ConnToOper, err = worker.operListener.Accept()
		if err != nil {
			fmt.Println("Failed to read response from operator")
		}
		bytesRead, err := worker.ConnToOper.Read(buffer)
		utils.HandleError(err, "Failed to read reply from operator")
		var reply cmpb.ReplyToConnect
		proto.Unmarshal(buffer[:bytesRead], &reply)
		fmt.Println(reply.Type)
		if reply.Type == "[WAIT]" {
			fmt.Println(reply.Msg)
			time.Sleep(time.Second * 5)
			worker.ConnToOper.Close()
			conn.Close()
			continue
		} else if reply.Type == "[INFO]" {
			worker.ManagerHost = reply.ManagerHost
			worker.ManagerPort = reply.ManagerPort
			worker.Config.OperatorPort = reply.OperPort
			id, _ := uuid.Parse(reply.ID)
			fmt.Println(id.String())
			worker.MyID = WorkerID(id)
			if configData.IsManager && !reply.IsManager {
				log.Println(reply.Msg)
			}
			worker.Config.IsManager = reply.IsManager
			conn.Close()
			break
		} else {
			log.Println(reply.Msg)
		}
		conn.Close()
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	stopCtx, stopCancel := context.WithCancel(context.Background())
	fmt.Println("Ready to exec")
	go worker.Exec(stopCtx)

	<-stop
	stopCancel()
	fmt.Println("Worker stopped")
}
