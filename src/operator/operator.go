package main

import (
	"container/list"
	"context"
	"encoding/json"
	"os/signal"
	"strings"
	"sync" // for mutex
	"syscall"
	"time" // for ConfigWorker.timeStart

	//"encoding/json"
	//"io"
	"flag"
	"os"

	// "strconv"
	//"fmt" // creating new errors
	"fmt"
	"log"
	"net"

	pb "github.com/YanDanilin/ParallelProg/protobuf"
	"google.golang.org/grpc"

	"github.com/YanDanilin/ParallelProg/utils"
)

type ConfigOperator struct {
	Host            string
	Port            string
	ListenWorkersOn string
}

type ConfigWorker struct {
	Id               WorkerID // unique id of worker made by operator
	OperatorHost     string   // worker will connect these host and port to send a request to connect ot server
	OperatorPort     string
	Host             string // where the worker works (figures out in main function)
	ListenOperatorOn string // worker will get info from operator on this port
	ListenManagerOn  string // worker will get tasks from manager on this port
	ManagerPort      string // worker will send response to the manager on this port
	IsManager        bool
	TimeStart        time.Time // timme when worker connected
}

var configFilePathFlag = flag.String("configPath", "./src/operator/config.json", "path to configuration file for operator")

type operator struct {
	pb.UnimplementedOperatorServer
}


func (s *operator) ProcessRequest(ctx context.Context, request *pb.RequestFromClient) (*pb.ResponseToClient, error) {
	log.Println("Client requested for summing", request.Array)
	var sum int64 = 0
	for _, elem := range request.Array {
		sum += int64(elem)
	}
	return &pb.ResponseToClient{Sum: sum}, nil
}

type ConfigStruct struct {
	ConfigOperator
}

type WorkerID int32

type Task struct {
}

type WorkersInfo struct {
	Workers      map[WorkerID]*ConfigWorker // all workers (including manager)
	ManagerID    WorkerID                   // manager id
	QueueTasks   *list.List                 // queue of Tasks
	WorkersCount int32                      // quantity of workers

}

func main() {
	flag.Parse()
	var configData ConfigStruct
	err := utils.DecodeConfigJSON(*configFilePathFlag, &configData)
	utils.HandleError(err, "Failed to get config parametrs")

	listener, err := net.Listen("tcp", configData.Host+":"+configData.Port)
	utils.HandleError(err, "Failed to listen port "+configData.Port)
	defer listener.Close()

	server := grpc.NewServer()
	pb.RegisterOperatorServer(server, &(operator{}))
	go func() {
		fmt.Println("Operator started")
		err = server.Serve(listener)
		utils.HandleError(err, "Failed to serve")
	}()
	workerListener, err := net.Listen("tcp", configData.Host+":"+configData.ListenWorkersOn)
	utils.HandleError(err, "Failed to listen port "+configData.ListenWorkersOn)
	defer workerListener.Close()

	stop := make(chan os.Signal) //, 1) why buffered

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	workersInfo := WorkersInfo{
		Workers:    make(map[WorkerID]*ConfigWorker),
		ManagerID:  -1,
		QueueTasks: list.New(),
	}
	var conn net.Conn
	var mutex sync.Mutex
	go func() {
	Loop:
		for {
			select {
			case <-stop:
				break Loop
			default:
				conn, err = workerListener.Accept()
				utils.HandleError(err, "Failed to accept connecction")
				handleWorker(conn, &workersInfo, &mutex) // connection closes inside this func
			}
		}
	}()

	<-stop
	server.Stop()
	fmt.Println("\nOperator stopped")
}

func handleWorker(conn net.Conn, workersInfo *WorkersInfo, mutex *sync.Mutex) error {
	defer conn.Close()
	workerAddr := conn.RemoteAddr().String()
	fmt.Println("Client connected:", workerAddr)
	buffer := make([]byte, 1024)
	bytesRead, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Failed to read someones config")
	}
	var workerConfigData *ConfigWorker = new(ConfigWorker)
	json.Unmarshal(buffer[:bytesRead], workerConfigData)
	fmt.Println(workerConfigData)

	connToWorker, err := net.Dial("tcp", strings.Split(workerAddr, ":")[0]+":"+workerConfigData.ListenOperatorOn)
	if err != nil {
		log.Println("connection failed")
	}
	msg := []byte("Data received")
	connToWorker.Write(msg)

	mutex.Lock()
	workersInfo.WorkersCount++
	if workersInfo.ManagerID == -1 {
		if workerConfigData.IsManager {
			workersInfo.ManagerID = WorkerID(workersInfo.WorkersCount)
		}
	} else {
		if workerConfigData.IsManager {
			workerConfigData.IsManager = false
		}
	}
	workerConfigData.Id = WorkerID(workersInfo.WorkersCount)
	workerConfigData.TimeStart = time.Now()
	workerConfigData.Host = strings.Split(workerAddr, ":")[0]
	if _, isIn := workersInfo.Workers[workerConfigData.Id]; !isIn {
		workersInfo.Workers[workerConfigData.Id] = workerConfigData
	}
	mutex.Unlock()

	fmt.Println("Client disconnected:", workerAddr)
	return err
}
