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
	IsBusy           bool
}

var configFilePathFlag = flag.String("configPath", "./src/operator/config.json", "path to configuration file for operator")

type WorkersInfo struct {
	Workers      map[WorkerID]*ConfigWorker // all workers (including manager)
	ManagerID    WorkerID                   // manager id
	WorkersCount int32                      // quantity of workers
}

type operator struct {
	pb.UnimplementedOperatorServer
	Info       WorkersInfo
	QueueTasks *list.List // queue of Tasks
}

var operServer operator

func (s *operator) ProcessRequest(ctx context.Context, request *pb.RequestFromClient) (*pb.ResponseToClient, error) {
	log.Println("Client requested for summing", request.Array)
	var sum int64 = 0
	// for _, elem := range request.Array {
	// 	sum += int64(elem)
	// }
	time.Sleep(time.Second * 3)
	fmt.Println(s.Info)
	sum = 10
	return &pb.ResponseToClient{Sum: sum}, nil
}

type ConfigStruct struct {
	ConfigOperator
}

type WorkerID int32

type Task struct {
	Ack       bool
	Array     []int32
	InProcess bool
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

	stop := make(chan os.Signal) //, 1) // why buffered

	operServer.Info = WorkersInfo{
		Workers:      make(map[WorkerID]*ConfigWorker),
		ManagerID:    -1,
		WorkersCount: 0,
	}
	operServer.QueueTasks = list.New()
	var mutex sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	// go func() {
	// 	for {
	// 		conn, err = workerListener.Accept()
	// 		if err != nil {
	// 			log.Println(err, "Failed to accept connecction")
	// 		} else {
	// 			handleWorker(conn, &mutex) // connection closes inside this func
	// 		}
	// 	}
	// }()
	go Accepting(ctx, workerListener, &mutex)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	cancel()
	time.Sleep(500 * time.Millisecond)
	server.Stop()
	fmt.Println("\nOperator stopped")
}

func Accepting(ctx context.Context, workerListener net.Listener, mutex *sync.Mutex) {
	var conn net.Conn
	var err error
	go func() {
		<-ctx.Done()
	}()
	for {
		conn, err = workerListener.Accept()
		if err != nil {
			if ctx.Err() == context.Canceled {
				return
			}
			log.Println(err, "Failed to accept connecction")
		} else {
			handleWorker(conn, mutex) // connection closes inside this func
		}

	}
}

func handleWorker(conn net.Conn, mutex *sync.Mutex) error {
	defer conn.Close()
	workerAddr := conn.RemoteAddr().String()
	fmt.Println("Worker connected:", workerAddr)
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
	fmt.Println(operServer.Info)
	if operServer.Info.ManagerID == -1 && !workerConfigData.IsManager {
		connToWorker.Write([]byte("[WAIT] Still no manager in system\n Wait..."))
		fmt.Println("Worker disconnected:", workerAddr)
		return nil
	}

	mutex.Lock()
	operServer.Info.WorkersCount++
	if operServer.Info.ManagerID == -1 {
		if workerConfigData.IsManager {
			operServer.Info.ManagerID = WorkerID(operServer.Info.WorkersCount)
		}
	} else {
		if workerConfigData.IsManager {
			workerConfigData.IsManager = false
		}
	}
	workerConfigData.Id = WorkerID(operServer.Info.WorkersCount)
	workerConfigData.TimeStart = time.Now()
	workerConfigData.Host = strings.Split(workerAddr, ":")[0]
	if _, isIn := operServer.Info.Workers[workerConfigData.Id]; !isIn {
		operServer.Info.Workers[workerConfigData.Id] = workerConfigData
	}
	msg := []byte("[INFO] " + operServer.Info.Workers[operServer.Info.ManagerID].Host + ":" + operServer.Info.Workers[operServer.Info.ManagerID].ManagerPort)
	connToWorker.Write(msg)
	fmt.Println(operServer.Info)
	fmt.Println(*operServer.Info.Workers[operServer.Info.ManagerID])
	mutex.Unlock()

	fmt.Println("Worker disconnected:", workerAddr)
	return err
}
