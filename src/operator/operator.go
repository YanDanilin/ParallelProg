package main

import (
	"context" // used in func Accepting to process failed connection due to finishing main func
	// for Marshaling json
	"os/signal" // for signals
	"strings"   // for converting []byte to string
	"sync"      // for mutex
	"syscall"   // for syscall.SIGTERM
	"time"      // for ConfigWorker.timeStart

	//"encoding/json"
	//"io"
	"flag" // for flag role and config path
	"os"   // for signals

	// "strconv"
	//"fmt" // creating new errors
	"fmt"
	"log"
	"net"

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	pb "github.com/YanDanilin/ParallelProg/protobuf"
	"github.com/YanDanilin/ParallelProg/utils"
	"github.com/google/uuid" // for creating unique ids

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var configFilePathFlag = flag.String("configPath", "./src/operator/config.json", "path to configuration file for operator")

type ConfigOperator struct {
	Host            string
	Port            string
	ListenWorkersOn string
	ListenManagerOn string
	MaxTasks        int32
}

type ConfigStruct struct {
	ConfigOperator
}

type WorkerID uuid.UUID
type TaskID uuid.UUID

type ConfigWorker struct {
	ID WorkerID // unique id of worker made by operator
	// OperatorHost     string   // worker will connect these host and port to send a request to connect to server
	// OperatorPort     string
	Host             string // where the worker works (figures out in main function)
	ListenOperatorOn string // worker gets info from operator on this port
	ListenOn         string // worker will get tasks from manager on this port
	// ManagerHost string
	// ManagerPort string    // worker dials and sends responses on this port
	IsManager bool      //if true, ManagerPort is where manager gets responses and ListenManagerOn is where to send tasks
	TimeStart time.Time // timme when worker connected
	IsBusy    bool
}

type WorkersInfo struct {
	Workers      map[WorkerID]*ConfigWorker // all workers (including manager)
	ManagerID    WorkerID                   // manager id
	ManagerHost  string
	ManagerPort  string
	WorkersCount int32 // quantity of workers
}

type operator struct {
	pb.UnimplementedOperatorServer
	ConfigData      ConfigOperator
	mutex           sync.Mutex
	Info            WorkersInfo
	ConnToManager   net.Conn
	ManagerListener net.Listener
	Tasks           map[TaskID]*cmpb.Task
	Responses       map[TaskID]*cmpb.Response
}

func (operServer *operator) AddTask(arr []int32) TaskID {
	id := TaskID(uuid.New())
	for _, isIn := operServer.Tasks[id]; isIn; {
		id = TaskID(uuid.New())
	}
	operServer.Tasks[id] = new(cmpb.Task)
	operServer.Tasks[id].ID = uuid.UUID(id).String()
	operServer.Tasks[id].Ack = false
	operServer.Tasks[id].Array = make([]int32, 0)
	copy(operServer.Tasks[id].Array, arr)
	return id
}

// func (operServer *operator) ConnectToManager() {
// 	ticker := time.NewTicker(time.Second)
// Loop:
// 	for {
// 		select {
// 		case <-ticker.C:
// 			operServer.mutex.Lock()
// 			if operServer.Info.ManagerID != WorkerID(uuid.Nil) {
// 				ticker.Stop()
// 				var err error
// 				operServer.ConnToManager, err = net.Dial("tcp", operServer.Info.ManagerHost+":"+operServer.Info.Workers[operServer.Info.ManagerID].ListenOperatorOn)
// 				operServer.mutex.Unlock()
// 				if err != nil {
// 					log.Println(err, "msg: Failed to connect to manager")
// 					ticker = time.NewTicker(time.Second)
// 				} else {
// 					break Loop
// 				}
// 			}
// 		}
// 	}
// }

func (operServer *operator) SendTaskToManager(task *cmpb.Task) { // func is called when connection exists
	msg, _ := proto.Marshal(task)
	for {
		operServer.mutex.Lock()
		if _, err := operServer.ConnToManager.Write(msg); err != nil {
			operServer.mutex.Unlock()
			// time.Sleep(500 * time.Millisecond)
		} else {
			operServer.mutex.Unlock()
			break
		}
	}
}

func (operServer *operator) ProcessRequest(ctx context.Context, request *pb.RequestFromClient) (*pb.ResponseToClient, error) {
	log.Println("Client requested for summing", request.Array)
	var sum int64 = 0
	id := operServer.AddTask(request.Array)
	operServer.SendTaskToManager(operServer.Tasks[id])
	for {
		if response, isIn := operServer.Responses[id]; isIn {
			sum = response.Res
			break
		}
		// response, err := operServer.WaitForResponse()
		// if err != nil {
		// 	log.Println("Manager disconnected")
		// }
		// if response.ID == uuid.UUID(id).String() {
		// 	sum = response.Res
		// 	break
		// }
	}
	return &pb.ResponseToClient{Sum: sum}, nil
}

func (operServer *operator) ListenManager(ctx context.Context) {
	go func() {
		<-ctx.Done()
	}()
	var err1 error
	operServer.ManagerListener, err1 = net.Listen("tcp", operServer.ConfigData.Host+":"+operServer.ConfigData.ListenManagerOn)
	if err1 != nil {
		log.Fatalln("Failed to listen manager")
	}
	for {
		conn, err := operServer.ManagerListener.Accept()
		if err != nil {
			//handle error
		}
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		if err != nil {
			if ctx.Err() == context.Canceled {
				return
			}
			// handle error
		}
		response := new(cmpb.Response)
		proto.Unmarshal(buffer[:bytesRead], response)
		id, _ := uuid.Parse(response.ID)
		operServer.Responses[TaskID(id)] = response
	}
}

func (operServer *operator) Accepting(ctx context.Context, workerListener net.Listener, mutex *sync.Mutex) {
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
			err = operServer.handleWorker(conn, mutex) // connection closes inside this func
			if err != nil {
				log.Println("Failed to handle worker connection")
			}
		}

	}
}

func (operServer *operator) handleWorker(conn net.Conn, mutex *sync.Mutex) error {
	// defer conn.Close()
	workerAddr := conn.RemoteAddr().String()
	fmt.Println("Worker connected:", workerAddr)

	buffer := make([]byte, 1024)
	bytesRead, err := conn.Read(buffer)
	if err != nil {
		log.Println("Failed to read config from " + workerAddr)
		return err
	}

	var workerConfigData *ConfigWorker = new(ConfigWorker)
	var request cmpb.RequestToConnect
	proto.Unmarshal(buffer[:bytesRead], &request)
	workerConfigData.Host = strings.Split(workerAddr, ":")[0]
	workerConfigData.ListenOperatorOn = request.ListenOperatorOn
	workerConfigData.IsManager = request.IsManager
	workerConfigData.ListenOn = request.ListenOn
	connToWorker, err := net.Dial("tcp", workerConfigData.Host+":"+workerConfigData.ListenOperatorOn)
	if err != nil {
		log.Println("Connection failed")
	}
	if operServer.Info.ManagerID == WorkerID(uuid.Nil) && !workerConfigData.IsManager {
		reply, err := proto.Marshal(&cmpb.ReplyToConnect{Type: "[WAIT]", Msg: "Still no manager in system\n Wait..."})
		connToWorker.Write(reply)
		fmt.Println("Worker disconnected:", workerAddr)
		return err
	}

	mutex.Lock()
	operServer.Info.WorkersCount++
	id := WorkerID(uuid.New())
	var msg string
	for _, isIn := operServer.Info.Workers[id]; isIn; {
		id = WorkerID(uuid.New())
	}
	if operServer.Info.ManagerID == WorkerID(uuid.Nil) {
		if workerConfigData.IsManager {
			operServer.Info.ManagerID = id
			operServer.Info.ManagerHost = workerConfigData.Host
			operServer.Info.ManagerPort = workerConfigData.ListenOn
		}
	} else {
		if workerConfigData.IsManager {
			workerConfigData.IsManager = false
			msg = "Manager already exists\nYou are assigned to worker"
		}
	}
	workerConfigData.ID = id
	// if _, isIn := operServer.Info.Workers[workerConfigData.Id]; !isIn {
	workerConfigData.TimeStart = time.Now()
	operServer.Info.Workers[workerConfigData.ID] = workerConfigData
	// }
	var lisManagerOn string
	if workerConfigData.IsManager {
		lisManagerOn = operServer.ConfigData.ListenManagerOn
	}
	reply, err := proto.Marshal(&cmpb.ReplyToConnect{
		Type:            "[INFO]",
		ManagerHost:     operServer.Info.ManagerHost,
		ManagerPort:     operServer.Info.ManagerPort,
		ListenManagerOn: lisManagerOn,
		IsManager:       workerConfigData.IsManager,
		Msg:             msg,
	})
	_, err = connToWorker.Write(reply)
	if err != nil { // надо как-то по-другому обработать
		fmt.Println("Failed to send reply to worker " + workerAddr)
	}
	if workerConfigData.IsManager {
		operServer.ConnToManager = connToWorker
	} else {
		conn.Close()
		msgToManager, _ := proto.Marshal(&cmpb.OperToManager{
			Type:           "[INFO]",
			ID:             uuid.UUID(workerConfigData.ID).String(),
			WorkerHost:     workerConfigData.Host,
			WorkerListenOn: workerConfigData.ListenOn,
		})
		operServer.ConnToManager.Write(msgToManager)
	}
	mutex.Unlock()
	fmt.Println("Worker disconnected:", workerAddr)
	return err
}

func (operServer *operator) CheckManager() {

}

func main() {
	flag.Parse()
	var configData ConfigStruct
	err := utils.DecodeConfigJSON(*configFilePathFlag, &configData)
	utils.HandleError(err, "Failed to get config parametrs")

	listener, err := net.Listen("tcp", configData.Host+":"+configData.Port)
	utils.HandleError(err, "Failed to listen port "+configData.Port)
	defer listener.Close()
	var operServer operator
	operServer.Info = WorkersInfo{
		Workers:      make(map[WorkerID]*ConfigWorker),
		ManagerID:    WorkerID(uuid.Nil),
		WorkersCount: 0,
	}
	operServer.Tasks = make(map[TaskID]*cmpb.Task)
	operServer.Responses = make(map[TaskID]*cmpb.Response)
	operServer.ConfigData = ConfigOperator{
		Host:            configData.Host,
		Port:            configData.Port,
		ListenWorkersOn: configData.ListenWorkersOn,
		ListenManagerOn: configData.ListenManagerOn,
		MaxTasks:        configData.MaxTasks,
	}

	workerListener, err := net.Listen("tcp", configData.Host+":"+configData.ListenWorkersOn)
	utils.HandleError(err, "Failed to listen port "+configData.ListenWorkersOn)
	defer workerListener.Close()

	stop := make(chan os.Signal) //, 1) // why buffered
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	//var mutex sync.Mutex
	ctxW, cancelW := context.WithCancel(context.Background())
	ctxM, cancelM := context.WithCancel(context.Background())
	go operServer.Accepting(ctxW, workerListener, &operServer.mutex)
	// operServer.ConnectToManager(&operServer.mutex)

	server := grpc.NewServer()
	pb.RegisterOperatorServer(server, &operServer)
	go func() {
		fmt.Println("Operator started")
		err = server.Serve(listener)
		utils.HandleError(err, "Failed to serve")
	}()

	go operServer.ListenManager(ctxM)

	<-stop
	cancelW()
	cancelM()
	time.Sleep(500 * time.Millisecond)
	server.Stop()
	fmt.Println("\nOperator stopped")
}
