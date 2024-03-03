package main

import (
	"context" // used in func Accepting to process failed connection due to finishing main func
	"flag"    // for flag role and config path
	"fmt"
	"log"
	"net"
	"os"        // for signals
	"os/signal" // for signals
	"strconv"
	"strings" // for converting []byte to string
	"sync"    // for mutex
	"syscall" // for syscall.SIGTERM
	"time"    // for ConfigWorker.timeStart

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	pb "github.com/YanDanilin/ParallelProg/protobuf"
	"github.com/YanDanilin/ParallelProg/utils"
	"github.com/google/uuid" // for creating unique ids

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var configFilePathFlag = flag.String("configPath", "./src/operator/config.json", "path to configuration file for operator")

const (
	typeTime       string = "[TIME]"
	typeCheck      string = "[CHECK]"
	typeOrder      string = "[ORDER]"
	typeMInfo      string = "[MANAGERINFO]" // мб не нужен
	typeBusy       string = "[BUSY]"
	typeFree       string = "[FREE]"
	typeChange     string = "[CHANGE]"
	typeInfo       string = "[INFO]"
	typeTask       string = "[TASK]"
	typeResp       string = "[RESPONSE]"
	typeReady      string = "[READY]"
	typeWorkerDead string = "[WORKERDEAD]"
)

type ConfigOperator struct {
	Host            string
	Port            string
	ListenWorkersOn string
	ListenManagerOn string
	CheckTime       int32
	MaxTasks        int32
}

type ConfigStruct struct {
	ConfigOperator
}

type WorkerID uuid.UUID
type TaskID uuid.UUID

type ConfigWorker struct {
	ID               WorkerID  // unique id of worker made by operator
	Host             string    // where the worker works (figures out in main function)
	ListenOperatorOn string    // worker gets info from operator on this port
	ListenOn         string    // worker will get tasks from manager on this port
	IsManager        bool      //if true, ManagerPort is where manager gets responses and ListenManagerOn is where to send tasks
	TimeStart        time.Time // timme when worker connected
	IsBusy           bool
	Task             TaskID
}

type WorkersInfo struct {
	Workers      map[WorkerID]*ConfigWorker // all workers (including manager)
	ManagerID    WorkerID                   // manager id
	ManagerHost  string
	ManagerPort  string
	WorkersCount int32 // quantity of workers
}
type TaskInfo struct {
	Task *cmpb.Task
	// Worker WorkerID
}

type operator struct {
	pb.UnimplementedOperatorServer
	ConfigData             ConfigOperator
	mutex                  sync.Mutex
	responseMutex          sync.Mutex
	Info                   WorkersInfo
	ConnToManager          net.Conn
	ManagerListener        net.Listener
	Tasks                  map[TaskID]TaskInfo //*cmpb.Task
	Responses              map[TaskID]*cmpb.Response
	ListeningConnToManager net.Conn
	TasksAfterRecovering   map[TaskID]TaskInfo
	AfterRecovering        bool // if operator once dead is true
}

func (operServer *operator) AddTask(arr []int32) TaskID {
	id := TaskID(uuid.New())
	for _, isIn := operServer.Tasks[id]; isIn; {
		id = TaskID(uuid.New())
	}
	fmt.Println("add task", arr)
	task := new(cmpb.Task)
	task.Type = typeTask
	task.ID = uuid.UUID(id).String()
	task.Array = make([]int32, len(arr))
	copy(task.Array, arr)
	operServer.Tasks[id] = TaskInfo{Task: task}
	return id
}

func (operServer *operator) SendTaskToManager(task *cmpb.Task) { // func is called when connection exists
	fmt.Println(task.Array)
	msg, _ := proto.Marshal(task)
	for {
		operServer.mutex.Lock()
		if operServer.Info.ManagerID == WorkerID(uuid.Nil) {
			operServer.mutex.Unlock()
			log.Println("SendTaskToManager: No manager yet")
			time.Sleep(2 * time.Second)
		} else {
			if _, err := operServer.ConnToManager.Write(msg); err != nil {
				operServer.mutex.Unlock()
				fmt.Println("task not written to manager")
				// time.Sleep(500 * time.Millisecond)
			} else {
				fmt.Println("task written to manager")
				operServer.mutex.Unlock()
				break
			}
		}
	}
}

func (operServer *operator) ProcessRequest(ctx context.Context, request *pb.RequestFromClient) (*pb.ResponseToClient, error) {
	log.Println("Client requested for summing", request.Array)
	var sum int64 = 0
	var id TaskID
	//if !request.Again {
	id = operServer.AddTask(request.Array)
	operServer.SendTaskToManager(operServer.Tasks[id].Task)
	fmt.Println("Task sent")
	// } else {
	// 	// for {
	// 	// 	operServer.mutex.Lock()
	// 	// 	if !operServer.AfterRecovering {
	// 	// 		operServer.mutex.Unlock()
	// 	// 		break
	// 	// 	}
	// 	// 	operServer.mutex.Unlock()
	// 	// }
	// 	for idT, tInfo := range operServer.TasksAfterRecovering {
	// 		if utils.Equal(tInfo.Task.Array, request.Array) {
	// 			id = idT
	// 			fmt.Println("task found after recovering")
	// 			break
	// 		}
	// 	}
	// 	fmt.Println(uuid.UUID(id).String())
	// }
	for {
		operServer.responseMutex.Lock()
		if response, isIn := operServer.Responses[id]; isIn {
			sum = response.Res
			operServer.responseMutex.Unlock()
			break
		}
		operServer.responseMutex.Unlock()
	}
	return &pb.ResponseToClient{Sum: sum}, nil
}

// getting responses from manager and adding them to the responses dictionary
func (operServer *operator) ListenManager(ctx context.Context) {
	go func() {
		<-ctx.Done()
	}() // сюда передается отдельный контекст, так что здесь можно вычитывать
	var err1 error
	//var conn net.Conn
	operServer.ManagerListener, err1 = net.Listen("tcp", operServer.ConfigData.Host+":"+operServer.ConfigData.ListenManagerOn)
	if err1 != nil {
		log.Fatalln("Failed to listen manager")
	}
	fmt.Println("Listening manager")
	operServer.ListeningConnToManager, _ = operServer.ManagerListener.Accept()
	defer operServer.ListeningConnToManager.Close()
	for {
		buffer := make([]byte, 1024)
		bytesRead, err := operServer.ListeningConnToManager.Read(buffer)
		if err != nil {
			if ctx.Err() == context.Canceled {
				return
			}
			// handle error
			continue
		}
		var respMsg cmpb.OperToManager
		proto.Unmarshal(buffer[:bytesRead], &respMsg)
		if !respMsg.AfterRecovering {
			operServer.AfterRecovering = false
			response := new(cmpb.Response)
			proto.Unmarshal(buffer[:bytesRead], response) // принимать сообщение о занятости воркера
			fmt.Println("ListenManager: ", respMsg.Type)
			if respMsg.Type == typeBusy {
				wID, _ := uuid.Parse(respMsg.ID)
				fmt.Println("Worker ", wID, " is busy")
				operServer.mutex.Lock()
				operServer.Info.Workers[WorkerID(wID)].IsBusy = true
				tID, _ := uuid.Parse(respMsg.TaskID)
				operServer.Info.Workers[WorkerID(wID)].Task = TaskID(tID)
				operServer.mutex.Unlock()
			} else if respMsg.Type == typeFree {
				wID, _ := uuid.Parse(respMsg.ID)
				fmt.Println("Worker ", wID, " is free")
				operServer.mutex.Lock()
				operServer.Info.Workers[WorkerID(wID)].IsBusy = false
				operServer.mutex.Unlock()
			} else { // when Response
				id, _ := uuid.Parse(response.ID)
				operServer.mutex.Lock()
				delete(operServer.Tasks, TaskID(id))
				operServer.mutex.Unlock()
				operServer.responseMutex.Lock()
				operServer.Responses[TaskID(id)] = response
				fmt.Println("ListenManager: response received: ", response.Res)
				operServer.responseMutex.Unlock()
			}
		} else {
			fmt.Println("afterRecovering!")
			// operServer.AfterRecovering = true
			// fmt.Println("ListenManager: ", respMsg.Type)
			// operServer.mutex.Lock()
			// operServer.afterRecovering()
			// operServer.mutex.Unlock()
			// operServer.AfterRecovering = false
		}
	}
}

func (operServer *operator) afterRecovering() {
	defer fmt.Println("afterRecovering finished")
	fmt.Println("afterRecovering: started")
	buffer := make([]byte, 1024)
	bytesRead, _ := operServer.ListeningConnToManager.Read(buffer)
	var msg cmpb.OperToManager
	proto.Unmarshal(buffer[:bytesRead], &msg)
	idM, _ := uuid.Parse(msg.ID)
	if operServer.Info.ManagerID != WorkerID(uuid.Nil) {
		log.Println("afterRecovering: someone tries to intercept")
		return
	}
	operServer.Info.ManagerHost = msg.WorkerHost
	operServer.Info.ManagerID = WorkerID(idM)
	operServer.Info.ManagerPort = msg.WorkerListenOn
	var workerConfigData *ConfigWorker = new(ConfigWorker)
	workerConfigData.Host = msg.WorkerHost
	workerConfigData.ListenOperatorOn = msg.WorkerListenOperatorOn
	workerConfigData.IsManager = true
	workerConfigData.ListenOn = msg.WorkerListenOn
	workerConfigData.ID = WorkerID(idM)
	operServer.Info.Workers[workerConfigData.ID] = workerConfigData
	operServer.Info.WorkersCount++
	operServer.ConnToManager, _ = net.Dial("tcp", workerConfigData.Host+":"+workerConfigData.ListenOperatorOn)
	for {
		fmt.Println("afterRecovering: wating info")
		bytesRead, err := operServer.ListeningConnToManager.Read(buffer)
		if err != nil {
			fmt.Println(err)
			return
		}
		var msg1 cmpb.OperToManager
		proto.Unmarshal(buffer[:bytesRead], &msg1)
		fmt.Println("afterRecovering: ", msg1.Type)
		if msg1.Type == typeReady {
			plug := []byte("")
			operServer.ConnToManager.Write(plug)
			break
		}
		wConfig := new(ConfigWorker)
		wConfig.Host = msg1.WorkerHost
		idW, _ := uuid.Parse(msg1.ID)
		wConfig.ID = WorkerID(idW)
		wConfig.IsBusy = msg1.IsBusy
		if msg1.IsBusy {
			idT, _ := uuid.Parse(msg1.TaskID)
			wConfig.Task = TaskID(idT)
		}
		wConfig.IsManager = false
		wConfig.ListenOn = msg1.WorkerListenOn
		wConfig.ListenOperatorOn = msg1.WorkerListenOperatorOn
		wConfig.TimeStart = time.Now() // костыль
		operServer.Info.WorkersCount++
		operServer.Info.Workers[wConfig.ID] = wConfig
		plug := []byte("")
		operServer.ConnToManager.Write(plug)
		fmt.Println("afterRecovering: written")
	}
	for {
		fmt.Println("afterRecovering: wating tasks")
		bytesRead, err := operServer.ListeningConnToManager.Read(buffer)
		if err != nil {
			return
		}
		task := new(cmpb.Task)
		proto.Unmarshal(buffer[:bytesRead], task)
		fmt.Println("afterRecovering: ", task.Type)
		if task.Type == typeReady {
			break
		}
		idT, _ := uuid.Parse(task.ID)
		operServer.Tasks[TaskID(idT)] = TaskInfo{Task: task}
		operServer.TasksAfterRecovering[TaskID(idT)] = TaskInfo{Task: task}
		plug := []byte("")
		operServer.ConnToManager.Write(plug)
	}

}

// accepting workers` connections
func (operServer *operator) AcceptingWorkers(ctx context.Context, workerListener net.Listener) {
	var conn net.Conn
	var err error
	go func() {
		<-ctx.Done()
	}()
	for {
		if ctx.Err() == context.Canceled {
			return
		}
		conn, err = workerListener.Accept()
		if err != nil {
			if ctx.Err() == context.Canceled {
				return
			}
			log.Println(err, "Failed to accept connecction")
		} else {
			err = operServer.handleWorker(conn) // connection closes inside this func
			if err != nil {
				log.Println("Failed to handle worker connection")
			}
		}

	}
}

// handling worker`s connection: analizing whether the worker is manager or just worker(iun this case sending msg about worker to manager)
// if a message to potential worker is unsuccessful the not nil err is returned
func (operServer *operator) handleWorker(conn net.Conn) error {
	defer conn.Close()
	workerAddr := conn.RemoteAddr().String()
	fmt.Println("Worker connected:", workerAddr)

	buffer := make([]byte, 1024)
	bytesRead, err := conn.Read(buffer)
	if err != nil {
		log.Println("handleWorker: Failed to read config from " + workerAddr)
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
		log.Println("handleWorker: Connection failed")
	}

	// if request.AfterRecovering {
	// 	operServer.mutex.Lock()
	// 	operServer.ConnToManager = connToWorker
	// 	operServer.Info.ManagerHost = workerConfigData.Host
	// 	operServer.Info.ManagerPort = workerConfigData.ListenOn
	// 	workerConfigData.TimeStart = time.Now()
	// 	operServer.Info.Workers[workerConfigData.ID] = workerConfigData
	// 	// idM, _ := uuid.Parse()
	// 	// operServer.Info.ManagerID =
	// 	operServer.mutex.Unlock()
	// }

	if operServer.Info.ManagerID == WorkerID(uuid.Nil) && !workerConfigData.IsManager {
		reply, err := proto.Marshal(&cmpb.ReplyToConnect{Type: "[WAIT]", Msg: "Still no manager in system\n Wait..."})
		connToWorker.Write(reply)
		fmt.Println("handleWorker: Worker disconnected:", workerAddr)
		return err
	}

	operServer.mutex.Lock()
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
	workerConfigData.TimeStart = time.Now()
	operServer.Info.Workers[workerConfigData.ID] = workerConfigData
	var operPort string = operServer.ConfigData.ListenWorkersOn
	if workerConfigData.IsManager {
		operPort = operServer.ConfigData.ListenManagerOn
	}
	reply, _ := proto.Marshal(&cmpb.ReplyToConnect{
		Type:        "[INFO]",
		ManagerHost: operServer.Info.ManagerHost,
		ManagerPort: operServer.Info.ManagerPort,
		OperPort:    operPort, // для менеджера здесь указан другой порт
		IsManager:   workerConfigData.IsManager,
		Msg:         msg,
		ID:          uuid.UUID(id).String(),
	})
	fmt.Println(reply)
	_, err = connToWorker.Write(reply)
	if err != nil { // надо как-то по-другому обработать
		fmt.Println("handleWorker: Failed to send reply to worker " + workerAddr)
	}
	if workerConfigData.IsManager {
		operServer.ConnToManager = connToWorker
		// fmt.Println("conn to manager saved")
		//operServer.changedManager <- true
	} else {
		connToWorker.Close()
		// operServer.ConnToManager, _ = net.Dial("tcp",  operServer.Info.ManagerHost+":"+operServer.Info.Workers[operServer.Info.ManagerID].ListenOperatorOn)
		msgToManager, _ := proto.Marshal(&cmpb.OperToManager{
			Type:                   "[INFO]",
			ID:                     uuid.UUID(workerConfigData.ID).String(),
			WorkerHost:             workerConfigData.Host,
			WorkerListenOn:         workerConfigData.ListenOn,
			WorkerListenOperatorOn: workerConfigData.ListenOperatorOn,
		})
		fmt.Println("send info about worker")
		operServer.ConnToManager.Write(msgToManager)
	}
	operServer.mutex.Unlock()
	fmt.Println("handleWorker: Worker disconnected:", workerAddr)
	return err
}

// ping workers and manager every second to check if they alive
func (operServer *operator) CheckWorkers(stopCtx context.Context, checkTime time.Duration) {
	timer := time.NewTimer(checkTime)
	for {
		<-timer.C
		timer.Stop()
		// for {
		// 	operServer.mutex.Lock()
		// 	if !operServer.AfterRecovering {
		// 		operServer.mutex.Unlock()
		// 		break
		// 	}
		// 	operServer.mutex.Unlock()
		// }
		if len(operServer.Info.Workers) != 0 {
			//operServer.mutex.Lock()
			for _, configWorker := range operServer.Info.Workers {
				if configWorker.ID != operServer.Info.ManagerID {
					fmt.Println("checkWorkers: check worker ", uuid.UUID(configWorker.ID).String())
					conn, err := net.Dial("tcp", configWorker.Host+":"+configWorker.ListenOperatorOn)
					if err != nil {
						operServer.mutex.Lock()
						msg, _ := proto.Marshal(&cmpb.OperToManager{
							Type: typeWorkerDead,
							ID:   uuid.UUID(configWorker.ID).String(),
						})
						operServer.ConnToManager.Write(msg)
						delete(operServer.Info.Workers, configWorker.ID)
						operServer.Info.WorkersCount--
						operServer.mutex.Unlock()
					} else {
						conn.Close()
					}
				} else {
					fmt.Println("checking manager")
					msg, _ := proto.Marshal(&cmpb.OperToManager{Type: typeCheck})
					_, err := operServer.ConnToManager.Write(msg)
					if err != nil {
						fmt.Println("manager is fool")
						operServer.mutex.Lock()
						operServer.ConnToManager.Close()
						delete(operServer.Info.Workers, operServer.Info.ManagerID)
						operServer.Info.WorkersCount--
						operServer.chooseManager()
						operServer.mutex.Unlock()
					}
				}
			}
			//operServer.mutex.Unlock()
		} else {
			fmt.Println("No workers")
		}
		timer = time.NewTimer(checkTime)
	}
}

// sending all workers info anbout new manager and sending tasks to new manager
func (operServer *operator) distribute() {
	// for _, task := range operServer.Tasks {
	// 	fmt.Println(task.Task.Array)
	// 	msg, _ := proto.Marshal(task.Task)
	// 	operServer.ConnToManager.Write(msg)
	// 	buffer := make([]byte, 1024)
	// 	operServer.ConnToManager.Read(buffer)
	// }
	for id, workerConfig := range operServer.Info.Workers {
		//if !(id == operServer.Info.ManagerID && !workerConfig.IsBusy) {
		conn, err := net.Dial("tcp", workerConfig.Host+":"+workerConfig.ListenOperatorOn)
		if err != nil {
			continue
		}
		defer conn.Close()
		msg, _ := proto.Marshal(&cmpb.ReplyToConnect{
			Type:        typeChange,
			ManagerHost: operServer.Info.ManagerHost,
			ManagerPort: operServer.Info.ManagerPort,
		})
		conn.Write(msg)
		var taskID string
		if workerConfig.IsBusy {
			taskID = uuid.UUID(workerConfig.Task).String()
		}
		msg, _ = proto.Marshal(&cmpb.OperToManager{
			Type:           typeInfo,
			WorkerHost:     workerConfig.Host,
			WorkerListenOn: workerConfig.ListenOn,
			ID:             uuid.UUID(id).String(),
			IsBusy:         workerConfig.IsBusy,
			TaskID:         taskID,
		})
		operServer.ConnToManager.Write(msg)
		//}
	}
	time.Sleep(2 * time.Second)
	for _, task := range operServer.Tasks {
		fmt.Println(task.Task.ID, task.Task.Array)
		msg1, _ := proto.Marshal(task.Task)
		operServer.ConnToManager.Write(msg1)
		buffer := make([]byte, 1024)
		_, err := operServer.ConnToManager.Read(buffer)
		if err != nil {
			fmt.Println(err)
		}
	}
	msg, _ := proto.Marshal(&cmpb.OperToManager{Type: typeReady})
	operServer.ConnToManager.Write(msg)
}

// if manager is dead, this function is called to choose new manager
func (operServer *operator) chooseManager() {
	for len(operServer.Info.Workers) == 0 {
		log.Println("chooseManager: No workers")
		return
	}
	fmt.Println("choosing new manager")
	currentTime := time.Now()
	var minBusy float64 = -1.0
	var minID WorkerID
	for id, workerConfig := range operServer.Info.Workers {
		fmt.Println(uuid.UUID(id).String())
		connToWorker, err := net.Dial("tcp", workerConfig.Host+":"+workerConfig.ListenOperatorOn)
		if err != nil {
			continue // ALARM CHANGE| IF NO WORKERS AT ALL
		}
		msg, _ := proto.Marshal(&cmpb.ReplyToConnect{Type: typeTime}) //&cmpb.OperWorker{Type: typeTime})
		connToWorker.Write(msg)
		buffer := make([]byte, 1024)
		bytesRead, _ := connToWorker.Read(buffer)
		var respMsg cmpb.ReplyToConnect //OperWorker
		proto.Unmarshal(buffer[:bytesRead], &respMsg)
		countTasks, _ := strconv.ParseInt(respMsg.Msg, 10, 32)
		business := float64(countTasks) / float64(currentTime.Sub(workerConfig.TimeStart).Nanoseconds())
		if minBusy == -1 || business < minBusy {
			minBusy = business
			minID = id
		}
		connToWorker.Close()
	}
	//operServer.changedManager <- true
	fmt.Println("New manager ", uuid.UUID(minID).String())
	connToWorker, err := net.Dial("tcp", operServer.Info.Workers[minID].Host+":"+operServer.Info.Workers[minID].ListenOperatorOn)
	if err != nil {
		// hadle error
	}
	buffer := make([]byte, 1024)
	msg, _ := proto.Marshal(&cmpb.ReplyToConnect{Type: typeOrder})
	connToWorker.Write(msg)
	fmt.Println("order sent")
	connToWorker.Read(buffer)
	operServer.ConnToManager = connToWorker
	operServer.Info.ManagerHost = operServer.Info.Workers[minID].Host
	operServer.Info.ManagerPort = operServer.Info.Workers[minID].ListenOn
	operServer.Info.ManagerID = minID
	msg, _ = proto.Marshal(&cmpb.ReplyToConnect{
		Type:     typeMInfo,
		OperPort: operServer.ConfigData.ListenManagerOn,
	})
	connToWorker.Write(msg)
	fmt.Println("manager info sent")
	bytesRead, _ := connToWorker.Read(buffer)
	var msg1 cmpb.ReplyToConnect
	proto.Unmarshal(buffer[:bytesRead], &msg1)
	fmt.Println("rady to accept manager")
	operServer.ListeningConnToManager, _ = operServer.ManagerListener.Accept()
	fmt.Println("ready to dustribute", msg1.Type)
	operServer.distribute()
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
	operServer.Tasks = make(map[TaskID]TaskInfo) //*cmpb.Task)
	operServer.TasksAfterRecovering = make(map[TaskID]TaskInfo)
	operServer.Responses = make(map[TaskID]*cmpb.Response)
	operServer.ConfigData = ConfigOperator{
		Host:            configData.Host,
		Port:            configData.Port,
		ListenWorkersOn: configData.ListenWorkersOn,
		ListenManagerOn: configData.ListenManagerOn,
		MaxTasks:        configData.MaxTasks,
		CheckTime:       configData.CheckTime,
	}

	workerListener, err := net.Listen("tcp", configData.Host+":"+configData.ListenWorkersOn)
	utils.HandleError(err, "Failed to listen port "+configData.ListenWorkersOn)
	defer workerListener.Close()

	stop := make(chan os.Signal, 1) // why buffered: .Notify requires buffered chan
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	ctxW, cancelW := context.WithCancel(context.Background())
	ctxM, cancelM := context.WithCancel(context.Background())
	//operServer.AfterRecovering = true
	go operServer.AcceptingWorkers(ctxW, workerListener)
	// operServer.ConnectToManager(&operServer.mutex)

	server := grpc.NewServer()
	pb.RegisterOperatorServer(server, &operServer)
	go func() {
		fmt.Println("Operator started")
		err = server.Serve(listener)
		utils.HandleError(err, "Failed to serve")
	}()

	go operServer.ListenManager(ctxM)
	go operServer.CheckWorkers(ctxW, time.Duration(operServer.ConfigData.CheckTime)*time.Millisecond)

	<-stop
	cancelW()
	cancelM()
	time.Sleep(500 * time.Millisecond)
	server.Stop()
	fmt.Println("\nOperator stopped")
}
