package main

import (
	"errors"
	"fmt"
	"net"
	"time"

	// "os"
	// "os/signal"
	// "syscall"
	// "time"
	"container/list"
	"context"
	"log"

	//"math/rand"

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	"github.com/YanDanilin/ParallelProg/utils"

	// "github.com/YanDanilin/ParallelProg/utils"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const (
	typeInfo       string = "[INFO]"
	typeWorkerDead string = "[WORKERDEAD]"
	typeBusy       string = "[BUSY]"
	typeFree       string = "[FREE]"
	typeTask       string = "[TASK]"
	typeReady      string = "[READY]"
)

type TaskID uuid.UUID

type WInfo struct {
	OperToManager *cmpb.OperToManager
	Task          *cmpb.Task
}

type Manager struct {
	Worker       *Worker
	WorkersInfo  map[WorkerID]*WInfo
	FreeWorkers  map[WorkerID]struct{}
	BusyWorkers  map[TaskID]WorkerID //*WInfo
	WorkersCount int32
	Tasks        *list.List
	ConnToOper   net.Conn
}

func (manager *Manager) tryConnect() (err error) {
	manager.Worker.ConnToOper, err = net.Dial("tcp", manager.Worker.Config.OperatorHost+":"+manager.Worker.Config.OperatorPort)
	// write replyToConnect
	return err
}

func (manager *Manager) ConnectToOper(stopCtx context.Context) {
	log.Println("Conn to oper")
	//conn, err := manager.Worker.operListener.Accept()
	for {
		// if err != nil {
		// 	// handle error
		// 	fmt.Println(err, "connToOper func")
		// 	if stopCtx.Err() == context.Canceled {
		// 		return
		// 	}
		// }
		// fmt.Println("Accepted")
		buffer := make([]byte, 1024)
		bytesRead, err := manager.Worker.ConnToOper.Read(buffer)
		for err != nil {
			// handle error
			if stopCtx.Err() == context.Canceled {
				return
			}
			log.Println("Connection to operator lost")
			time.Sleep(5 * time.Second)
			err = manager.tryConnect()
			//utils.HandleError(err, "Connection to opeator lost")
		}
		workerInfo := new(cmpb.OperToManager)
		proto.Unmarshal(buffer[:bytesRead], workerInfo)
		task := new(cmpb.Task)
		proto.Unmarshal(buffer[:bytesRead], task)
		if task.Type == typeTask {
			manager.Worker.mutex.Lock()
			manager.Tasks.PushBack(task)
			fmt.Println("ConnectToOper: task added ", task.Array)
			//msg := []byte("")
			//manager.Worker.ConnToOper.Write(msg)
			manager.Worker.mutex.Unlock()
		} else {
			log.Println(workerInfo.Type)
			id, _ := uuid.Parse(workerInfo.ID)
			idW := WorkerID(id)
			if workerInfo.Type == typeInfo {
				tID, _ := uuid.Parse(workerInfo.TaskID)
				manager.Worker.mutex.Lock()
				if idW != manager.Worker.MyID {
					wInfo := new(WInfo)
					wInfo.OperToManager = workerInfo
					wInfo.Task = nil
					manager.WorkersInfo[idW] = wInfo
					manager.WorkersCount++
				}
				if !workerInfo.IsBusy {
					manager.FreeWorkers[idW] = struct{}{}
				} else {
					fmt.Println("ConnectToOper: ", idW)
					manager.BusyWorkers[TaskID(tID)] = idW //wInfo
				}
				manager.Worker.mutex.Unlock()
			} else if workerInfo.Type == typeWorkerDead {
				fmt.Println("ConnectToOper: ", workerInfo.Type)
				manager.Worker.mutex.Lock()
				if wInfo := manager.WorkersInfo[idW]; wInfo.Task != nil {
					manager.Tasks.PushFront(wInfo.Task)
				}
				delete(manager.WorkersInfo, idW)
				manager.WorkersCount--
				// if _, isIn := manager.FreeWorkers[idW]; isIn {
				delete(manager.FreeWorkers, idW)
				// }
				manager.Worker.mutex.Unlock()
			} else if workerInfo.Type == typeCheck {
				continue
			}
		}
	}
}

func (manager *Manager) findFreeWorker() (net.Conn, WorkerID) {
	//var l int
	// manager.Worker.mutex.Lock()
	for len(manager.FreeWorkers) == 0 {
		fmt.Println("findFreeWorker: empty free workers list")
		// manager.Worker.
		time.Sleep(500 * time.Millisecond)
	}
	// num := rand.Intn(l)
	// i := 0
	var idRet WorkerID
	var conn net.Conn
	var err error
	// Loop:
	// 	for {
	// 		num := rand.Intn(l)
	// 		i := 0

	manager.Worker.mutex.Lock()
	// for len(manager.FreeWorkers) == 0 {
	// 	manager.Worker.mutex.Unlock()
	// 	fmt.Println("finding free worker")
	// 	time.Sleep(5 * time.Second)
	// 	manager.Worker.mutex.Lock()
	// }
	for err = errors.New("plug"); err != nil; {
		for id := range manager.FreeWorkers {
			// if i == num {
			fmt.Println("findFreeWorker: ", manager.WorkersInfo[id].OperToManager.WorkerHost+":"+manager.WorkersInfo[id].OperToManager.WorkerListenOn)
			conn, err = net.Dial("tcp", manager.WorkersInfo[id].OperToManager.WorkerHost+":"+manager.WorkersInfo[id].OperToManager.WorkerListenOn)
			if err != nil {
				// handle error
				fmt.Println("findFreeWorker: Connection to worker failed", err)
				continue
			}
			idRet = id
			fmt.Println("findFreeWorker: free worker found")
			break
			// break Loop
			// }
			// 		i++
			// 	}
		}
	}
	manager.Worker.mutex.Unlock()
	return conn, idRet
}

func (manager *Manager) SendTaskToWorker(stopCtx context.Context) {
	for {
		if manager.Tasks.Len() != 0 {
			if stopCtx.Err() == context.Canceled {
				return
			}
			manager.Worker.mutex.Lock()
			taskFromList := manager.Tasks.Front()
			if taskFromList != nil {
				manager.Tasks.Remove(taskFromList)
				manager.Worker.mutex.Unlock()
				var task *cmpb.Task = taskFromList.Value.(*cmpb.Task)
				// fmt.Println(task.Array)
				msg, _ := proto.Marshal(task)
				fmt.Println("SendTaskToWorker: befose find ", task.Array)
				conn, id := manager.findFreeWorker()
				conn.Write(msg)
				conn.Close()
				fmt.Println("SendTaskToWorker: task sent ", task.Array)
				manager.Worker.mutex.Lock()
				manager.WorkersInfo[id].Task = task
				taskID, _ := uuid.Parse(task.ID)
				manager.BusyWorkers[TaskID(taskID)] = id //manager.WorkersInfo[id]
				delete(manager.FreeWorkers, id)
				msg, _ = proto.Marshal(&cmpb.OperToManager{Type: typeBusy, ID: uuid.UUID(id).String(), TaskID: taskID.String()})
				manager.ConnToOper.Write(msg)
				manager.Worker.mutex.Unlock()
			} else {
				manager.Worker.mutex.Unlock()
			}
		}
	}
}

func (manager *Manager) GetResponses(stopCtx context.Context) {
	var err error
	//workerListener,
	fmt.Println("GetResponses: started")
	manager.Worker.managerListener, err = net.Listen("tcp", manager.Worker.ManagerHost+":"+manager.Worker.ManagerPort)
	if err != nil {
		// handle error
		if stopCtx.Err() == context.Canceled {
			return
		}
		fmt.Println("GetResponses: ", err)
		if err.Error() == "listen tcp "+manager.Worker.ManagerHost+":"+manager.Worker.ManagerPort+": bind: address already in use" {
			manager.Worker.managerListener.Close()
			manager.Worker.managerListener, err = net.Listen("tcp", manager.Worker.ManagerHost+":"+manager.Worker.ManagerPort)
			fmt.Println("GetResponse: ", err)
		}
	}
	fmt.Println("GetResponses: ", "Listening responses")
	fmt.Println("GetResponses: ", manager.Worker.ManagerHost+":"+manager.Worker.ManagerPort)
	defer manager.Worker.managerListener.Close()
	for {
		fmt.Println("GetResponses: ", "waiting for response")
		conn, err := manager.Worker.managerListener.Accept()
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
			/// handle error
		}
		defer conn.Close()
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
			utils.HandleError(err, "GetResponse: Failed to read response")
			//handle error
		}
		manager.ConnToOper.Write(buffer[:bytesRead])
		response := new(cmpb.Response)
		proto.Unmarshal(buffer[:bytesRead], response)
		uuID, _ := uuid.Parse(response.ID)
		manager.Worker.mutex.Lock()
		wInfo, isIn := manager.BusyWorkers[TaskID(uuID)]
		//manager.Worker.mutex.Unlock()
		// for !isIn {
		// 	fmt.Println("GetResponses: cycle ", response.ID)
		// 	manager.Worker.mutex.Lock()
		// 	wInfo, isIn = manager.BusyWorkers[TaskID(uuID)]
		// 	manager.Worker.mutex.Unlock()
		// 	time.Sleep(100 * time.Millisecond)
		// }
		//manager.Worker.mutex.Lock()
		if isIn {
			var wID uuid.UUID = uuid.UUID(wInfo)
			//if wInfo != nil {
			//wID = uuid.UUID(wInfo) //, err = uuid.Parse(wInfo)//wInfo.OperToManager.ID)
			if err != nil {
				fmt.Println("GetResponse:", err)
			}
			if WorkerID(wID) != manager.Worker.MyID {
				fmt.Println("GetResponse: free worker added ", wID)
				manager.FreeWorkers[WorkerID(wID)] = struct{}{}
			}
			delete(manager.BusyWorkers, TaskID(uuID))
			//} else {
			// wID = uuid.UUID(manager.Worker.MyID)
			//}
			msg, _ := proto.Marshal(&cmpb.OperToManager{Type: typeFree, ID: wID.String()}) //wInfo.OperToManager.ID}) //uuid.UUID(wID).String()})

			manager.ConnToOper.Write(msg)
		}
		manager.Worker.mutex.Unlock()
		// go func() {
		//manager.ConnToOper.Write(buffer[:bytesRead])
		// }()
	}
}

func (worker *Worker) ExecManager(stopCtx context.Context) {
	manager := Manager{
		Worker:       worker,
		WorkersInfo:  make(map[WorkerID]*WInfo),
		WorkersCount: 0,
		FreeWorkers:  make(map[WorkerID]struct{}),
		BusyWorkers:  make(map[TaskID]WorkerID), //*WInfo),
		Tasks:        list.New(),
	}
	var err error

	manager.ConnToOper, err = net.Dial("tcp", worker.Config.OperatorHost+":"+worker.Config.OperatorPort)
	if err != nil {
		utils.HandleError(err, "ExecManager: Failed to connect to operator")
	}
	if !worker.Config.IsManager {
		for {

			buffer := make([]byte, 1024)
			bytesRead, err := manager.Worker.ConnToOper.Read(buffer)
			for err != nil {
				// handle error
				if stopCtx.Err() == context.Canceled {
					return
				}
				log.Println("ExecManager: Connection to operator lost")
				time.Sleep(5 * time.Second)
				err = manager.tryConnect()
				//utils.HandleError(err, "Connection to opeator lost")
			}
			workerInfo := new(cmpb.OperToManager)
			proto.Unmarshal(buffer[:bytesRead], workerInfo)
			task := new(cmpb.Task)
			proto.Unmarshal(buffer[:bytesRead], task)
			if task.Type == typeTask {
				manager.Worker.mutex.Lock()
				tID, _ := uuid.Parse(task.ID)
				if _, isIn := manager.BusyWorkers[TaskID(tID)]; !isIn {
					manager.Tasks.PushBack(task)
				}
				fmt.Println("ExecManager: task added ", task.ID, task.Array)
				msg := []byte("a")
				manager.Worker.ConnToOper.Write(msg)
				fmt.Println("ExecManager: written")
				manager.Worker.mutex.Unlock()
			} else {
				log.Println(workerInfo.Type)
				id, _ := uuid.Parse(workerInfo.ID)
				idW := WorkerID(id)
				if workerInfo.Type == typeInfo {
					tID, _ := uuid.Parse(workerInfo.TaskID)
					manager.Worker.mutex.Lock()
					if idW != manager.Worker.MyID {
						wInfo := new(WInfo)
						wInfo.OperToManager = workerInfo
						wInfo.Task = nil
						manager.WorkersInfo[idW] = wInfo
						manager.WorkersCount++
					}
					if !workerInfo.IsBusy {
						fmt.Println("ExecManager: free: ", uuid.UUID(idW).String())
						manager.FreeWorkers[idW] = struct{}{}
					} else {
						fmt.Println("ExecManager: busy: ", uuid.UUID(idW).String())
						manager.BusyWorkers[TaskID(tID)] = idW //wInfo
					}
					manager.Worker.mutex.Unlock()
				} else if workerInfo.Type == typeReady {
					break
				}
			}
		}
	}
	defer manager.ConnToOper.Close()
	go manager.ConnectToOper(stopCtx)
	go manager.SendTaskToWorker(stopCtx)
	go manager.GetResponses(stopCtx)
	<-stopCtx.Done()
}
