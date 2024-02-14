package main

import (
	"net"
	// "os"
	// "os/signal"
	// "syscall"
	// "time"

	// "os"
	// "log"
	"container/list"
	"context"
	"math/rand"

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	"github.com/YanDanilin/ParallelProg/utils"

	// "github.com/YanDanilin/ParallelProg/utils"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
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
	BusyWorkers  map[TaskID]*WInfo
	WorkersCount int32
	Tasks        *list.List
	ConnToOper   net.Conn
}

func (manager *Manager) ConnectToOper(stopCtx context.Context) {
	for {
		conn, err := manager.Worker.operListener.Accept()
		if err != nil {
			// handle error
			if stopCtx.Err() == context.Canceled {
				return
			}
		}
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		workerInfo := new(cmpb.OperToManager)
		err = proto.Unmarshal(buffer[:bytesRead], workerInfo)
		if err != nil {
			task := new(cmpb.Task)
			proto.Unmarshal(buffer[:bytesRead], task)
			manager.Worker.mutex.Lock()
			manager.Tasks.PushBack(task)
			manager.Worker.mutex.Unlock()
		} else {
			id, _ := uuid.Parse(workerInfo.ID)
			idW := WorkerID(id)
			if workerInfo.Type == "[INFO]" {
				wInfo := new(WInfo)
				wInfo.OperToManager = workerInfo
				wInfo.Task = nil
				manager.Worker.mutex.Lock()
				manager.WorkersInfo[idW] = wInfo
				manager.WorkersCount++
				manager.FreeWorkers[idW] = struct{}{}
				manager.Worker.mutex.Unlock()
			} else if workerInfo.Type == "[WORKERDEAD]" {
				manager.Worker.mutex.Lock()
				if wInfo, _ := manager.WorkersInfo[idW]; wInfo.Task != nil {
					manager.Tasks.PushFront(wInfo.Task)
				}
				delete(manager.WorkersInfo, idW)
				manager.WorkersCount--
				if _, isIn := manager.FreeWorkers[idW]; isIn {
					delete(manager.FreeWorkers, idW)
				}
				manager.Worker.mutex.Unlock()
			}
		}
	}
}

func (manager *Manager) findFreeWorker() (net.Conn, WorkerID) {
	var l int
	for l = len(manager.FreeWorkers); l == 0; {
	}
	num := rand.Intn(l)
	i := 0
	var idRet WorkerID
	var conn net.Conn
	var err error
	for id, _ := range manager.FreeWorkers {
		if i == num {
			conn, err = net.Dial("tcp", manager.WorkersInfo[id].OperToManager.WorkerHost+":"+manager.WorkersInfo[id].OperToManager.WorkerListenOn)
			if err != nil {
				// handle error
			}
			idRet = id
			break
		}
		i++
	}
	return conn, idRet
}

func (manager *Manager) SendTaskToWorker(stopCtx context.Context) {
	for {
		if stopCtx.Err() == context.Canceled {
			return
		}
		manager.Worker.mutex.Lock()
		taskFromList := manager.Tasks.Front()
		if taskFromList != nil {
			manager.Tasks.Remove(taskFromList)
			manager.Worker.mutex.Unlock()
			var task *cmpb.Task = taskFromList.Value.(*cmpb.Task)
			msg, _ := proto.Marshal(task)
			conn, id := manager.findFreeWorker()
			conn.Write(msg)
			conn.Close()
			manager.Worker.mutex.Lock()
			manager.WorkersInfo[id].Task = task
			taskID, _ := uuid.Parse(task.ID)
			manager.BusyWorkers[TaskID(taskID)] = manager.WorkersInfo[id]
			delete(manager.FreeWorkers, id)
			manager.Worker.mutex.Unlock()
		} else {
			manager.Worker.mutex.Unlock()
		}
	}
}

func (manager *Manager) GetResponses(stopCtx context.Context) {
	workerListener, err := net.Listen("tcp", manager.Worker.ManagerHost+":"+manager.Worker.ManagerPort)
	if err != nil {
		// handle error
		if stopCtx.Err() == context.Canceled {
			return
		}
	}
	defer workerListener.Close()
	for {
		conn, err := workerListener.Accept()
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
		}
		defer conn.Close()
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
			//handle error
		}
		response := new(cmpb.Response)
		proto.Unmarshal(buffer[:bytesRead], response)
		uuID, _ := uuid.Parse(response.ID)
		manager.Worker.mutex.Lock()
		wInfo := manager.BusyWorkers[TaskID(uuID)]
		delete(manager.BusyWorkers, TaskID(uuID))
		wID, _ := uuid.Parse(wInfo.OperToManager.ID)
		manager.FreeWorkers[WorkerID(wID)] = struct{}{}
		manager.Worker.mutex.Unlock()
		go func() {
			manager.ConnToOper.Write(buffer[:bytesRead])
		}()
	}
}

func (worker *Worker) ExecManager(stopCtx context.Context) {
	manager := Manager{
		Worker:       worker,
		WorkersInfo:  make(map[WorkerID]*WInfo),
		WorkersCount: 0,
		FreeWorkers:  make(map[WorkerID]struct{}),
		Tasks:        list.New(),
	}
	var err error
	manager.ConnToOper, err = net.Dial("tcp", worker.Config.OperatorHost+":"+worker.Config.OperatorPort)
	if err != nil {
		utils.HandleError(err, "Failed to connect to operator")
	}
	defer manager.ConnToOper.Close()
	go manager.ConnectToOper(stopCtx)
	go manager.SendTaskToWorker(stopCtx)
	<-stopCtx.Done()
}
