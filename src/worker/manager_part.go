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

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	// "github.com/YanDanilin/ParallelProg/utils"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type Manager struct {
	Worker       *Worker
	WorkersInfo  map[WorkerID]*cmpb.OperToManager
	WorkersCount int32
	Tasks        *list.List
}

func (manager *Manager) ConnectToOper() {
	for {
		conn, err := manager.Worker.operListener.Accept()
		if err != nil {
			// handle error
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
			if workerInfo.Type == "[INFO]" {
				manager.Worker.mutex.Lock()
				manager.WorkersInfo[WorkerID(id)] = workerInfo
				manager.WorkersCount++
				manager.Worker.mutex.Unlock()
			} else if workerInfo.Type == "[WORKERDEAD]" {
				manager.Worker.mutex.Lock()
				delete(manager.WorkersInfo, WorkerID(id))
				manager.WorkersCount--
				manager.Worker.mutex.Unlock()
			}
		}
	}
}

func (worker *Worker) ExecManager(ctx context.Context) {
	manager := Manager{
		Worker:       worker,
		WorkersInfo:  make(map[WorkerID]*cmpb.OperToManager),
		WorkersCount: 0,
		Tasks:        list.New(),
	}
	connToOper, err := net.Dial("tcp", worker.Config.OperatorHost+":"+worker.Config.OperatorPort)
	go manager.ConnectToOper()

	<-ctx.Done()
}
