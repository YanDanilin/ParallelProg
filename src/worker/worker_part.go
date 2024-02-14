package main

import (
	"net"
	"strconv"

	// "os"
	// "os/signal"
	// "syscall"
	"fmt"
	"math/rand"
	"time"

	// "os"
	// "log"
	"context"
	// "container/list"

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	// "github.com/YanDanilin/ParallelProg/utils"
	// "github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func (worker *Worker) ConnectToOper(stopCtx context.Context, changeToManager context.CancelFunc) {
	for {
		conn, err := worker.operListener.Accept()
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
			// handle error
		}
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		var msg cmpb.ReplyToConnect
		proto.Unmarshal(buffer[:bytesRead], &msg)
		if msg.Type == "[ORDER]" {
			// become manager mb with contex? cancel?
			changeToManager()
			conn.Close()
			return
		} else if msg.Type == "[CHECK]" {
			conn.Write(buffer[:bytesRead])
		} else if msg.Type == "[TIME]" {
			msg.Msg = strconv.FormatInt(int64(worker.TasksDone), 10)
			reply, _ := proto.Marshal(&msg)
			conn.Write(reply)
		} else if msg.Type == "[CHANGE]" {
			// отправлять дргуому менеджеру
			// мб передать функцию для подключению к другому менеджеру
		}
		conn.Close()
	}
}

func (worker *Worker) GettingTask(ctx context.Context) {
	managerListener, err := net.Listen("tcp", worker.Config.Host+":"+worker.Config.ListenOn)
	defer managerListener.Close()
	if err != nil {
		// handle error
	}
	for {
		conn, err := managerListener.Accept()
		if err != nil {
			if ctx.Err() == context.Canceled {
				conn.Close()
				return
			}
			// handle error
		}
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		task := cmpb.Task{}
		err = proto.Unmarshal(buffer[:bytesRead], &task)
		response := worker.processingTask(&task)
		marshaledResponse, _ := proto.Marshal(&response)
		worker.mutex.Lock()
		connToManager, err := net.Dial("tcp", worker.ManagerHost+":"+worker.ManagerPort)
		worker.mutex.Unlock()
		if err != nil {
			// что делать, елси менеджер не отвечает
		}
		connToManager.Write(marshaledResponse)
		conn.Close()
		connToManager.Close()
	}
}

func (worker *Worker) processingTask(task *cmpb.Task) cmpb.Response {
	var sum int64
	fmt.Println("Summing array ", task.Array)
	for _, elem := range task.Array {
		sum += int64(elem)
	}
	time.Sleep(time.Duration(3000+rand.Intn(7000)) * time.Millisecond) // hard work is done
	return cmpb.Response{ID: task.ID, Res: sum}
}

func (worker *Worker) ExecWorker(stopCtx context.Context, changeToManagerCtx context.Context, changeToManagerCancel context.CancelFunc) {
	go worker.ConnectToOper(stopCtx, changeToManagerCancel)
	go worker.GettingTask(stopCtx)

	select {
	case <-stopCtx.Done():
	case <-changeToManagerCtx.Done():
	}
}
