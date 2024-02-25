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

const (
	typeOrder  string = "[ORDER]"
	typeCheck  string = "[CHECK]"
	typeTime   string = "[TIME]"
	typeChange string = "[CHANGE]"
	typeMInfo  string = "[MANAGERINFO]"
)

func (worker *Worker) ConnectToOper(stopCtx context.Context, changeToManager context.CancelFunc) {
	for {
		// conn, err := worker.operListener.Accept()
		// if err != nil {
		// 	if stopCtx.Err() == context.Canceled {
		// 		return
		// 	}
		// 	// handle error
		// }
		buffer := make([]byte, 1024)
		bytesRead, err := worker.ConnToOper.Read(buffer)
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
			// handle error
		}
		var msg cmpb.ReplyToConnect
		proto.Unmarshal(buffer[:bytesRead], &msg)
		if msg.Type == typeOrder {
			// become manager mb with contex? cancel?
			changeToManager()
			//conn.Close()
			return
		} else if msg.Type == typeCheck {
			worker.ConnToOper.Write(buffer[:bytesRead])
		} else if msg.Type == typeTime {
			msg.Msg = strconv.FormatInt(int64(worker.TasksDone), 10)
			reply, _ := proto.Marshal(&msg)
			worker.ConnToOper.Write(reply)
		} else if msg.Type == typeChange {
			// отправлять дргуому менеджеру
			// мб передать функцию для подключению к другому менеджеру
		}
		// conn.Close()
	}
}

func (worker *Worker) GettingTask(ctx context.Context, changeToManagerCtx context.Context) {
	managerListener, err := net.Listen("tcp", worker.Config.Host+":"+worker.Config.ListenOn)
	if err != nil {
		if ctx.Err() == context.Canceled {
			return
		}
		// handle error
	}
	fmt.Println(worker.Config.Host + ":" + worker.Config.ListenOn)
	fmt.Println("Listening")
	defer managerListener.Close()
	for {
		conn, err := managerListener.Accept()
		fmt.Println("Accepted")
		if err != nil {
			if ctx.Err() == context.Canceled || changeToManagerCtx.Err() == context.Canceled {
				//conn.Close()
				return
			}
			// handle error
		}
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		if err != nil {
			if ctx.Err() == context.Canceled || changeToManagerCtx.Err() == context.Canceled {
				conn.Close()
				return
			}
			// handle error
		}
		task := cmpb.Task{}
		proto.Unmarshal(buffer[:bytesRead], &task)
		fmt.Println("Task is ready to be done")
		response := worker.processingTask(&task)
		marshaledResponse, _ := proto.Marshal(&response)
		var connToManager net.Conn
		for {
			worker.mutex.Lock()
			connToManager, err = net.Dial("tcp", worker.ManagerHost+":"+worker.ManagerPort)
			worker.mutex.Unlock()
			if err != nil {
				// что делать, если менеджер не отвечает
				continue
			}
			break
		}
		connToManager.Write(marshaledResponse)
		fmt.Println("response written")
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
	sleepTime := time.Duration(10000+rand.Intn(7000)) * time.Millisecond
	fmt.Println("Sleep after hard work for ", sleepTime)
	time.Sleep(sleepTime) // hard work is done
	return cmpb.Response{ID: task.ID, Res: sum}
}

func (worker *Worker) getManagerInfo() {
	worker.mutex.Lock()
	//conn, err := worker.operListener.Accept()
	// if err != nil {
	// 	// handle error
	// }
	buffer := make([]byte, 1024)
	bytesRead, _ := worker.ConnToOper.Read(buffer)
	var msg cmpb.ReplyToConnect
	proto.Unmarshal(buffer[:bytesRead], &msg)
	worker.Config.IsManager = true
	worker.Config.OperatorPort = msg.OperPort
	worker.ManagerPort = worker.Config.ListenOn
	worker.ManagerHost = worker.Config.Host
	worker.mutex.Unlock()
}

func (worker *Worker) ExecWorker(stopCtx context.Context, changeToManagerCtx context.Context, changeToManagerCancel context.CancelFunc) {
	fmt.Println("ExecWorker  func")
	go worker.ConnectToOper(stopCtx, changeToManagerCancel)
	go worker.GettingTask(stopCtx, changeToManagerCtx)

	select {
	case <-stopCtx.Done():
	case <-changeToManagerCtx.Done():
		worker.getManagerInfo()
	}
}
