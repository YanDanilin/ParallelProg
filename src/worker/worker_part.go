package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	cmpb "github.com/YanDanilin/ParallelProg/communication"

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
		if stopCtx.Err() == context.Canceled {
			return
		}
		conn, err := worker.operListener.Accept()
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
			// handle error
		}
		// defer conn.Close()
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer) //worker.ConnToOper.Read(buffer)
		if err != nil {
			if stopCtx.Err() == context.Canceled {
				return
			}
			// handle error CONNECTION TO OPERATOR LOST !!!
		}
		var msg cmpb.ReplyToConnect
		proto.Unmarshal(buffer[:bytesRead], &msg)
		fmt.Println(msg.Type)
		if msg.Type == typeOrder {
			// become manager mb with contex? cancel?
			fmt.Println("order to become manager")
			worker.ConnToOper = conn
			conn.Write(buffer[:bytesRead])
			worker.managerListener.Close()
			changeToManager()
			//conn.Close()
			return
		} else if msg.Type == typeCheck {
			worker.ConnToOper.Write(buffer[:bytesRead])
		} else if msg.Type == typeTime {
			msg.Msg = strconv.FormatInt(int64(worker.TasksCount), 10)
			reply, _ := proto.Marshal(&msg)
			conn.Write(reply)
			//worker.ConnToOper.Write(reply)
		} else if msg.Type == typeChange {
			worker.mutex.Lock()
			worker.ManagerHost = msg.ManagerHost
			worker.ManagerPort = msg.ManagerPort
			worker.mutex.Unlock()
		} else {
			log.Println(typeCheck)
		}
		// } else if msg.Type == typeMInfo {
		// 	worker.mutex.Lock()
		// 	worker.Config.IsManager = true
		// 	worker.Config.OperatorPort = msg.OperPort
		// 	worker.ManagerPort = worker.Config.ListenOn
		// 	worker.ManagerHost = worker.Config.Host
		// 	//ready <- true
		// 	worker.mutex.Unlock()
		// 	conn.Close()
		// 	break
		// }
		conn.Close()
	}
}

func (worker *Worker) GettingTask(ctx context.Context, changeToManagerCtx context.Context) {
	defer fmt.Println("func is returned")
	var err error
	worker.managerListener, err = net.Listen("tcp", worker.Config.Host+":"+worker.Config.ListenOn)
	if err != nil {
		if ctx.Err() == context.Canceled {
			return
		}
		// handle error
	}
	fmt.Println(worker.Config.Host + ":" + worker.Config.ListenOn)
	fmt.Println("Listening")
	defer func() {
		if ctx.Err() == context.Canceled {
			fmt.Println("GettingTasks: closed managerListener")
			worker.managerListener.Close()
		}
	}()
	for {
		fmt.Println("getting task func")
		//if err != nil {
		if ctx.Err() == context.Canceled || changeToManagerCtx.Err() == context.Canceled {
			//conn.Close()
			fmt.Println("GettingTask: finished")
			return
		}
		// handle error
		//continue
		//}
		conn, err := worker.managerListener.Accept()
		fmt.Println("Accepted")
		if err != nil {
			if ctx.Err() == context.Canceled || changeToManagerCtx.Err() == context.Canceled {
				//conn.Close()
				return
			}
			// handle error
			worker.managerListener.Close()
			worker.managerListener, _ = net.Listen("tcp", worker.Config.Host+":"+worker.Config.ListenOn)
			fmt.Println("GettingTask: ", err)
			continue
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
		worker.TasksCount++
		response := worker.processingTask(&task)
		marshaledResponse, _ := proto.Marshal(&response)
		var connToManager net.Conn
		for {
			fmt.Println("sending resp")
			worker.mutex.Lock()
			connToManager, err = net.Dial("tcp", worker.ManagerHost+":"+worker.ManagerPort)
			worker.mutex.Unlock()
			if err != nil {
				// что делать, если менеджер не отвечает
				if changeToManagerCtx.Err() == context.Canceled {
					time.Sleep(3 * time.Second)
				} else {
					time.Sleep(10 * time.Millisecond)
				}
				continue
			}
			break
		}
		connToManager.Write(marshaledResponse)
		fmt.Println("response written")
		conn.Close()
		connToManager.Close()
	}
	//worker.managerListener.Close()
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
	// var err error
	// worker.ConnToOper, err = worker.operListener.Accept()
	// if err != nil {
	// 	// handle error
	// }
	var buffer []byte
	var bytesRead int
	for {
		fmt.Println("get manager info")
		buffer = make([]byte, 1024)
		bytesRead, _ = worker.ConnToOper.Read(buffer)
		var msg cmpb.ReplyToConnect
		proto.Unmarshal(buffer[:bytesRead], &msg)
		if msg.Type == typeMInfo {
			fmt.Println(msg.Type)
			worker.Config.OperatorPort = msg.OperPort
			worker.ManagerPort = worker.Config.ListenOn
			worker.ManagerHost = worker.Config.Host
			break
		}
	}
	worker.ConnToOper.Write(buffer[:bytesRead])

	worker.mutex.Unlock()
}

func (worker *Worker) ExecWorker(stopCtx context.Context, changeToManagerCtx context.Context, changeToManagerCancel context.CancelFunc) {
	fmt.Println("ExecWorker  func")
	//ready := make(chan bool)
	go worker.ConnectToOper(stopCtx, changeToManagerCancel)
	go worker.GettingTask(stopCtx, changeToManagerCtx)

	select {
	case <-stopCtx.Done():
	case <-changeToManagerCtx.Done():
		fmt.Println("becoming Manager")
		worker.getManagerInfo()
	}
}
