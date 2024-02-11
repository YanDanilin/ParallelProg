package main

import (
	// "net"
	// "os"
	// "os/signal"
	// "syscall"
	// "time"

	// "os"
	// "log"
	"context"
	// "container/list"

	cmpb "github.com/YanDanilin/ParallelProg/communication"
	// "github.com/YanDanilin/ParallelProg/utils"
	// "github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func (worker *Worker) ConnectToOper() {
	for {
		conn, err := worker.operListener.Accept()
		if err != nil {
			// handle error
		}
		buffer := make([]byte, 1024)
		bytesRead, err := conn.Read(buffer)
		var msg cmpb.OperWorker
		proto.Unmarshal(buffer[:bytesRead], &msg)
		if msg.Type == "[ORDER]" {
			// become manager	
		} else if msg.Type ==  "[CHECK]" {
			conn.Write(buffer[:bytesRead])
		}
		conn.Close()
	}
}

func (worker *Worker) ExecWorker(ctx context.Context) {


	<-ctx.Done()
}