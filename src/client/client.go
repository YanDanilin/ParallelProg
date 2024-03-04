package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/YanDanilin/ParallelProg/protobuf"
	"github.com/YanDanilin/ParallelProg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var configFilePathFlag = flag.String("configPath", "./src/client/config.json", "path to configuration file for client")

type ConfigClientJSON struct {
	Host string
	Port string
}

type ConfigStruct struct {
	ConfigClientJSON
}

func readArray(input string) ([]int32, error) {
	nums := strings.Split(input, " ")
	var resArray []int32 = make([]int32, 0, 20)
	for _, numStr := range nums {
		num, err := strconv.ParseInt(numStr, 10, 32)
		if err != nil {
			return []int32{}, err
		}
		resArray = append(resArray, int32(num))
	}
	return resArray, nil
}

func main() {
	flag.Parse()
	var configData ConfigStruct
	err := utils.DecodeConfigJSON(*configFilePathFlag, &configData)
	utils.HandleError(err, "Failed to get config parametrs")

	conn, err := grpc.Dial(configData.Host+":"+configData.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	utils.HandleError(err, "Failed to connect to operator server")
	defer conn.Close()
	log.Println("Successful connection to operator")
	client := pb.NewOperatorClient(conn)

	reader := bufio.NewScanner(os.Stdin)
	var requestCount int32
	for reader.Scan() {
		var input string = reader.Text()
		if input == "quit" || input == "exit" {
			break
		}
		array, err := readArray(input)
		if err != nil {
			log.Println("Failed to read inputed data")
			continue
		}
		requestCount++
		fmt.Println(requestCount, "-", array)
		go func(req int32, arr []int32) {
			response, err := client.ProcessRequest(context.Background(), &pb.RequestFromClient{Array: arr, Again: false})

			for err != nil {
				log.Println("Connection to operator lost")
				fmt.Println("Trying again task", arr)
				time.Sleep(3 * time.Second)
				// вынести подключение в отдельную функцию
				response, err = client.ProcessRequest(context.Background(), &pb.RequestFromClient{Array: arr, Again: true})
			}
			fmt.Println("  ", req, "-", response)
		}(requestCount, array)
	}

	fmt.Println("Client stopped working")
}
