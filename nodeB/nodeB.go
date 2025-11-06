package main

import (
	proto "RequestCP/grpc"
	//"bufio"
	"os"
	"fmt"
	"log"
	"net"
	//"io/ioutil"
	//"strings"
	//"sync"

	"google.golang.org/grpc"
)

type CommServer struct {
	proto.UnimplementedRequestServiceServer
	nodes 		map[string]proto.RequestService_CommServer
	fileName 	string
	LCt     	int64
}

// NewCommServer initializes a new CommServer
func NewCommServer() *CommServer {
	return &CommServer{
		nodes: 		make(map[string]proto.RequestService_CommServer),
		fileName:	"../Database.txt",
		LCt:     	0,
	}
}

func main() {
	lis, err := net.Listen("tcp", ":5068")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	newComm := NewCommServer()
	proto.RegisterRequestServiceServer(server, newComm)

	newComm.LCt = newComm.LCt + 1

	fmt.Printf("L: %d - [Server] (Announcement) Comm server started on port 5068...\n", newComm.LCt)

	go func() {
		content, err := os.ReadFile(newComm.fileName)
		if err != nil {
			log.Fatalf("Failed to read database file: %v", err)
		}
		fmt.Println(string(content))
	}()

	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	

}