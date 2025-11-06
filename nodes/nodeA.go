package main

import (
	proto "RequestCP/grpc"
	"bufio"
	"os"
	"fmt"
	"log"
	"net"
	"context"
	//"io/ioutil"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CommServer struct {
	proto.UnimplementedRequestServiceServer
	username 		string
	clients 		map[string]proto.RequestService_CommServer
	servers         map[string]proto.RequestServiceClient
	fileName 		string
	mutex			sync.Mutex
	state 			string
	deferred		[]string
	LCt     		int64
}

// NewCommServer initializes a new CommServer
func NewCommServer() *CommServer {
	return &CommServer{
		username: 	"Node A",
		clients: 		make(map[string]proto.RequestService_CommServer),
		fileName:	"../Database.txt",
		mutex:		sync.Mutex{},
		state: 		"RELEASED",
		deferred:	[]string{},
		LCt:     	0,
	}
}

func (s *CommServer) Comm(stream proto.RequestService_CommServer) error {
	// Implementation of the Comm method
	var username string

	// Receive messages from the client
	for {
		msg, err := stream.Recv()

		// Handle disconnection
		if err != nil {
			s.removeClient(username)
			msg = &proto.RequestMessage{ // Makes a new msg as it is nil when user disconnects so we can announce it
				Username:  username,
				Message:   "disconnected",
				Timestamp: 0,
			}

			// Update Logical Clock
			s.mutex.Lock()
			s.LCt = s.LCt + 1
			msg.Timestamp = s.LCt
			s.mutex.Unlock()

			return err
		}

		// Update Logical Clock
		s.mutex.Lock()
		s.LCt = max(s.LCt, msg.Timestamp) + 1
		msg.Timestamp = s.LCt
		s.mutex.Unlock()
	}
}

// Add client to the list
func (s *CommServer) addClient(username string, stream proto.RequestService_CommServer) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.clients[username] = stream
}

// Remove client from the list
func (s *CommServer) removeClient(username string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.clients, username)
}

func (s *CommServer) handleRequest(msg *proto.RequestMessage) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
}

func (s *CommServer) handleReply(msg *proto.RequestMessage) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
}

func (s *CommServer) sendRequest(msg *proto.RequestMessage) {
	s.mutex.Lock()
	s.state = "WANTED"
	s.LCt++
	timestamp := s.LCt
	s.mutex.Unlock()

	reqMsg := &proto.RequestMessage{
			Username:  s.username,
			Message:   "REQUEST",
			Timestamp: timestamp,
	}
	
	for node := range s.clients {
		stream, err := s.clients[node]
		if err{
			log.Printf("Error getting stream for node %s: %v", node, err)
			continue
		}
		stream.Send(reqMsg)
	}

}

func (s *CommServer) sendReply(msg *proto.RequestMessage) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
}

func (s *CommServer) enterCriticalSection() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
}



func main() {
	lis, err := net.Listen("tcp", ":5069")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	newComm := NewCommServer()
	proto.RegisterRequestServiceServer(server, newComm)

	newComm.LCt = newComm.LCt + 1

	fmt.Printf("L: %d - [Server] (Announcement) Comm server started on port 5069...\n", newComm.LCt)

	// Read Nodes.txt to get addresses and add each line to an array entry
	nodeArray := []string{}
	file, err := os.Open("../Nodes.txt")
	if err != nil {
		log.Fatalf("Failed to open Nodes.txt: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		nodeArray = append(nodeArray, line)
	}

	for node := range nodeArray {
		fmt.Printf("Node %d: %s\n", node, nodeArray[node])
	}

	// Connect to other nodes
	for _, address := range nodeArray {
		if address == "localhost:5069" {
			continue // Skip connecting to itself
		}
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		client := proto.NewRequestServiceClient(conn)
		stream, err := client.Comm(context.Background())
		if err != nil {
			log.Fatalf("Failed to create stream: %v", err)
		}
		fmt.Printf("Connected to node at %s\n", address)
	}

	go func() {
		for {

			content, err := os.ReadFile(newComm.fileName)
			if err != nil {
				log.Fatalf("Failed to read database file: %v", err)
			}
			fmt.Println(string(content))
			time.Sleep(2 * time.Second)
		}
		
	}()

	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	

}