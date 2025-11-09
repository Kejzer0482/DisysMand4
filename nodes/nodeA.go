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
	servers         map[string]grpc.BidiStreamingClient[proto.RequestMessage, proto.RequestMessage]
	fileName 		string
	mutex			sync.Mutex
	state 			string
	deferred		[]string
	waitingRep		[]string
	LCt     		int64
}

// NewCommServer initializes a new CommServer
func NewCommServer() *CommServer {
	return &CommServer{
		username: 	"localhost:5069",
		clients: 	make(map[string]proto.RequestService_CommServer),
		servers:	make(map[string]grpc.BidiStreamingClient[proto.RequestMessage, proto.RequestMessage]),
		fileName:	"../Database.txt",
		mutex:		sync.Mutex{},
		state: 		"RELEASED",
		deferred:	[]string{},
		waitingRep:	[]string{},
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
	delete(s.servers, username)
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
	
	for node := range s.servers {
		stream, err := s.servers[node]
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

func (s *CommServer) checkConnections() {
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
		if address == s.username {
			continue // Skip connecting to itself
		}
		if _, exists := s.servers[address]; exists {
			continue // Skip if address is in servers map
		}

		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Node at %s failed to connect", address)
		} else {
			client := proto.NewRequestServiceClient(conn)
			stream, err := client.Comm(context.Background())
			if err != nil {
				log.Fatalf("Failed to create stream: %v", err)
			}
			fmt.Printf("Connected to node at %s\n", address)

			s.servers[address] = stream

			// Start a goroutine to listen for messages from this node
			go func(address string, stream grpc.BidiStreamingClient[proto.RequestMessage, proto.RequestMessage]) {
				for {
					msg, err := stream.Recv()
					if err != nil {
						log.Printf("Error receiving message from %s: %v", address, err)
						return
					}
					switch msg.Message {
					case "REQUEST":
						s.handleRequest(msg)
					case "REPLY":
						s.handleReply(msg)
					}
				}
			}(address, stream)
			defer conn.Close()
		}
	}
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

	

	// Listen to other nodes

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