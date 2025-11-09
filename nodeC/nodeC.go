package main

import (
	proto "RequestCP/grpc"
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"

	//"io/ioutil"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CommServer struct {
	proto.UnimplementedRequestServiceServer
	username   string
	clients    map[string]proto.RequestService_CommServer
	servers    map[string]grpc.BidiStreamingClient[proto.RequestMessage, proto.RequestMessage]
	fileName   string
	mutex      sync.Mutex
	state      string
	deferred   []string
	waitingRep []string
	LCt        int64
}

// NewCommServer initializes a new CommServer
func NewCommServer() *CommServer {
	return &CommServer{
		username:   "localhost:5067",
		clients:    make(map[string]proto.RequestService_CommServer),
		servers:    make(map[string]grpc.BidiStreamingClient[proto.RequestMessage, proto.RequestMessage]),
		fileName:   "../Database.txt",
		mutex:      sync.Mutex{},
		state:      "RELEASED",
		deferred:   []string{},
		waitingRep: []string{},
		LCt:        0,
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

		// First message should contain the username
		if username == "" {
			username = msg.Username
			s.addClient(username, stream)
			fmt.Printf("User %s connected.\n", username)
		}

		// Update Logical Clock
		s.mutex.Lock()
		s.LCt = max(s.LCt, msg.Timestamp) + 1
		msg.Timestamp = s.LCt
		s.mutex.Unlock()

		switch msg.Message {
		case "REQUEST":
			go s.handleRequest(msg)
		case "REPLY":
			go s.handleReply(msg)
		case "PING":
			// Ignore PING messages
		}
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

	if (msg.Timestamp > s.LCt && s.state == "WANTED") || (s.state == "HELD") {
		s.deferred = append(s.deferred, msg.Username)
	} else {
		s.sendReply(msg)
	}
	s.LCt = max(s.LCt, msg.Timestamp) + 1

}

func (s *CommServer) handleReply(msg *proto.RequestMessage) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Remove the sender from waitingRep
	for i, username := range s.waitingRep {
		if username == msg.Username {
			s.waitingRep = append(s.waitingRep[:i], s.waitingRep[i+1:]...)
			break
		}
	}
	s.LCt = max(s.LCt, msg.Timestamp) + 1

}

func (s *CommServer) sendRequests() {
	s.mutex.Lock()
	s.state = "WANTED"
	s.LCt = s.LCt + 1
	timestamp := s.LCt
	s.checkConnections()
	s.mutex.Unlock()

	reqMsg := &proto.RequestMessage{
		Username:  s.username,
		Message:   "REQUEST",
		Timestamp: timestamp,
	}

	for node := range s.servers {
		stream, err := s.servers[node]
		if err {
			log.Printf("Error getting stream for node %s: %v", node, err)
			continue
		}
		stream.Send(reqMsg)
		s.waitingRep = append(s.waitingRep, node)
	}

	s.waitReplies()
}

func (s *CommServer) sendReply(msg *proto.RequestMessage) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	//Update Logical Clock
	s.LCt = max(s.LCt, msg.Timestamp) + 1

	// Send REPLY message
	replyMsg := &proto.RequestMessage{
		Username:  s.username,
		Message:   "REPLY",
		Timestamp: s.LCt,
	}
	s.servers[msg.Username].Send(replyMsg)

}

func (s *CommServer) waitReplies() {
	for {
		if len(s.waitingRep) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond) // Sleep to avoid busy waiting
	}
	s.enterCriticalSection()
}

func (s *CommServer) enterCriticalSection() {
	// Implementation for entering critical section
	s.mutex.Lock()
	s.state = "HELD"
	s.LCt = s.LCt + 1
	s.mutex.Unlock()

	fmt.Printf("L: %d - [Server] (Critical Section) Entered critical section.\n", s.LCt)

	// Simulate critical section by reading the database file
	content, err := os.ReadFile(s.fileName)
	if err != nil {
		log.Fatalf("Failed to read database file: %v", err)
	}
	fmt.Println(string(content))

	time.Sleep(5 * time.Second) // Simulate time spent in critical section

	s.exitCriticalSection()
}

func (s *CommServer) exitCriticalSection() {
	s.mutex.Lock()
	s.state = "RELEASED"
	s.LCt = s.LCt + 1
	s.mutex.Unlock()

	fmt.Printf("L: %d - [Server] (Critical Section) Exited critical section.\n", s.LCt)
	// Send REPLY messages to deferred requests
	for _, username := range s.deferred {
		s.sendReply(&proto.RequestMessage{
			Username:  username,
			Message:   "REPLY",
			Timestamp: s.LCt,
		})
	}
	s.deferred = []string{}
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

		stream, exists := s.servers[address]
		if exists {
			// Try sending a ping to test if the connection is alive
			err := stream.Send(&proto.RequestMessage{
				Username:  s.username,
				Message:   "PING",
				Timestamp: s.LCt})
			if err == nil {
				continue // Connection is healthy
			}
			log.Printf("Connection to %s is dead, removing and reconnecting", address)
			delete(s.servers, address)
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

	go func() {
		for {
			newComm.sendRequests()
			time.Sleep(10 * time.Second) // Wait before next request
		}
	}()

	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
