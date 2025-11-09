package main

import (
	proto "RequestCP/grpc"
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"

	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	ID     string
	Stream proto.RequestService_CommServer
	Client proto.RequestService_CommClient
	Active bool
}

type CommServer struct {
	proto.UnimplementedRequestServiceServer
	username    string
	peers       map[string]*Peer
	fileName    string
	mutex       sync.Mutex
	state       string
	requestLC   int64
	deferred    []string
	waitingReps []string
	LCt         int64
}

// NewCommServer initializes a new CommServer
func NewCommServer() *CommServer {
	return &CommServer{
		username:    "empty",
		peers:       make(map[string]*Peer),
		fileName:    "../Database.txt",
		mutex:       sync.Mutex{},
		state:       "RELEASED",
		requestLC:   0,
		deferred:    []string{},
		waitingReps: []string{},
		LCt:         0,
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
			s.mutex.Lock()
			s.peers[username].Active = false
			s.mutex.Unlock()
			s.incrementLC()
			fmt.Printf("L: %d - [Comm] Client %s disconnected.\n", s.LCt, username)
			return err
		}

		// First message should contain the username
		if username == "" {
			s.mutex.Lock()
			s.peers[msg.Username] = &Peer{
				ID:     msg.Username,
				Stream: stream,
				Active: true,
			}
			s.mutex.Unlock()
			username = msg.Username
		}

		if !s.peers[username].Active {
			s.mutex.Lock()
			s.peers[username].Active = true
			s.mutex.Unlock()
		}

		// Handle different message types
		switch msg.Message {
		case "REQUEST":
			go s.handleRequest(msg)
		case "REPLY":
			go s.handleReply(msg)
		case "PING":
			s.mutex.Lock()
			s.LCt = max(s.LCt, msg.Timestamp) + 1
			s.mutex.Unlock()

			pingReply := &proto.RequestMessage{
				Username:  s.username,
				Message:   "PING_REPLY",
				Timestamp: s.LCt,
			}
			s.sendTo(msg.Username, pingReply)
		case "PING_REPLY":
			s.mutex.Lock()
			s.LCt = max(s.LCt, msg.Timestamp) + 1
			s.mutex.Unlock()
		}
	}
}

func (s *CommServer) sendTo(peerID string, msg *proto.RequestMessage) error {
	s.mutex.Lock()
	p, ok := s.peers[peerID]
	s.mutex.Unlock()

	if !ok {
		return fmt.Errorf("no such peer %s", peerID)
	}

	if p.Client != nil {
		return p.Client.Send(msg)
	}
	return fmt.Errorf("peer %s has no active stream", peerID)
}

func (s *CommServer) sendReply(msg *proto.RequestMessage) {
	s.incrementLC()
	fmt.Printf("L: %d - [sendReply] Sending REPLY\n", s.LCt)

	replyMsg := &proto.RequestMessage{
		Username:  s.username,
		Message:   "REPLY",
		Timestamp: s.LCt,
	}

	s.sendTo(msg.Username, replyMsg)
}

func (s *CommServer) handleRequest(msg *proto.RequestMessage) {
	fmt.Printf("L: %d - [handleRequest] Received REQUEST\n", s.LCt)

	switch s.state {
	case "RELEASED":
		s.sendReply(msg)
	case "HELD":
		s.deferred = append(s.deferred, msg.Username)
		s.incrementLC()
	case "WANTED":
		if msg.Timestamp < s.LCt {
			s.sendReply(msg)
		} else {
			s.deferred = append(s.deferred, msg.Username)
			s.incrementLC()
		}
	}
}

func (s *CommServer) handleReply(msg *proto.RequestMessage) {
	fmt.Printf("L: %d - [handleReply] Received REPLY\n", s.LCt)

	// Remove the sender from waitingRep
	s.mutex.Lock()
	for i, username := range s.waitingReps {
		if username == msg.Username {
			s.waitingReps = append(s.waitingReps[:i], s.waitingReps[i+1:]...)
			break
		}
	}
	s.mutex.Unlock()

	s.incrementLC()
}

func (s *CommServer) sendRequests() {
	s.incrementLC()
	s.mutex.Lock()
	s.state = "WANTED"
	s.requestLC = s.LCt
	s.mutex.Unlock()

	fmt.Printf("L: %d - [sendRequests] Sending REQUEST messages to all nodes...\n", s.LCt)
	reqMsg := &proto.RequestMessage{
		Username:  s.username,
		Message:   "REQUEST",
		Timestamp: s.LCt,
	}
	s.mutex.Lock()
	for peerID, peer := range s.peers {
		if peer.Active {
			err := s.sendTo(peerID, reqMsg)
			if err != nil {
				fmt.Printf("Failed to send request to %s: %v", peerID, err)
				continue
			}
			s.waitingReps = append(s.waitingReps, peerID)
		}
	}
	s.mutex.Unlock()

	s.waitReplies()
}

func (s *CommServer) waitReplies() {
	s.incrementLC()

	fmt.Printf("L: %d - [waitReplies] Waiting for REPLY messages from all nodes...\n", s.LCt)

	for {
		if len(s.waitingReps) == 0 {
			break
		}
		time.Sleep(2 * time.Second) // Sleep to avoid busy waiting
	}
	s.enterCriticalSection()
}

// Enter the critical section
func (s *CommServer) enterCriticalSection() {
	s.incrementLC()
	s.mutex.Lock()
	s.state = "HELD"
	s.mutex.Unlock()

	fmt.Printf("L: %d - [enterCriticalSection] (Critical Section) Entered critical section.\n", s.LCt)

	// Simulate critical section by reading the database file
	content, err := os.ReadFile(s.fileName)
	if err != nil {
		fmt.Printf("Failed to read database file: %v", err)
	}
	fmt.Println(string(content))

	time.Sleep(5 * time.Second) // Simulate time spent in critical section

	s.exitCriticalSection()
}

// Exit the critical section
func (s *CommServer) exitCriticalSection() {
	s.incrementLC()
	s.mutex.Lock()
	s.state = "RELEASED"
	s.mutex.Unlock()

	fmt.Printf("L: %d - [exitCriticalSection] (Critical Section) Exited critical section.\n", s.LCt)

	// Send REPLY messages to deferred requests
	for _, username := range s.deferred {
		s.sendReply(&proto.RequestMessage{
			Username:  username,
			Message:   "REPLY",
			Timestamp: s.LCt,
		})
	}
	s.deferred = []string{}

	s.sendReplies()

	time.Sleep(2 * time.Second) // Short delay before next operation
}

func (s *CommServer) sendReplies() {
	s.incrementLC()

	fmt.Printf("L: %d - [sendReplies] Sending REPLY messages to deferred requests...\n", s.LCt)

	for _, username := range s.deferred {
		s.sendReply(&proto.RequestMessage{
			Username:  username,
			Message:   "REPLY",
			Timestamp: s.LCt,
		})
	}
	s.deferred = []string{}
	time.Sleep(2 * time.Second) // Short delay before next operation
}

// Initial connections to active nodes
func (s *CommServer) initialConnections() {
	// Read Nodes.txt to get addresses and add each line to an array entry
	nodeArray := []string{}
	file, err := os.Open("../Nodes.txt")
	if err != nil {
		fmt.Printf("Failed to open Nodes.txt: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		nodeArray = append(nodeArray, line)
	}

	// Connect to other nodes
	for _, address := range nodeArray {
		if address == s.username {
			continue // Skip connecting to itself
		}

		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Failed to connect to %s: %v", address, err)
			continue
		}

		client := proto.NewRequestServiceClient(conn)
		stream, err := client.Comm(context.Background())
		if err != nil {
			fmt.Printf("Node at %s not active.\n", address)
		}

		s.mutex.Lock()
		peer, exists := s.peers[address]
		if !exists {
			peer = &Peer{ID: address}
			s.peers[address] = peer
		}
		peer.Client = stream
		peer.Active = true
		s.mutex.Unlock()

	}
}

// Check and maintain connections to other nodes
func (s *CommServer) pingPeers() {
	s.mutex.Lock()
	peers := make([]*Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	s.mutex.Unlock()

	for _, p := range peers {
		msg := &proto.RequestMessage{
			Username:  s.username,
			Message:   "PING",
			Timestamp: s.LCt,
		}
		if err := s.sendTo(p.ID, msg); err != nil {
			//fmt.Printf("Peer %s unreachable: %v\n", p.ID, err)
			s.mutex.Lock()
			p.Active = false
			s.mutex.Unlock()
		}
	}
}

func (s *CommServer) incrementLC() {
	s.mutex.Lock()
	s.LCt = s.LCt + 1
	s.mutex.Unlock()
}

func main() {
	port := flag.String("port", "5069", "Port to run the server on")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%s", *port)

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Printf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	newComm := NewCommServer()
	newComm.username = addr
	proto.RegisterRequestServiceServer(server, newComm)

	newComm.LCt = newComm.LCt + 1

	fmt.Printf("L: %d - [Server] Comm server started on port %s...\n", newComm.LCt, *port)

	newComm.initialConnections()

	// Periodically send REQUEST messages
	go func() {
		for {
			newComm.pingPeers()
			time.Sleep(10 * time.Second) // Wait before next ping
		}
	}()

	go func() {
		for {
			newComm.sendRequests()
			time.Sleep(10 * time.Second) // Wait before next request
		}
	}()

	if err := server.Serve(lis); err != nil {
		fmt.Printf("Failed to serve: %v", err)
	}

}
