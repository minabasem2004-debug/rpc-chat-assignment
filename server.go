package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	serverAddress = ":1234"
	clientTimeout = 15 * time.Second
)

type ClientState struct {
	queue    []Update
	lastSeen time.Time
}

type ChatServer struct {
	mu      sync.Mutex
	clients map[string]*ClientState
}

func NewChatServer() *ChatServer {
	return &ChatServer{
		clients: make(map[string]*ClientState),
	}
}

// Add an update to every client except the sender.
func (s *ChatServer) broadcastLocked(update Update, exclude string) {
	for username, client := range s.clients {
		if username != exclude {
			client.queue = append(client.queue, update)
		}
	}
}

func (s *ChatServer) Join(args JoinArgs, reply *JoinReply) error {
	username := strings.TrimSpace(args.Username)

	if username == "" {
		reply.Error = "Username cannot be empty."
		return nil
	}
	if strings.ContainsAny(username, "\r\n") {
		reply.Error = "Username cannot contain line breaks."
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.clients[username]; exists {
		reply.Error = "That username is already in use."
		return nil
	}

	s.clients[username] = &ClientState{
		queue:    make([]Update, 0),
		lastSeen: time.Now(),
	}

	s.broadcastLocked(Update{
		Kind: "join",
		From: username,
		Text: username + " joined the chat.",
	}, username)

	reply.OK = true
	fmt.Printf("[JOIN] %s\n", username)
	return nil
}

func (s *ChatServer) SendMessage(args MessageArgs, reply *bool) error {
	text := strings.TrimSpace(args.Text)
	if text == "" {
		*reply = false
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	client, exists := s.clients[args.Username]
	if !exists {
		*reply = false
		return nil
	}

	client.lastSeen = time.Now()

	s.broadcastLocked(Update{
		Kind: "message",
		From: args.Username,
		Text: text,
	}, args.Username)

	fmt.Printf("[MESSAGE] %s: %s\n", args.Username, text)
	*reply = true
	return nil
}

func (s *ChatServer) Poll(args PollArgs, reply *PollReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, exists := s.clients[args.Username]
	if !exists {
		reply.Updates = []Update{{
			Kind: "info",
			Text: "Your session has expired. Please reconnect.",
		}}
		return nil
	}

	client.lastSeen = time.Now()
	reply.Updates = append([]Update(nil), client.queue...)
	client.queue = client.queue[:0]
	return nil
}

func (s *ChatServer) ListUsers(_ struct{}, reply *ListReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply.Users = make([]string, 0, len(s.clients))
	for username := range s.clients {
		reply.Users = append(reply.Users, username)
	}
	sort.Strings(reply.Users)
	return nil
}

func (s *ChatServer) Leave(args LeaveArgs, reply *bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.clients[args.Username]; !exists {
		*reply = false
		return nil
	}

	delete(s.clients, args.Username)
	s.broadcastLocked(Update{
		Kind: "leave",
		From: args.Username,
		Text: args.Username + " left the chat.",
	}, args.Username)

	fmt.Printf("[LEAVE] %s\n", args.Username)
	*reply = true
	return nil
}

// Remove clients that have stopped polling, for example after a crash.
func (s *ChatServer) cleanupInactive() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()

		for username, client := range s.clients {
			if now.Sub(client.lastSeen) > clientTimeout {
				delete(s.clients, username)
				s.broadcastLocked(Update{
					Kind: "leave",
					From: username,
					Text: username + " disconnected unexpectedly.",
				}, username)
				fmt.Printf("[TIMEOUT] %s\n", username)
			}
		}

		s.mu.Unlock()
	}
}

func main() {
	chat := NewChatServer()

	if err := rpc.RegisterName("Chat", chat); err != nil {
		log.Fatal("RPC registration failed:", err)
	}

	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatal("Could not start server:", err)
	}

	fmt.Println("Chat server listening on port 1234.")
	fmt.Println("Press Ctrl+C to stop the server.")

	// Close the listener when Ctrl+C is pressed.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go chat.cleanupInactive()

	go func() {
		<-stop
		fmt.Println("\nShutting down server...")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		go rpc.ServeConn(conn)
	}

	fmt.Println("Server stopped.")
}
