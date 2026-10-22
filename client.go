package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const server = "127.0.0.1:1234"

type Input struct {
	Text string
	OK   bool
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter your username: ")
	username, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Could not read username.")
		return
	}
	username = strings.TrimSpace(username)

	conn, err := net.Dial("tcp", server)
	if err != nil {
		fmt.Println("Could not connect to the server:", err)
		return
	}

	rpcClient := rpc.NewClient(conn)

	var joinReply JoinReply
	if err := rpcClient.Call("Chat.Join", JoinArgs{Username: username}, &joinReply); err != nil {
		fmt.Println("Join failed:", err)
		rpcClient.Close()
		return
	}
	if !joinReply.OK {
		fmt.Println("Could not join:", joinReply.Error)
		rpcClient.Close()
		return
	}

	fmt.Printf("\nWelcome, %s!\n", username)
	fmt.Println("Type a message, /users to list users, or /quit to exit.")

	// Input and updates are delivered to the main goroutine.
	inputs := make(chan Input)
	updates := make(chan Update, 100)
	done := make(chan struct{})
	var closeOnce sync.Once

	closeDone := func() {
		closeOnce.Do(func() { close(done) })
	}

	// Read terminal input independently so incoming messages remain visible.
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case inputs <- Input{Text: scanner.Text(), OK: true}:
			case <-done:
				return
			}
		}
		select {
		case inputs <- Input{OK: false}:
		case <-done:
		}
	}()

	// Poll the server continuously for queued messages and notifications.
	go func() {
		defer close(updates)

		for {
			select {
			case <-done:
				return
			default:
			}

			var reply PollReply
			err := rpcClient.Call("Chat.Poll", PollArgs{Username: username}, &reply)
			if err != nil {
				select {
				case updates <- Update{Kind: "info", Text: "Lost connection to the server."}:
				case <-done:
				}
				return
			}

			for _, update := range reply.Updates {
				select {
				case updates <- update:
				case <-done:
					return
				}
			}

			time.Sleep(300 * time.Millisecond)
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	joined := true

	leave := func() {
		if !joined {
			return
		}
		joined = false
		var reply bool
		_ = rpcClient.Call("Chat.Leave", LeaveArgs{Username: username}, &reply)
		closeDone()
		_ = rpcClient.Close()
	}

	for {
		select {
		case update, ok := <-updates:
			if !ok {
				fmt.Println("\nDisconnected from the server.")
				leave()
				return
			}

			switch update.Kind {
			case "message":
				fmt.Printf("\n[%s]: %s\n> ", update.From, update.Text)
			case "join", "leave", "info":
				fmt.Printf("\n%s\n> ", update.Text)
				if strings.Contains(update.Text, "session has expired") {
					leave()
					return
				}
				if update.Text == "Lost connection to the server." {
					leave()
					return
				}
			}

		case input := <-inputs:
			if !input.OK {
				leave()
				fmt.Println("\nDisconnected.")
				return
			}

			text := strings.TrimSpace(input.Text)
			if text == "" {
				continue
			}

			switch text {
			case "/quit":
				leave()
				fmt.Println("You left the chat.")
				return

			case "/users":
				var reply ListReply
				if err := rpcClient.Call("Chat.ListUsers", struct{}{}, &reply); err != nil {
					fmt.Println("Could not retrieve users:", err)
					continue
				}
				fmt.Println("Connected users:", strings.Join(reply.Users, ", "))

			default:
				var accepted bool
				err := rpcClient.Call("Chat.SendMessage", MessageArgs{
					Username: username,
					Text:     text,
				}, &accepted)

				if err != nil || !accepted {
					fmt.Println("Message could not be sent. You may have disconnected.")
				}
			}

		case <-interrupt:
			fmt.Println("\nCtrl+C received.")
			leave()
			return
		}
	}
}
