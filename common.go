package main

// JoinArgs contains the username requested by a client.
type JoinArgs struct {
	Username string
}

type JoinReply struct {
	OK    bool
	Error string
}

// MessageArgs is used when a client sends a chat message.
type MessageArgs struct {
	Username string
	Text     string
}

// PollArgs asks the server for pending updates.
type PollArgs struct {
	Username string
}

type Update struct {
	Kind string // message, join, leave, or info
	From string
	Text string
}

type PollReply struct {
	Updates []Update
}

// ListReply contains the currently connected usernames.
type ListReply struct {
	Users []string
}

// LeaveArgs identifies the client that is disconnecting.
type LeaveArgs struct {
	Username string
}
