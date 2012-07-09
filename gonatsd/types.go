// Copyright (c) 2012 VMware, Inc.

package gonatsd

type Message struct {
	Subject string
	ReplyTo string
	Content []byte
}

type Subscription struct {
	Id           int
	Subject      string
	Queue        *string
	Conn         Conn
	MaxResponses int
	Responses    uint64
}

type SubscribedMessage struct {
	Subscription *Subscription
	Message      *Message
	Last         bool
}
