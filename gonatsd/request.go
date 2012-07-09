// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"encoding/json"
	"io"
	"unicode"
)

// Map of all request parsers.
var REQUEST_PARSERS = map[string]func(Conn, string) (Request, error){
	PUB:     ParsePublishRequest,
	SUB:     ParseSubscriptionRequest,
	UNSUB:   ParseUnsubscriptionRequest,
	PING:    ParsePingRequest,
	PONG:    ParsePongRequest,
	CONNECT: ParseConnectRequest,
	INFO:    ParseInfoRequest,
}

var OK = "OK"

// A Request represents an incoming client request.
type Request interface {

	// Serves the request by returning an optional payload to the client.
	Serve(Conn) *Response

	// Dispatches the request to the proper processing channel(s).
	Dispatch(Conn)
}

// A PingRequest represents a Request sent to verify that the server is alive.
type PingRequest struct {
}

func ParsePingRequest(c Conn, args string) (Request, error) {
	if IsAllSpace(args) {
		return PING_REQUEST, nil
	}
	return nil, ErrUnknownOp
}

func (r *PingRequest) Serve(c Conn) *Response {
	return NewStringResponse(PONG)
}

func (r *PingRequest) Dispatch(c Conn) {
	c.ServeRequest(r)
}

// A PongRequest represents a Request sent as a response to the server's 
// ping request. The terminology is reversed because the server is the one 
// making the request.
type PongRequest struct {
}

func ParsePongRequest(c Conn, args string) (Request, error) {
	if IsAllSpace(args) {
		return PONG_REQUEST, nil
	}
	return nil, ErrUnknownOp
}

func (r *PongRequest) Serve(c Conn) *Response {
	c.HeartbeatHelper().Pong()
	return nil
}

func (r *PongRequest) Dispatch(c Conn) {
	c.ServeRequest(r)
}

// A InfoRequest represents a Request sent to return the server 
// configuration.
type InfoRequest struct {
}

func ParseInfoRequest(c Conn, args string) (Request, error) {
	if IsAllSpace(args) {
		return INFO_REQUEST, nil
	}
	return nil, ErrUnknownOp
}

var INFO_PRELUDE = "INFO "

func (r *InfoRequest) Serve(c Conn) *Response {
	return &Response{Value: &INFO_PRELUDE, Bytes: c.Server().Info()}
}

func (r *InfoRequest) Dispatch(c Conn) {
	c.ServeRequest(r)
}

// A PublishRequest represents a Request sent to publish a message.
type PublishRequest struct {
	Message *Message
}

func ParsePublishRequest(c Conn, args string) (Request, error) {
	message := &Message{}
	var length int
	var err error

	fields := fieldsN(args, unicode.IsSpace, 3)

	switch len(fields) {
	case 2:
		message.Subject = fields[0]
		length, err = parseInt(fields[1])
		if err != nil {
			return nil, ErrUnknownOp
		}
	case 3:
		message.Subject = fields[0]
		message.ReplyTo = fields[1]
		length, err = parseInt(fields[2])
		if err != nil {
			return nil, ErrUnknownOp
		}
	default:
		return nil, ErrUnknownOp
	}

	if length > c.Server().Config().Limits.Payload {
		return nil, ErrPayloadTooBig
	}

	message.Content = make([]byte, length)
	_, err = io.ReadFull(c, message.Content)
	if err != nil {
		return nil, err
	}

	l, err := c.ReadControlLine()
	if err != nil {
		return nil, err
	}

	if l != "" {
		return nil, ErrUnknownOp
	}

	if c.Options().Pedantic && !ensureValidPublishedSubject(message.Subject) {
		return nil, ErrInvalidSubject
	}
	return &PublishRequest{message}, nil
}

func (r *PublishRequest) Serve(c Conn) *Response {
	return &Response{Value: &OK}
}

func (r *PublishRequest) Dispatch(c Conn) {
	c.Server().Commands() <- &PublishCmd{r.Message}

	if c.Options().Verbose {
		c.ServeRequest(r)
	}
}

// A ConnectRequest represents a Request sent to authenticate (if needed) and 
// negotiate any connection options.
type ConnectRequest struct {
	Verbose  *bool   `json:"verbose"`
	Pedantic *bool   `json:"pedantic"`
	User     *string `json:"user"`
	Password *string `json:"pass"`
}

func ParseConnectRequest(c Conn, args string) (Request, error) {
	request := &ConnectRequest{}
	err := json.Unmarshal([]byte(args), request)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	return request, nil
}

func (r *ConnectRequest) Serve(c Conn) *Response {
	if r.Pedantic != nil {
		c.Options().Pedantic = *r.Pedantic
	}
	if r.Verbose != nil {
		c.Options().Verbose = *r.Verbose
	}
	if c.Options().Verbose {
		return &Response{Value: &OK}
	}
	return nil
}

func (r *ConnectRequest) Dispatch(c Conn) {
	c.ServeRequest(r)
}

// A BadRequest represents a "Request" that had a problem for which 
// an error will be returned to the client.
type BadRequest struct {
	Error *NATSError
}

func (r *BadRequest) Serve(c Conn) *Response {
	if r.Error.Close {
		c.CloseWithError(r.Error)
		return nil
	}
	return &Response{Value: &r.Error.Message}
}

func (r *BadRequest) Dispatch(c Conn) {
	panic("should never dispatch bad requests")
}

// A SubscriptionRequest
type SubscriptionRequest struct {
	Subscription *Subscription
	Done         chan bool
}

func ParseSubscriptionRequest(c Conn, args string) (Request, error) {
	var err error
	subscription := &Subscription{Conn: c, MaxResponses: -1}
	fields := fieldsN(args, unicode.IsSpace, 3)
	switch len(fields) {
	case 2:
		subscription.Subject = fields[0]
		subscription.Id, err = parseInt(fields[1])
		if err != nil {
			return nil, ErrUnknownOp
		}
	case 3:
		subscription.Subject = fields[0]
		subscription.Queue = &fields[1]
		subscription.Id, err = parseInt(fields[2])
		if err != nil {
			return nil, ErrUnknownOp
		}
	default:
		return nil, ErrUnknownOp
	}

	if !ensureValidSubscribedSubject(subscription.Subject) {
		return nil, ErrInvalidSubject
	}

	return &SubscriptionRequest{subscription, make(chan bool, 1)}, nil
}

func (r *SubscriptionRequest) Serve(c Conn) *Response {
	if c.Subscriptions()[r.Subscription.Id] != nil {
		r.Done <- true
		return &Response{Value: &ErrInvalidSidTaken.Message}
	}

	c.Subscriptions()[r.Subscription.Id] = r.Subscription
	c.SendServerCmd(&SubscribeCmd{r.Subscription, r.Done})
	if c.Options().Verbose {
		return &Response{Value: &OK}
	}
	return nil
}

func (r *SubscriptionRequest) Dispatch(c Conn) {
	c.ServeRequest(r)
	<-r.Done
}

// An UnsubscriptionRequest
type UnsubscriptionRequest struct {
	SubscriptionId int
	MaxResponses   int
}

func ParseUnsubscriptionRequest(c Conn, args string) (Request, error) {
	var err error
	request := &UnsubscriptionRequest{}
	fields := fieldsN(args, unicode.IsSpace, 2)
	switch len(fields) {
	case 1:
		request.SubscriptionId, err = parseInt(fields[0])
		if err != nil {
			return nil, ErrUnknownOp
		}
		request.MaxResponses = -1
	case 2:
		request.SubscriptionId, err = parseInt(fields[0])
		if err != nil {
			return nil, ErrUnknownOp
		}

		request.MaxResponses, err = parseInt(fields[1])
		if err != nil {
			return nil, ErrUnknownOp
		}
	default:
		return nil, ErrUnknownOp
	}
	return request, nil
}

func (r *UnsubscriptionRequest) Serve(c Conn) *Response {
	subscription := c.Subscriptions()[r.SubscriptionId]
	if subscription != nil {
		cmd := &UnsubscribeCmd{subscription, r.MaxResponses, make(chan bool, 1)}
		c.SendServerCmd(cmd)
		if <-cmd.Unsubscribed {
			delete(c.Subscriptions(), subscription.Id)
		}
		if c.Options().Verbose {
			return &Response{Value: &OK}
		}
		return nil
	}

	if c.Options().Pedantic {
		return &Response{Value: &ErrInvalidSidNoexist.Message}
	}

	return nil
}

func (r *UnsubscriptionRequest) Dispatch(c Conn) {
	c.ServeRequest(r)
}

var (
	PING_REQUEST = &PingRequest{}
	PONG_REQUEST = &PongRequest{}
	INFO_REQUEST = &InfoRequest{}
)
