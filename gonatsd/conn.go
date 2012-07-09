// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"bufio"
	"fmt"
	log "github.com/cihub/seelog"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
)

type ConnOptions struct {
	Verbose  bool
	Pedantic bool
}

// Client connection.
type Conn interface {
	// Serve a subscribed message on the dispatch loop.
	ServeMessage(*SubscribedMessage)

	// Serve a client request on the dispatch loop.
	ServeRequest(Request)

	// Serve a client command on the dispatch loop.
	ServeCommand(ClientCmd)

	// Read from the client.
	Read([]byte) (int, error)

	// Read a single NATS control line from the client.
	ReadControlLine() (string, error)

	// Write a response to the client.
	Write(*Response)

	// Send a command to the server.
	SendServerCmd(ServerCmd)

	// Start the connection handling.
	Start()

	// Close the connection.
	Close()

	// Returns true iff the connection is closing/closed.
	Closed() bool

	// Close the connection and send the error to the client.
	CloseWithError(*NATSError)

	// Return a map<SID, subscription>.
	Subscriptions() map[int]*Subscription

	// Server reference.
	Server() Server

	// Return the connection heartbeat helper.
	HeartbeatHelper() HeartbeatHelper

	// Returns the connection options.
	Options() *ConnOptions

	// Returns the client remote address.
	RemoteAddr() net.Addr
}

// TCP connection interface for testing.
type TCPConn interface {
	net.Conn
	CloseRead() error
}

type conn struct {
	server             Server
	inbox              chan Request
	commands           chan ClientCmd
	subscribedMessages chan *SubscribedMessage
	subcriptions       map[int]*Subscription
	started            bool
	closed             bool
	options            *ConnOptions
	outboxQueue        *BoundedQueue
	tc                 TCPConn
	reader             *bufio.Reader
	writer             *bufio.Writer
	heartbeatHelper    HeartbeatHelper
	authHelper         *AuthHelper
	fatalError         chan *NATSError
	writerDone         chan bool
}

const (
	INFO    = "INFO"
	PUB     = "PUB"
	SUB     = "SUB"
	UNSUB   = "UNSUB"
	PING    = "PING"
	PONG    = "PONG"
	CONNECT = "CONNECT"
)

const (
	MAX_CONN_CHAN_BACKLOG   = 16
	MAX_OUTBOUND_QUEUE_SIZE = 32
)

var (
	// not a const so we can change it for testing
	BUF_IO_SIZE = 64 * 1024

	REQUESTS = []string{INFO, PUB, SUB, UNSUB, PING, PONG, CONNECT}
)

// Creates a new connection given a server and an underlying TCP connection.
func NewConn(server Server, tc TCPConn) Conn {
	c := &conn{}
	c.inbox = make(chan Request, MAX_CONN_CHAN_BACKLOG)
	c.outboxQueue = NewBoundedQueue(int32(server.Config().Limits.Pending))
	c.commands = make(chan ClientCmd, MAX_CONN_CHAN_BACKLOG)
	c.subscribedMessages = make(chan *SubscribedMessage, MAX_CONN_CHAN_BACKLOG)

	c.server = server
	c.subcriptions = make(map[int]*Subscription)
	c.options = &ConnOptions{Pedantic: true, Verbose: true}

	c.tc = tc
	c.reader = bufio.NewReaderSize(tc, BUF_IO_SIZE)
	c.writer = bufio.NewWriterSize(tc, BUF_IO_SIZE)

	c.fatalError = make(chan *NATSError, 1)
	c.writerDone = make(chan bool, 1)

	c.heartbeatHelper = NewHeartbeatHelper(c, server.Config().Ping.IntervalDuration,
		server.Config().Ping.MaxOutstanding)
	c.authHelper = NewAuthHelper(c, server.Config().Auth.Users,
		server.Config().Auth.TimeoutDuration)
	return c
}

// ServeMessage implements the Conn ServeMessage method.
func (c *conn) ServeMessage(message *SubscribedMessage) {
	c.subscribedMessages <- message
}

// ServeRequest implements the Conn ServeRequest method.
func (c *conn) ServeRequest(request Request) {
	c.inbox <- request
}

// ServeCommand implements the Conn ServeCommand method.
func (c *conn) ServeCommand(cmd ClientCmd) {
	c.commands <- cmd
}

// Read implements the Conn Read method.
func (c *conn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// ReadConrtolLine implements the Conn ReadConrtolLine method.
func (c *conn) ReadControlLine() (string, error) {
	line, partial, err := c.reader.ReadLine()

	if err != nil {
		return string(line), err
	}

	for partial {
		if len(line) > c.server.Config().Limits.ControlLine {
			return string(line), ErrProtocolOpTooBig
		}

		var l []byte
		l, partial, err = c.reader.ReadLine()
		line = append(line, l...)
		if err != nil {
			return string(line), err
		}
	}

	if len(line) > c.server.Config().Limits.ControlLine {
		return string(line), ErrProtocolOpTooBig
	}

	return string(line), nil
}

// Write implements the Conn Write method.
func (c *conn) Write(response *Response) {
	err := c.outboxQueue.Enqueue(response)
	if err != nil {
		if err == ErrQueueFull {
			c.CloseWithError(ErrSlowConsumer)
			return
		}
		panic(fmt.Sprintf("Unknown error: %s", err))
	}
}

// SendServerCmd implements the Conn SendServerCmd method.
func (c *conn) SendServerCmd(r ServerCmd) {
	// Tries to send the message while at the same time handling requests from the server
	// to avoid a deadlock.
	subscribedMessages := c.subscribedMessages
	for {
		select {
		case c.server.Commands() <- r:
			return
		case message, ok := <-subscribedMessages:
			if !ok {
				subscribedMessages = nil
				break
			}
			if !c.closed {
				c.processMessage(message)
			}
		}
	}
}

// Start implements the Conn Start method.
func (c *conn) Start() {
	log.Info("[client %s] connected", c.RemoteAddr())
	c.started = true
	go c.writeLoop()
	go c.readLoop()
	c.dispatchLoop()
}

// Close implements the Conn Close method.
func (c *conn) Close() {
	if !c.closed {
		c.closed = true

		c.heartbeatHelper.Stop()
		c.authHelper.Stop()

		c.tc.CloseRead()
		c.outboxQueue.Close()

		c.unregister()

		// Only wait for writes to finish if the dispatch loop started
		if c.started {
			select {
			case <-c.writerDone:
			case <-time.After(time.Second):
			}
		}
		c.tc.Close()
	}
}

// CloseWithError implements the Conn CloseWithError method.
func (c *conn) CloseWithError(err *NATSError) {
	if !c.closed {
		switch err {
		case ErrSlowConsumer:
			atomic.AddInt64(&c.server.Stats().slow_consumer, 1)
		case ErrPayloadTooBig:
			atomic.AddInt64(&c.server.Stats().payload_too_big, 1)
		case ErrAuthFailed, ErrAuthRequired:
			atomic.AddInt64(&c.server.Stats().bad_auth, 1)
		case ErrUnresponsive:
			atomic.AddInt64(&c.server.Stats().unresponsive, 1)
		default:
			atomic.AddInt64(&c.server.Stats().errors, 1)
		}

		log.Warn("[client %s] error: %s", c.RemoteAddr(), err.Message)
		c.fatalError <- err
		c.Close()
	}
}

// Server implements the Conn Server method.
func (c *conn) Server() Server {
	return c.server
}

// HeartbeatHelper implements the Conn HeartbeatHelper method.
func (c *conn) HeartbeatHelper() HeartbeatHelper {
	return c.heartbeatHelper
}

// Subscriptions implements the Conn Subscriptions method.
func (c *conn) Subscriptions() map[int]*Subscription {
	return c.subcriptions
}

// Closed implements the Conn Closed method.
func (c *conn) Closed() bool {
	return c.closed
}

// Options implements the Conn Options method.
func (c *conn) Options() *ConnOptions {
	return c.options
}

// RemoteAddr implements the Conn RemoteAddr method.
func (c *conn) RemoteAddr() net.Addr {
	return c.tc.RemoteAddr()
}

func (c *conn) unregister() {
	cmd := &UnregisterConnCmd{c, make(chan bool)}

	c.SendServerCmd(cmd)

	// Drain incoming messages in case server is trying to send us something.
	for {
		select {
		case <-cmd.Done:
			close(c.subscribedMessages)
			return
		case <-c.subscribedMessages:
		}
	}
}

func (c *conn) dispatchLoop() {
	defer log.Trace("[client %s] stopped dispatch loop", c.RemoteAddr())

	c.Write(INFO_REQUEST.Serve(c))

	for {
		select {
		case message, ok := <-c.subscribedMessages:
			if !ok {
				return
			}
			if !c.closed {
				c.processMessage(message)
			}
		case request := <-c.inbox:
			if !c.closed {
				c.processRequest(request)
			}
		case command := <-c.commands:
			command.Process(c)
		case <-c.authHelper.Timer():
			c.authHelper.Timeout()
		case <-c.heartbeatHelper.Ticker():
			c.heartbeatHelper.Ping()
		}
	}
}

func (c *conn) readLoop() {
	for {
		line, err := c.ReadControlLine()
		if err != nil {
			switch err {
			case io.EOF:
				log.Info("[client %s] disconnected", c.RemoteAddr())
				c.commands <- CLOSE_CMD
			case ErrProtocolOpTooBig:
				c.inbox <- &BadRequest{ErrProtocolOpTooBig}
			default:
				c.commands <- &ErrorCmd{err}
			}
			return
		}

		log.Trace("[client %s] %s", c.RemoteAddr(), line)

		fields := fieldsN(line, unicode.IsSpace, 2)
		if len(fields) == 0 {
			continue
		}

		if len(fields) == 1 {
			fields = append(fields, "")
		}

		err = c.processLine(fields[0], fields[1])
		if err != nil {
			switch err.(type) {
			case *NATSError:
				c.inbox <- &BadRequest{err.(*NATSError)}
			default:
				c.commands <- &ErrorCmd{err}
				return
			}
		}

	}
}

func (c *conn) writeLoop() {
	for {
		o, err := c.outboxQueue.Dequeue()
		if err != nil {
			if err != ErrQueueClosed {
				c.commands <- &ErrorCmd{err}
			}
			break
		}

		response := o.(*Response)
		err = response.Write(c.writer)
		if err != nil {
			c.commands <- &ErrorCmd{err}
			break
		}

		if !c.outboxQueue.HasMore() {
			err := c.writer.Flush()
			if err != nil {
				c.commands <- &ErrorCmd{err}
				break
			}
		}
	}

	c.writeFatalError()
	c.writer.Flush()
	c.writerDone <- true
}

// Write out a fatal error to the client if available.
func (c *conn) writeFatalError() {
	select {
	case err := <-c.fatalError:
		(&Response{Value: &err.Message}).Write(c.writer)
	default:
	}
}

func (c *conn) processMessage(subscribedMessage *SubscribedMessage) {
	message := subscribedMessage.Message
	subscription := subscribedMessage.Subscription

	if len(message.ReplyTo) > 0 {
		header := fmt.Sprintf("MSG %s %d %s %d\r\n", message.Subject, subscription.Id, message.ReplyTo,
			len(message.Content))
		c.Write(&Response{Value: &header, Bytes: &message.Content})
	} else {
		header := fmt.Sprintf("MSG %s %d %d\r\n", message.Subject, subscription.Id,
			len(message.Content))
		c.Write(&Response{Value: &header, Bytes: &message.Content})
	}
}

func (c *conn) processRequest(request Request) {
	authorized, err := c.authHelper.Auth(request)
	if authorized {
		response := request.Serve(c)
		if response != nil {
			c.Write(response)
		}
		return
	}
	c.CloseWithError(err)
}

func (c *conn) processLine(command, args string) error {
	if parseFunc, found := REQUEST_PARSERS[strings.ToUpper(command)]; found {
		atomic.AddInt64(c.server.Stats().ops[command], 1)
		request, err := parseFunc(c, args)
		if err != nil {
			return err
		}
		request.Dispatch(c)
		return nil
	}
	return ErrUnknownOp
}
