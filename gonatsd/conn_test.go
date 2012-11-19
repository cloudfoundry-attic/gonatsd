// Copyright (c) 2012 VMware, Inc.

package gonatsd_test

import (
	"bufio"
	"code.google.com/p/gomock/gomock"
	. "gonatsd/gonatsd"
	. "gonatsd/gonatsd/mocks"
	"io"
	. "launchpad.net/gocheck"
	"time"
)

type ConnSuite struct {
	delegate        *TestReporterDelegate
	ctrl            *gomock.Controller
	conn            Conn
	tcpConn         *DummyTCPConn
	started         bool
	server          *MockServer
	serverCmds      chan ServerCmd
	heartbeatHelper *MockHeartbeatHelper
	pingTicker      chan time.Time
}

var _ = Suite(&ConnSuite{})

func (s *ConnSuite) SetUpTest(c *C) {
	s.delegate = &TestReporterDelegate{}
	s.ctrl = gomock.NewController(s.delegate)

	s.pingTicker = make(chan time.Time, 1)
	s.tcpConn = NewDummyTCPConn()
	s.serverCmds = make(chan ServerCmd, 1)

	s.heartbeatHelper = NewMockHeartbeatHelper(s.ctrl)
	s.heartbeatHelper.EXPECT().Ticker().Return(s.pingTicker).AnyTimes()
	s.heartbeatHelper.EXPECT().Stop().AnyTimes()
	NewHeartbeatHelper = func(Conn, time.Duration, int) HeartbeatHelper {
		return s.heartbeatHelper
	}

	config := &Config{}
	config.Limits.ControlLine = 20
	config.Limits.Pending = 1024

	s.server = NewMockServer(s.ctrl)
	s.server.EXPECT().Config().Return(config).AnyTimes()
	s.server.EXPECT().Info().Return(&dummyInfo).AnyTimes()
	s.server.EXPECT().Commands().Return(s.serverCmds).AnyTimes()
	s.server.EXPECT().Stats().Return(NewStats()).AnyTimes()
}

func (s *ConnSuite) TearDownTest(c *C) {
	NewHeartbeatHelper = NewRealHeartbeatHelper
	BUF_IO_SIZE = 262144

	if s.conn != nil {
		if !s.conn.Closed() {
			// Dummy server unregister mock
			go func() {
				cmd := <-s.serverCmds
				switch cmd.(type) {
				case *UnregisterConnCmd:
					cmd := cmd.(*UnregisterConnCmd)
					cmd.Done <- true
				}
			}()

			// Consume any unread output so write can finish
			go func() {
				buf := make([]byte, 1024)
				for {
					_, err := s.tcpConn.client.Read(buf)
					if err != nil {
						break
					}
				}
			}()

			// Instead of just closing connection we schedule a CLOSE_CMD
			// in order to let dispatch loop read it, otherwise there is
			// a possible race condition: dispatch loop might kick in after
			// teardown closes the connection.
			s.conn.ServeCommand(CLOSE_CMD)
		}
	}
}

func (s *ConnSuite) TestNewConn(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.server.Config().Ping.IntervalDuration = 5 * time.Second
	s.server.Config().Ping.MaxOutstanding = 7

	NewHeartbeatHelper = func(conn Conn, interval time.Duration, maxOutstanding int) HeartbeatHelper {
		c.Check(interval, Equals, 5*time.Second)
		c.Check(maxOutstanding, Equals, 7)
		return s.heartbeatHelper
	}
	s.conn = NewConn(s.server, s.tcpConn)
}

func (s *ConnSuite) TestReadControlLine(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)

	go func() {
		io.WriteString(s.tcpConn.client, "Hello\r\n\r\n")
		s.tcpConn.client.Close()
	}()

	line, err := s.conn.ReadControlLine()
	c.Check(err, IsNil)
	c.Check(line, Equals, "Hello")
	line, err = s.conn.ReadControlLine()
	c.Check(err, IsNil)
	c.Check(line, Equals, "")
	line, err = s.conn.ReadControlLine()
	c.Check(err, Equals, io.EOF)
	c.Check(line, Equals, "")
}

func (s *ConnSuite) TestReadControlLineTooLong(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)

	s.server.Config().Limits.ControlLine = 4

	go func() {
		io.WriteString(s.tcpConn.client, "1234\r\n12345\r\n")
		s.tcpConn.client.Close()
	}()

	line, err := s.conn.ReadControlLine()
	c.Check(err, IsNil)
	c.Check(line, Equals, "1234")
	line, err = s.conn.ReadControlLine()
	c.Check(err, Equals, ErrProtocolOpTooBig)
}

func (s *ConnSuite) TestReadControlLineTooLongPartial(c *C) {
	BUF_IO_SIZE = 16

	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)

	go func() {
		io.WriteString(s.tcpConn.client, "12345678901234567890123456789012\r\n")
		s.tcpConn.client.Close()
	}()

	_, err := s.conn.ReadControlLine()
	c.Check(err, Equals, ErrProtocolOpTooBig)
}

func (s *ConnSuite) TestMessageDispatch(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	sm := &SubscribedMessage{&Subscription{}, &Message{"foo", "X", []byte("msg")}, false}
	s.conn.ServeMessage(sm)

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
	checkReadLine(c, reader, "MSG foo 0 X 3")
	checkReadLine(c, reader, "msg")
}

type TestRequest struct{}

func (t *TestRequest) Serve(c Conn) *Response {
	return NewStringResponse("TestReq")
}

func (t *TestRequest) Dispatch(c Conn) {
	c.ServeRequest(t)
}

func (s *ConnSuite) TestRequestDispatch(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	s.conn.ServeRequest(&TestRequest{})

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
	checkReadLine(c, reader, "TestReq")
}

type TestClientCmd struct{}

func (t *TestClientCmd) Process(c Conn) {
	c.Write(NewStringResponse("TestCmd"))
}

func (s *ConnSuite) TestCommandDispatch(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	s.conn.ServeCommand(&TestClientCmd{})

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
	checkReadLine(c, reader, "TestCmd")
}

func (s *ConnSuite) TestHeartbeatDispatch(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	var barrier = make(chan bool, 1)
	s.heartbeatHelper.EXPECT().Ping().Do(func() {
		barrier <- true
	})

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	s.pingTicker <- time.Now()

	// Give it a chance to be dispatched
	select {
	case <-barrier:
	case <-time.After(time.Second):
	}

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
}

func (s *ConnSuite) TestReadLoopTooBig(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	s.server.Config().Limits.ControlLine = 4

	go func() {
		cmd := <-s.serverCmds
		switch cmd.(type) {
		case *UnregisterConnCmd:
			cmd := cmd.(*UnregisterConnCmd)
			cmd.Done <- true
		}
	}()

	io.WriteString(s.tcpConn.client, "TESTCOMMAND\r\n")

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
	checkReadLine(c, reader, "-ERR 'Protocol Operation size exceeded'")
}

func (s *ConnSuite) TestReadRequest(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	io.WriteString(s.tcpConn.client, "PING\r\n")

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
	checkReadLine(c, reader, "PONG")
}

func (s *ConnSuite) TestReadBadRequest(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	io.WriteString(s.tcpConn.client, "PING foo\r\n")

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
	checkReadLine(c, reader, "-ERR 'Unknown Protocol Operation'")
}

func (s *ConnSuite) TestReadUnknownRequest(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	io.WriteString(s.tcpConn.client, "HELLO\r\n")

	reader := bufio.NewReader(s.tcpConn.client)
	reader.ReadLine()
	checkReadLine(c, reader, "-ERR 'Unknown Protocol Operation'")
}

func (s *ConnSuite) TestClientClosed(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	s.tcpConn.client.Close()
	serverCmd := <-s.serverCmds
	(serverCmd.(*UnregisterConnCmd)).Done <- true
}

type TestServerCmd struct{}

func (c *TestServerCmd) Process(Server) {

}

func (s *ConnSuite) TestSendServerCmd(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	s.conn.SendServerCmd(&TestServerCmd{})

	select {
	case <-s.serverCmds:
	case <-time.After(time.Second):
		c.Errorf("Should have received server command")
	}
}

type CallbackClientCmd struct {
	cb func()
}

func (c *CallbackClientCmd) Process(conn Conn) {
	if c.cb != nil {
		c.cb()
	}
}

func (s *ConnSuite) TestSendServerCmdFull(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	barrier := make(chan bool, 1)

	s.conn.ServeCommand(&CallbackClientCmd{cb: func() {
		// Fill up server channel
		s.serverCmds <- &TestServerCmd{}

		go func() {
			// Fill Up connection channel
			sm := &SubscribedMessage{}
			sm.Message = &Message{}
			sm.Subscription = &Subscription{}
			for i := 0; i < MAX_CONN_CHAN_BACKLOG+1; i++ {
				s.conn.ServeMessage(sm)
			}
			// Process a server command to unblock connection from sending
			<-s.serverCmds
		}()

		s.conn.SendServerCmd(&TestServerCmd{})

		select {
		case <-s.serverCmds:
		case <-time.After(time.Second):
			c.Errorf("Should have received server command")
		}

		barrier <- true
	}})

	<-barrier
}

func (s *ConnSuite) TestWriteFull(c *C) {
	s.delegate.Set(c)
	defer s.ctrl.Finish()

	s.server.Config().Limits.Pending = 16
	s.conn = NewConn(s.server, s.tcpConn)
	go s.conn.Start()

	reader := bufio.NewReader(s.tcpConn.client)

	value := "12345678901234567890"

	reader.ReadLine()

	go func() {
		cmd := <-s.serverCmds
		switch cmd.(type) {
		case *UnregisterConnCmd:
			cmd := cmd.(*UnregisterConnCmd)
			cmd.Done <- true
		}
	}()

	go s.conn.Write(NewStringResponse(value))
	checkReadLine(c, reader, "-ERR 'Slow consumer detected, connection dropped'")
}

func checkReadLine(c *C, reader *bufio.Reader, expected string) {
	line, prefix, err := reader.ReadLine()
	c.Check(err, IsNil)
	c.Check(prefix, Equals, false)
	c.Check(string(line), Equals, expected)
}
