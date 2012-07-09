// Copyright (c) 2012 VMware, Inc.

package gonatsd_test

import (
	"code.google.com/p/gomock/gomock"
	. "gonatsd/gonatsd"
	. "gonatsd/gonatsd/mocks"
	"io"
	. "launchpad.net/gocheck"
	"time"
)

type RequestSuite struct{}

var _ = Suite(&RequestSuite{})

func (s *RequestSuite) TestPingParse(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParsePingRequest(conn, "")
	c.Check(err, IsNil)
	c.Check(req, DeepEquals, &PingRequest{})
}

func (s *RequestSuite) TestBadPingParse(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParsePingRequest(conn, "extra")
	c.Check(err, Equals, ErrUnknownOp)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPingServe(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)

	req, _ := ParsePingRequest(conn, "")
	resp := req.Serve(conn)

	c.Check(resp, DeepEquals, NewStringResponse("PONG"))
}

func (s *RequestSuite) TestPingDispatch(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, _ := ParsePingRequest(conn, "")
	conn.EXPECT().ServeRequest(req)

	req.Dispatch(conn)
}

func (s *RequestSuite) TestPongParse(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParsePongRequest(conn, "")
	c.Check(err, IsNil)
	c.Check(req, DeepEquals, &PongRequest{})
}

func (s *RequestSuite) TestBadPongParse(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParsePongRequest(conn, "extra")
	c.Check(err, Equals, ErrUnknownOp)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPongServe(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	heartbeatHelper := NewMockHeartbeatHelper(ctrl)
	conn.EXPECT().HeartbeatHelper().Return(heartbeatHelper).AnyTimes()
	heartbeatHelper.EXPECT().Pong()

	req, _ := ParsePongRequest(conn, "")
	resp := req.Serve(conn)
	c.Check(resp, IsNil)
}

func (s *RequestSuite) TestPongDispatch(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, _ := ParsePongRequest(conn, "")
	conn.EXPECT().ServeRequest(req)

	req.Dispatch(conn)
}

func (s *RequestSuite) TestInfoParse(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParseInfoRequest(conn, "")
	c.Check(err, IsNil)
	c.Check(req, DeepEquals, &InfoRequest{})
}

func (s *RequestSuite) TestBadInfoParse(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParseInfoRequest(conn, "extra")
	c.Check(err, Equals, ErrUnknownOp)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestInfoServe(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	server := NewMockServer(ctrl)
	server.EXPECT().Info().Return(&dummyInfo).AnyTimes()
	conn.EXPECT().Server().Return(server).AnyTimes()

	req, _ := ParseInfoRequest(conn, "")
	resp := req.Serve(conn)
	c.Check(resp, DeepEquals, NewResponse("INFO ", dummyInfo))
}

func (s *RequestSuite) TestInfoDispatch(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, _ := ParseInfoRequest(conn, "")
	conn.EXPECT().ServeRequest(req)

	req.Dispatch(conn)
}

func (s *RequestSuite) TestPublishParse(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	config := &Config{}
	config.Limits.Payload = 100
	options := &ConnOptions{}
	server := NewMockServer(ctrl)
	server.EXPECT().Config().Return(config).AnyTimes()
	conn := NewMockConn(ctrl)
	conn.EXPECT().Options().Return(options).AnyTimes()
	conn.EXPECT().Server().Return(server).AnyTimes()
	conn.EXPECT().Read(gomock.Any()).Return(100, nil).Do(func(buf []byte) {
		copy(buf, []byte("TEST"))
	}).Times(2)
	conn.EXPECT().ReadControlLine().Times(2)

	req, err := ParsePublishRequest(conn, "FOO 4")
	c.Check(err, IsNil)
	c.Check(req, DeepEquals, &PublishRequest{
		&Message{Subject: "FOO", Content: []byte("TEST")}})

	req, err = ParsePublishRequest(conn, "FOO inbox 4")
	c.Check(err, IsNil)
	c.Check(req, DeepEquals, &PublishRequest{
		&Message{Subject: "FOO", ReplyTo: "inbox", Content: []byte("TEST")}})
}

func (s *RequestSuite) TestPublishParseNoArgs(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParsePublishRequest(conn, "")
	c.Check(err, Equals, ErrUnknownOp)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPublishParseBadLength(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req, err := ParsePublishRequest(conn, "FOO BAR")
	c.Check(err, Equals, ErrUnknownOp)
	c.Check(req, IsNil)

	req, err = ParsePublishRequest(conn, "FOO inbox BAR")
	c.Check(err, Equals, ErrUnknownOp)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPublishParseTooBig(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	config := &Config{}
	config.Limits.Payload = 100
	server := NewMockServer(ctrl)
	server.EXPECT().Config().Return(config).AnyTimes()
	conn := NewMockConn(ctrl)
	conn.EXPECT().Server().Return(server).AnyTimes()

	req, err := ParsePublishRequest(conn, "FOO 101")
	c.Check(err, Equals, ErrPayloadTooBig)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPublishParseBadRead(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	config := &Config{}
	config.Limits.Payload = 100
	server := NewMockServer(ctrl)
	server.EXPECT().Config().Return(config).AnyTimes()
	conn := NewMockConn(ctrl)
	conn.EXPECT().Server().Return(server).AnyTimes()
	conn.EXPECT().Read(gomock.Any()).Return(0, io.EOF)

	req, err := ParsePublishRequest(conn, "FOO 20")
	c.Check(err, Equals, io.EOF)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPublishParseBadRead2(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	config := &Config{}
	config.Limits.Payload = 100
	server := NewMockServer(ctrl)
	server.EXPECT().Config().Return(config).AnyTimes()
	conn := NewMockConn(ctrl)
	conn.EXPECT().Server().Return(server).AnyTimes()
	conn.EXPECT().Read(gomock.Any()).Return(100, nil).Do(func(buf []byte) {
		copy(buf, []byte("TEST"))
	}).Times(2)

	conn.EXPECT().ReadControlLine().Return("hi", nil)
	req, err := ParsePublishRequest(conn, "FOO 4")
	c.Check(err, Equals, ErrUnknownOp)
	c.Check(req, IsNil)

	conn.EXPECT().ReadControlLine().Return("", io.EOF)
	req, err = ParsePublishRequest(conn, "FOO 4")
	c.Check(err, Equals, io.EOF)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPublishParseBadSubject(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	config := &Config{}
	config.Limits.Payload = 100
	options := &ConnOptions{Pedantic: true}
	server := NewMockServer(ctrl)
	server.EXPECT().Config().Return(config).AnyTimes()
	conn := NewMockConn(ctrl)
	conn.EXPECT().Options().Return(options).AnyTimes()
	conn.EXPECT().Server().Return(server).AnyTimes()
	conn.EXPECT().Read(gomock.Any()).Return(100, nil).Do(func(buf []byte) {
		copy(buf, []byte("TEST"))
	})
	conn.EXPECT().ReadControlLine()

	req, err := ParsePublishRequest(conn, "FOO.>.> 4")
	c.Check(err, Equals, ErrInvalidSubject)
	c.Check(req, IsNil)
}

func (s *RequestSuite) TestPublishServe(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	req := &PublishRequest{}
	resp := req.Serve(conn)
	c.Check(resp, DeepEquals, NewStringResponse("OK"))
}

func (s *RequestSuite) TestPublishDispatch(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	serverCmds := make(chan ServerCmd, 1)
	server := NewMockServer(ctrl)
	server.EXPECT().Commands().Return(serverCmds).AnyTimes()

	options := &ConnOptions{}
	conn := NewMockConn(ctrl)
	conn.EXPECT().Options().Return(options).AnyTimes()
	conn.EXPECT().Server().Return(server).AnyTimes()

	msg := &Message{Subject: "Foo"}
	req := &PublishRequest{msg}
	req.Dispatch(conn)

	select {
	case cmd := <-serverCmds:
		c.Check(cmd, DeepEquals, &PublishCmd{msg})
	case <-time.After(time.Second):
		c.Errorf("Did not dispatch server command")
	}
}

func (s *RequestSuite) TestPublishDispatchVerbose(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	serverCmds := make(chan ServerCmd, 1)
	server := NewMockServer(ctrl)
	server.EXPECT().Commands().Return(serverCmds).AnyTimes()

	options := &ConnOptions{Verbose: true}
	conn := NewMockConn(ctrl)
	conn.EXPECT().Options().Return(options).AnyTimes()
	conn.EXPECT().Server().Return(server).AnyTimes()

	msg := &Message{Subject: "Foo"}
	req := &PublishRequest{msg}
	conn.EXPECT().ServeRequest(req)
	req.Dispatch(conn)

	select {
	case cmd := <-serverCmds:
		c.Check(cmd, DeepEquals, &PublishCmd{msg})
	case <-time.After(time.Second):
		c.Errorf("Did not dispatch server command")
	}
}
