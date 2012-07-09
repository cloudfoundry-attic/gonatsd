// Copyright (c) 2012 VMware, Inc.

package gonatsd_test

import (
	"code.google.com/p/gomock/gomock"
	. "gonatsd/gonatsd"
	. "gonatsd/gonatsd/mocks"
	. "launchpad.net/gocheck"
	"time"
)

type HeartbeatHelperSuite struct{}

var _ = Suite(&HeartbeatHelperSuite{})

func (s *HeartbeatHelperSuite) TestPing(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	helper := NewHeartbeatHelper(conn, 1*time.Second, 3)
	defer helper.Stop()

	conn.EXPECT().Write(NewStringResponse(PING))
	helper.Ping()
}

func (s *HeartbeatHelperSuite) TestInterval(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	helper := NewHeartbeatHelper(conn, 1*time.Second, 3)
	defer helper.Stop()
	c.Check(helper.Ticker(), NotNil)
}

func (s *HeartbeatHelperSuite) TestNoInterval(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	helper := NewHeartbeatHelper(conn, 0, 3)
	defer helper.Stop()
	c.Check(helper.Ticker(), IsNil)
}

func (s *HeartbeatHelperSuite) TestPingUnresponsive(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	helper := NewHeartbeatHelper(conn, 1*time.Second, 2)
	defer helper.Stop()

	conn.EXPECT().Write(gomock.Eq(NewStringResponse(PING))).AnyTimes()
	conn.EXPECT().CloseWithError(ErrUnresponsive)
	helper.Ping()
	helper.Ping()
	helper.Ping()
}

func (s *HeartbeatHelperSuite) TestPong(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	helper := NewHeartbeatHelper(conn, 1*time.Second, 1)
	defer helper.Stop()

	conn.EXPECT().Write(gomock.Eq(NewStringResponse(PING))).AnyTimes()
	helper.Ping()
	helper.Pong()
	helper.Ping()
	helper.Pong()
}

func (s *HeartbeatHelperSuite) TestPongNeg(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	helper := NewHeartbeatHelper(conn, 1*time.Second, 1)
	defer helper.Stop()

	conn.EXPECT().Write(gomock.Eq(NewStringResponse(PING))).AnyTimes()
	conn.EXPECT().CloseWithError(ErrUnresponsive)
	helper.Pong()
	helper.Pong()
	helper.Ping()
	helper.Ping()
}

func (s *HeartbeatHelperSuite) TestStop(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	helper := NewHeartbeatHelper(conn, 1*time.Second, 3)
	helper.Stop()
	c.Check(helper.Ticker(), IsNil)
}
