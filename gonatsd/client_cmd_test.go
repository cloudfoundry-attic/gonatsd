// Copyright (c) 2012 VMware, Inc.

package gonatsd_test

import (
	"code.google.com/p/gomock/gomock"
	. "gonatsd/gonatsd"
	. "gonatsd/gonatsd/mocks"
	"io"
	. "launchpad.net/gocheck"
)

type ClientCmdSuite struct{}

var _ = Suite(&ClientCmdSuite{})

func (s *ClientCmdSuite) TestCloseCmd(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().Close()
	CLOSE_CMD.Process(conn)
}

func (s *ClientCmdSuite) TestErrorCmd(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().Return(&DummyAddr{}).AnyTimes()
	conn.EXPECT().Closed().Return(false)
	conn.EXPECT().Close()
	cmd := &ErrorCmd{io.EOF}
	cmd.Process(conn)
}
