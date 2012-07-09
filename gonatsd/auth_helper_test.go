// Copyright (c) 2012 VMware, Inc.

package gonatsd_test

import (
	"code.google.com/p/gomock/gomock"
	. "gonatsd/gonatsd"
	. "gonatsd/gonatsd/mocks"
	. "launchpad.net/gocheck"
)

type AuthHelperSuite struct{}

var _ = Suite(&AuthHelperSuite{})

func (s *AuthHelperSuite) TestNoAuth(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{}, 0)
	defer helper.Stop()

	authed, err := helper.Auth(PING_REQUEST)
	c.Check(authed, Equals, true)
	c.Check(err, IsNil)
}

func (s *AuthHelperSuite) TestAuth(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 0)
	defer helper.Stop()

	user := "foo"
	password := "bar"

	authed, err := helper.Auth(&ConnectRequest{User: &user, Password: &password})
	c.Check(authed, Equals, true)
	c.Check(err, IsNil)

	// Check to make sure further requests don't need to be authed
	authed, err = helper.Auth(PING_REQUEST)
	c.Check(authed, Equals, true)
	c.Check(err, IsNil)
}

func (s *AuthHelperSuite) TestNoCreds(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 0)
	defer helper.Stop()

	authed, err := helper.Auth(new(ConnectRequest))
	c.Check(authed, Equals, false)
	c.Check(err, Equals, ErrAuthRequired)
}

func (s *AuthHelperSuite) TestNoConnectReq(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 0)
	defer helper.Stop()

	authed, err := helper.Auth(PING_REQUEST)
	c.Check(authed, Equals, false)
	c.Check(err, Equals, ErrAuthRequired)
}

func (s *AuthHelperSuite) TestWrongCreds(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 0)
	defer helper.Stop()

	user := "foo"
	password := "boz"

	authed, err := helper.Auth(&ConnectRequest{User: &user, Password: &password})
	c.Check(authed, Equals, false)
	c.Check(err, Equals, ErrAuthFailed)
}

func (s *AuthHelperSuite) TestTimer(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 1)
	defer helper.Stop()

	c.Check(helper.Timer(), NotNil)
}

func (s *AuthHelperSuite) TestNoTimer(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 0)
	defer helper.Stop()

	c.Check(helper.Timer(), IsNil)
}

func (s *AuthHelperSuite) TestStop(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 1)
	c.Check(helper.Timer(), NotNil)
	helper.Stop()
	c.Check(helper.Timer(), IsNil)
}

func (s *AuthHelperSuite) TestTimeout(c *C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	conn.EXPECT().RemoteAddr().AnyTimes().Return(&DummyAddr{})
	conn.EXPECT().CloseWithError(ErrAuthRequired)
	helper := NewAuthHelper(conn, map[string]string{"foo": "bar"}, 1)
	defer helper.Stop()

	helper.Timeout()
}
