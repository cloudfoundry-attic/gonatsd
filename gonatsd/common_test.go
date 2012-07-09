// Copyright (c) 2012 VMware, Inc.

package gonatsd_test

import (
	"code.google.com/p/gomock/gomock"
	"gonatsd/gonatsd"
	"io"
	. "launchpad.net/gocheck"
	"net"
	"testing"
	"time"
)

// Glue from gocheck to testing
func Test(t *testing.T) { TestingT(t) }

// Cache real factory so it can be reset after we mock it out.
var NewRealHeartbeatHelper = gonatsd.NewHeartbeatHelper

// Fake connection address for testing
type DummyAddr struct{}

func (a *DummyAddr) Network() string {
	return "dummy"
}

func (a *DummyAddr) String() string {
	return "dummy"
}

// Fake server info
var dummyInfo = []byte("Hello")

// Fake TCP connection for testing
type DummyTCPConn struct {
	client     net.Conn
	server     net.Conn
	remote     net.Addr
	closed     bool
	closedRead bool
}

func NewDummyTCPConn() *DummyTCPConn {
	c := new(DummyTCPConn)
	c.client, c.server = net.Pipe()
	return c
}

func (c *DummyTCPConn) Read(b []byte) (int, error) {
	if c.closedRead {
		return 0, io.EOF
	}
	return c.server.Read(b)
}

func (c *DummyTCPConn) Write(b []byte) (int, error) {
	return c.server.Write(b)
}

func (c *DummyTCPConn) Close() error {
	c.server.Close()
	return nil
}

func (c *DummyTCPConn) LocalAddr() net.Addr {
	return c.server.LocalAddr()
}

func (c *DummyTCPConn) RemoteAddr() net.Addr {
	return c.server.RemoteAddr()
}

func (c *DummyTCPConn) CloseRead() error {
	c.closedRead = true
	return nil
}

func (c *DummyTCPConn) SetDeadline(t time.Time) error {
	return c.server.SetDeadline(t)
}

func (c *DummyTCPConn) SetReadDeadline(t time.Time) error {
	return c.server.SetReadDeadline(t)
}

func (c *DummyTCPConn) SetWriteDeadline(t time.Time) error {
	return c.server.SetWriteDeadline(t)
}

// Delegate to be able to create mocks in fixtures but report errors
// to the current test case.
type TestReporterDelegate struct {
	delegate gomock.TestReporter
}

func (d *TestReporterDelegate) Set(delegate gomock.TestReporter) {
	d.delegate = delegate
}

func (d *TestReporterDelegate) Errorf(format string, args ...interface{}) {
	if d.delegate != nil {
		d.delegate.Errorf(format, args)
	}
}

func (d *TestReporterDelegate) Fatalf(format string, args ...interface{}) {
	if d.delegate != nil {
		d.delegate.Fatalf(format, args)
	}
}
