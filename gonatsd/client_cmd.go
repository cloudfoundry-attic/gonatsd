// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	log "github.com/cihub/seelog"
)

// Simple interface for commands that need to be handled by the 
// client dispatch loop.
type ClientCmd interface {

	// Process the command in context of the client dispatch loop.
	Process(Conn)
}

// Close the connection
type CloseCmd struct {
}

func (c *CloseCmd) Process(conn Conn) {
	conn.Close()
}

// Close the connection due to a low level error.
type ErrorCmd struct {
	Error error
}

func (c *ErrorCmd) Process(conn Conn) {
	err := c.Error
	if !conn.Closed() {
		log.Warn("[client %s]: error: %s", conn.RemoteAddr(), err)
		conn.Close()
	}
}

var (
	CLOSE_CMD = &CloseCmd{}
)
