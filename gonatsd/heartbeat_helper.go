// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"time"
)

type HeartbeatHelper interface {
	Ticker() <-chan time.Time
	Ping()
	Pong()
	Stop()
}

type heartbeatHelper struct {
	conn           Conn
	ticker         *time.Ticker
	channel        <-chan time.Time
	outstanding    int
	maxOutstanding int
}

func newHeartbeatHelper(conn Conn, interval time.Duration, maxOutstanding int) HeartbeatHelper {
	helper := &heartbeatHelper{conn: conn, maxOutstanding: maxOutstanding}

	if interval > 0 {
		helper.ticker = time.NewTicker(interval)
		helper.channel = helper.ticker.C
	}

	return helper
}

var NewHeartbeatHelper = newHeartbeatHelper

func (h *heartbeatHelper) Ticker() <-chan time.Time {
	return h.channel
}

func (h *heartbeatHelper) Ping() {
	h.outstanding++
	if h.outstanding > h.maxOutstanding {
		h.conn.CloseWithError(ErrUnresponsive)
		return
	}
	h.conn.Write(NewStringResponse(PING))
}

func (h *heartbeatHelper) Pong() {
	if h.outstanding > 0 {
		h.outstanding--
	}
}

func (h *heartbeatHelper) Stop() {
	if h.ticker != nil {
		h.ticker.Stop()
		h.ticker = nil
		h.channel = nil
	}
}
