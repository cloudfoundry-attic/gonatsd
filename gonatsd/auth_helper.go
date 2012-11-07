// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	log "github.com/cihub/seelog"
	"time"
)

type AuthHelper struct {
	conn       Conn
	users      map[string]string
	timer      *time.Timer
	channel    <-chan time.Time
	authorized bool
}

// Create a new AuthHelper for the connection with the specified user map and auth timeout.
func NewAuthHelper(conn Conn, users map[string]string, timeout time.Duration) *AuthHelper {
	h := &AuthHelper{conn: conn, users: users}
	if len(users) > 0 {
		if timeout > 0 {
			h.timer = time.NewTimer(timeout)
			h.channel = h.timer.C
		}
	} else {
		h.authorized = true
	}
	return h
}

// Authenticate and authorize the request.
// Returns true iff the request is authed, otherwise false with an error.
func (h *AuthHelper) Auth(req Request) (bool, *NATSError) {
	if !h.authorized {
		switch req.(type) {
		case *ConnectRequest:
			request := req.(*ConnectRequest)
			if request.User == nil || request.Password == nil {
				log.Tracef("[client %s] did not send credentials", h.conn.RemoteAddr())
				return false, ErrAuthRequired
			}

			password, ok := h.users[*request.User]
			if ok && password == *request.Password {
				log.Tracef("[client %s] authenticated with: %s", h.conn.RemoteAddr(), *request.User)
				h.authorized = true
				h.Stop()
				return true, nil
			} else {
				log.Tracef("[client %s] sent wrong credentials", h.conn.RemoteAddr())
				return false, ErrAuthFailed
			}
		default:
			log.Tracef("[client %s] did not send credentials", h.conn.RemoteAddr())
			return false, ErrAuthRequired
		}
	}
	return true, nil
}

// Stop the AuthHelper.
// Must be called to cleanup the internal timers.
func (h *AuthHelper) Stop() {
	if h.timer != nil {
		h.timer.Stop()
		h.timer = nil
		h.channel = nil
	}
}

// Returns the internal timer channel, which should be used to call Timeout().
// It's exposed to avoid an additional goroutine, instead it can be handled
// from a single select statement.
func (h *AuthHelper) Timer() <-chan time.Time {
	return h.channel
}

// Initiates auth timeout when the Timer() fires.
func (h *AuthHelper) Timeout() {
	h.conn.CloseWithError(ErrAuthRequired)
}
