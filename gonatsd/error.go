// Copyright (c) 2012 VMware, Inc.

package gonatsd

type NATSError struct {
	Message string
	Close   bool
}

func (e *NATSError) Error() string {
	return e.Message
}

var (
	ErrPayloadTooBig     = &NATSError{"-ERR 'Payload size exceeded'", true}
	ErrProtocolOpTooBig  = &NATSError{"-ERR 'Protocol Operation size exceeded'", true}
	ErrInvalidSubject    = &NATSError{"-ERR 'Invalid Subject'", false}
	ErrInvalidSidTaken   = &NATSError{"-ERR 'Invalid Subject Identifier (sid), already taken'", false}
	ErrInvalidSidNoexist = &NATSError{"-ERR 'Invalid Subject-Identifier (sid), no subscriber registered'", false}
	ErrInvalidConfig     = &NATSError{"-ERR 'Invalid config, valid JSON required for connection configuration'", false}
	ErrAuthRequired      = &NATSError{"-ERR 'Authorization is required'", true}
	ErrAuthFailed        = &NATSError{"-ERR 'Authorization failed'", true}
	ErrUnknownOp         = &NATSError{"-ERR 'Unknown Protocol Operation'", false}
	ErrSlowConsumer      = &NATSError{"-ERR 'Slow consumer detected, connection dropped'", true}
	ErrUnresponsive      = &NATSError{"-ERR 'Unresponsive client detected, connection dropped'", true}
	ErrMaxConnsExceeded  = &NATSError{"-ERR 'Maximum client connections exceeded, connection dropped'", true}
)
