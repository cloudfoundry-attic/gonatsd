// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"io"
)

// NATS response payload.
// Instead of having a single string or byte array we can pass the original
// message instead of copying it to the client.
type Response struct {
	Value *string
	Bytes *[]byte
}

func NewStringResponse(value string) *Response {
	return &Response{Value: &value}
}

func NewByteResponse(bytes []byte) *Response {
	return &Response{Bytes: &bytes}
}

func NewResponse(value string, bytes []byte) *Response {
	return &Response{&value, &bytes}
}

func (r *Response) Size() (result int32) {
	if r.Value != nil {
		result += int32(len(*r.Value))
	}

	if r.Bytes != nil {
		result += int32(len(*r.Bytes))
	}
	return
}

func (r *Response) Write(writer io.Writer) (err error) {
	if r.Value != nil {
		_, err = io.WriteString(writer, *r.Value)
		if err != nil {
			return
		}
	}
	if r.Bytes != nil {
		_, err = writer.Write(*r.Bytes)
		if err != nil {
			return
		}
	}
	_, err = io.WriteString(writer, "\r\n")
	if err != nil {
		return
	}
	return
}
