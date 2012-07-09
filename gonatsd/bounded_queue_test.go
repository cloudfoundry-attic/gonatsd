// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	. "launchpad.net/gocheck"
	"time"
)

type BoundedQueueSuite struct{}

var _ = Suite(&BoundedQueueSuite{})

type DummySizedObject struct {
	size int32
}

func (o *DummySizedObject) Size() int32 {
	return o.size
}

func (s *BoundedQueueSuite) TestEnqueue(c *C) {
	q := NewBoundedQueue(10)
	defer q.Close()

	o := &DummySizedObject{1}
	err := q.Enqueue(o)
	c.Check(err, IsNil)

	done := make(chan bool, 1)
	go func() {
		dequeued, err := q.Dequeue()
		c.Check(err, IsNil)
		c.Check(dequeued, Equals, o)
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		c.Error("Failed to dequeue object")
	}
}

func (s *BoundedQueueSuite) TestEnqueueFull(c *C) {
	q := NewBoundedQueue(10)
	defer q.Close()

	err := q.Enqueue(&DummySizedObject{5})
	c.Check(err, IsNil)

	err = q.Enqueue(&DummySizedObject{5})
	c.Check(err, IsNil)

	err = q.Enqueue(&DummySizedObject{5})
	c.Check(err, Equals, ErrQueueFull)
}

func (s *BoundedQueueSuite) TestDequeueClosed(c *C) {
	q := NewBoundedQueue(10)
	q.Close()
	_, err := q.Dequeue()
	c.Check(err, Equals, ErrQueueClosed)
}

func (s *BoundedQueueSuite) TestDequeueChanClosed(c *C) {
	q := NewBoundedQueue(10)
	q.Close()
	q.closed = false

	_, err := q.Dequeue()
	c.Check(err, Equals, ErrQueueClosed)
}

func (s *BoundedQueueSuite) TestHasMorePending(c *C) {
	q := NewBoundedQueue(10)
	defer q.Close()

	q.pending = append(q.pending, &DummySizedObject{5})
	c.Check(q.HasMore(), Equals, true)
}

func (s *BoundedQueueSuite) TestHasMoreChan(c *C) {
	q := NewBoundedQueue(10)
	defer q.Close()

	buffer := []HasSize{&DummySizedObject{1}}
	q.out <- buffer
	c.Check(q.HasMore(), Equals, true)
}

func (s *BoundedQueueSuite) TestHasMoreChanClosed(c *C) {
	q := NewBoundedQueue(10)
	q.Close()
	q.closed = false

	// Wait for loop to close the chan
	select {
	case _, err := <-q.in:
		c.Check(err, NotNil)
	}

	c.Check(q.HasMore(), Equals, false)
}

func (s *BoundedQueueSuite) TestHasMoreEmpty(c *C) {
	q := NewBoundedQueue(10)
	defer q.Close()
	c.Check(q.HasMore(), Equals, false)
}

func (s *BoundedQueueSuite) TestHasMoreClosed(c *C) {
	q := NewBoundedQueue(10)
	q.Close()
	c.Check(q.HasMore(), Equals, false)
}
