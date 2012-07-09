// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"errors"
	"sync/atomic"
)

var (
	ErrQueueFull   = errors.New("Full Queue")
	ErrQueueClosed = errors.New("Closed Queue")
)

const (
	queueBacklog = 32
)

type HasSize interface {
	Size() int32
}

type BoundedQueue struct {
	in        chan HasSize   // incoming elements
	out       chan []HasSize // next batch for dequeuing 
	buffer    []HasSize      // temp buffer to accumulate elements in between queue and dequeue
	pending   []HasSize      // current batch for dequeuing
	totalSize int32          // current queue size
	maxSize   int32          // max allowed queue size
	closed    bool
}

// Create a new BoundedQueue with the specified max size
func NewBoundedQueue(maxSize int32) *BoundedQueue {
	q := &BoundedQueue{}
	q.maxSize = maxSize
	q.in = make(chan HasSize, queueBacklog)
	q.out = make(chan []HasSize, 1)
	q.buffer = make([]HasSize, 0, queueBacklog)
	q.pending = make([]HasSize, 0, queueBacklog)
	go q.loop()
	return q
}

// Enqueue element.
func (q *BoundedQueue) Enqueue(o HasSize) error {
	totalSize := atomic.AddInt32(&q.totalSize, o.Size())
	if totalSize > q.maxSize {
		return ErrQueueFull
	}
	q.in <- o
	return nil
}

// Dequeue element. Will block until there is something to dequeue.
func (q *BoundedQueue) Dequeue() (HasSize, error) {
	if q.closed {
		return nil, ErrQueueClosed
	}

	if len(q.pending) == 0 {
		var ok bool
		select {
		case q.pending, ok = <-q.out:
			if !ok {
				return nil, ErrQueueClosed
			}
		}
	}

	o := q.pending[0]
	atomic.AddInt32(&q.totalSize, -o.Size())
	q.pending = q.pending[1:]
	return o, nil
}

// Returns true if the queue has more elements to dequeue without blocking.
func (q *BoundedQueue) HasMore() bool {
	if q.closed {
		return false
	}

	if len(q.pending) > 0 {
		return true
	}

	var ok bool
	select {
	case q.pending, ok = <-q.out:
		if !ok {
			return false
		}
		return true
	default:
	}
	return false
}

// Close the queue. Must be called to cleanup the internal goroutine.
func (q *BoundedQueue) Close() {
	if !q.closed {
		q.closed = true
		close(q.in)
	}
}

func (q *BoundedQueue) loop() {
	var out chan []HasSize
	pending := q.pending

	for {
		select {
		case o, ok := <-q.in:
			if !ok {
				close(q.out)
				return
			}
			q.buffer = append(q.buffer, o)
			out = q.out
		case out <- q.buffer:
			tmp := q.buffer
			q.buffer = pending[:0]
			pending = tmp
			out = nil
		}
	}
}
