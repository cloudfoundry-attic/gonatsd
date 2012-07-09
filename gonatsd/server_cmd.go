// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"math/rand"
	"sync/atomic"
)

type ServerCmd interface {
	Process(Server)
}

type SubscribeCmd struct {
	Subscription *Subscription
	Done         chan bool
}

func (cmd *SubscribeCmd) Process(s Server) {
	subscription := cmd.Subscription
	s.Subscriptions().Insert(subscription.Subject, subscription)
	cmd.Done <- true
}

type UnsubscribeCmd struct {
	Subscription *Subscription
	MaxResponses int
	Unsubscribed chan bool
}

func (cmd *UnsubscribeCmd) Process(s Server) {
	subscription := cmd.Subscription
	if cmd.MaxResponses > 0 {
		subscription.MaxResponses = cmd.MaxResponses
		if subscription.Responses < uint64(subscription.MaxResponses) {
			cmd.Unsubscribed <- false
			return
		}
	}

	s.Subscriptions().Delete(subscription.Subject, subscription)
	cmd.Unsubscribed <- true
}

type PublishCmd struct {
	Message *Message
}

func (cmd *PublishCmd) Process(s Server) {
	atomic.AddInt64(&s.Stats().msg_recv, 1)
	atomic.AddInt64(&s.Stats().bytes_recv, int64(len(cmd.Message.Content)))

	var queueGroups map[string][]*Subscription

	for _, match := range s.Subscriptions().Match(cmd.Message.Subject, WildcardMatcher) {
		subscription := match.(*Subscription)
		if subscription.Queue != nil {
			if queueGroups == nil {
				queueGroups = make(map[string][]*Subscription)
			}
			subscriptions := queueGroups[*subscription.Queue]
			if subscriptions == nil {
				subscriptions = make([]*Subscription, 0, 16)
			}
			queueGroups[*subscription.Queue] = append(subscriptions, subscription)
		} else {
			s.DeliverMessage(subscription, cmd.Message)
		}
	}

	if queueGroups != nil {
		for _, subscriptions := range queueGroups {
			index := rand.Int31n(int32(len(subscriptions)))
			s.DeliverMessage(subscriptions[index], cmd.Message)
		}
	}
}

type UnregisterConnCmd struct {
	Conn Conn
	Done chan bool
}

func (cmd *UnregisterConnCmd) Process(s Server) {
	for _, subscription := range cmd.Conn.Subscriptions() {
		s.Subscriptions().Delete(subscription.Subject, subscription)
	}
	cmd.Done <- true
}
