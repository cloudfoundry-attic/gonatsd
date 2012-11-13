// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	DEFAULT_RATE_UPDATE_INTERVAL = 5 * time.Second
)

type StringVal struct {
	val string
}

func (v *StringVal) String() string {
	return v.val
}

type Counter struct {
	addr *int64
}

func (c *Counter) String() string {
	return strconv.FormatInt(*c.addr, 10)
}

type RateSample struct {
	ts    int64
	value int64
}

type Rate struct {
	addr     *int64
	buckets  []RateSample
	index    int
	interval time.Duration
	lock     *sync.RWMutex
}

func (r *Rate) String() string {
	r.lock.RLock()
	defer r.lock.RUnlock()

	lastSampleIndex := (r.index + len(r.buckets) - 1) % len(r.buckets)
	lastSample := r.buckets[lastSampleIndex]

	firstSample := r.buckets[r.index]
	if firstSample.ts == 0 {
		firstSample = r.buckets[0]
	}

	dx := lastSample.value - firstSample.value
	dt := lastSample.ts - firstSample.ts

	if dt == 0 {
		return "N/A"
	}

	return fmt.Sprintf("%f", float64(dx)/float64(dt))
}

func (r *Rate) Snapshot(ts int64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.buckets[r.index].ts = ts
	r.buckets[r.index].value = *r.addr
	r.index = (r.index + 1) % len(r.buckets)
}

type Registry struct {
	lock     *sync.RWMutex
	metrics  map[string]fmt.Stringer
	ticker   *time.Ticker
	interval time.Duration
}

func NewRegistry(rateUpdateInterval time.Duration) *Registry {
	registry := &Registry{}
	registry.lock = &sync.RWMutex{}
	registry.metrics = make(map[string]fmt.Stringer)
	registry.ticker = time.NewTicker(DEFAULT_RATE_UPDATE_INTERVAL)
	registry.interval = DEFAULT_RATE_UPDATE_INTERVAL
	go registry.Loop()
	return registry
}

func (r *Registry) Loop() {
	for {
		select {
		case t := <-r.ticker.C:
			ts := t.Unix()
			r.lock.RLock()
			for _, metric := range r.metrics {
				switch metric.(type) {
				case *Rate:
					rate := metric.(*Rate)
					rate.Snapshot(ts)
				}
			}
			r.lock.RUnlock()
		}
	}
}

func (r *Registry) NewStringVal(name string, val string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.metrics[name] = &StringVal{val}
}

func (r *Registry) NewCounter(name string, addr *int64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.metrics[name] = &Counter{addr}
}

func (r *Registry) NewRate(name string, addr *int64, interval time.Duration) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if interval < r.interval {
		interval = r.interval
	}

	numBuckets := interval / r.interval
	if numBuckets < 2 {
		numBuckets = 2
	}

	buckets := make([]RateSample, numBuckets)
	r.metrics[name] = &Rate{addr, buckets, 0, interval, &sync.RWMutex{}}
}

func (r *Registry) NewRates(name string, addr *int64, durations ...string) {
	for _, duration := range durations {
		d, err := time.ParseDuration(duration)
		if err == nil {
			r.NewRate(fmt.Sprintf("%s.%s", name, duration), addr, d)
		} else {
			Log.Warnf("Invalid rate interval, ignoring: %s %s", name, duration)
		}
	}
}

type Gauge struct {
	fn func() string
}

func (g *Gauge) String() string {
	return g.fn()
}

func (r *Registry) NewGauge(name string, fn func() string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.metrics[name] = &Gauge{fn}
}

func (r *Registry) NewStringerGauge(name string, stringer fmt.Stringer) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.metrics[name] = stringer
}

func (r *Registry) Metrics(fn func(map[string]fmt.Stringer)) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	fn(r.metrics)
}

var DefaultRegistry *Registry = NewRegistry(DEFAULT_RATE_UPDATE_INTERVAL)
