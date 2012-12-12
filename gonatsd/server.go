// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	VERSION                = "0.0.1"
	DEFAULT_SERVER_BACKLOG = 1024
)

type Info struct {
	ServerId     string `json:"server_id"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Version      string `json:"version"`
	AuthRequired bool   `json:"auth_required"`
	SslRequired  bool   `json:"ssl_required"`
	MaxPayload   int    `json:"max_payload"`
}

type Stats struct {
	ops             map[string]*int64
	bad_ops         map[string]*int64
	msg_sent        int64
	msg_recv        int64
	bytes_sent      int64
	bytes_recv      int64
	unkown_ops      int64
	slow_consumer   int64
	payload_too_big int64
	unresponsive    int64
	bad_auth        int64
	errors          int64
}

func NewStats() *Stats {
	stats := &Stats{}
	stats.ops = make(map[string]*int64)
	stats.bad_ops = make(map[string]*int64)
	for _, request := range REQUESTS {
		var ops, bad_ops int64
		stats.ops[request] = &ops
		stats.bad_ops[request] = &bad_ops
	}
	return stats
}

type Server interface {
	Start()
	DeliverMessage(subscription *Subscription, message *Message)
	Commands() chan<- ServerCmd
	Subscriptions() *Trie
	Info() *[]byte
	Stats() *Stats
	Config() *Config
}

type server struct {
	commands      chan ServerCmd
	config        *Config
	subscriptions *Trie
	connections   int64
	info          []byte
	stats         *Stats
}

func NewServer(config *Config) (Server, error) {
	s := new(server)
	s.commands = make(chan ServerCmd, DEFAULT_SERVER_BACKLOG)
	s.config = config
	s.stats = NewStats()
	s.subscriptions = NewTrie(".")

	err := s.initLogger()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Config() *Config {
	return s.config
}

func (s *server) Info() *[]byte {
	return &s.info
}

func (s *server) Commands() chan<- ServerCmd {
	return s.commands
}

func (s *server) Subscriptions() *Trie {
	return s.subscriptions
}

func (s *server) Stats() *Stats {
	return s.stats
}

func (s *server) Start() {
	s.exportPprof()
	s.exportVarz()

	authRequired := len(s.config.Auth.Users) > 0
	Log.Infof("Starting server on: %s [auth: %v] [users: %d]", s.config.BindAddress, authRequired,
		len(s.config.Auth.Users))
	ln, err := net.Listen("tcp", s.config.BindAddress)
	if err != nil {
		Log.Fatalf("Could not listen: %s", err)
		os.Exit(1)
	}

	addr := ln.Addr().(*net.TCPAddr)
	info := &Info{"my_server_id", addr.IP.String(), addr.Port, VERSION, authRequired, false,
		s.config.Limits.Payload}
	s.info, _ = json.Marshal(info)

	s.bindMetrics()

	go s.loop()

	for {
		nc, err := ln.Accept()
		if err != nil {
			Log.Fatalf("Could not accept: %s", err)
			os.Exit(1)
		}
		go s.processConn(nc)
	}
}

func (s *server) DeliverMessage(subscription *Subscription, message *Message) {
	subscribedMessage := &SubscribedMessage{Subscription: subscription, Message: message}
	subscription.Responses++
	if subscription.MaxResponses > 0 && subscription.Responses >= uint64(subscription.MaxResponses) {
		s.Subscriptions().Delete(subscription.Subject, subscription)
		subscribedMessage.Last = true
	}
	subscription.Conn.ServeMessage(subscribedMessage)
	atomic.AddInt64(&s.stats.msg_sent, 1)
	atomic.AddInt64(&s.stats.bytes_sent, int64(len(message.Content)))
}

func (s *server) exportPprof() {
	if len(s.config.Profile.BindAddress) > 0 {
		mux := http.NewServeMux()
		mux.Handle("/debug/pprof/", http.HandlerFunc(httppprof.Index))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(httppprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(httppprof.Profile))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(httppprof.Symbol))
		Log.Infof("Starting pprof server on: %s", s.config.Profile.BindAddress)
		go func() {
			err := http.ListenAndServe(s.config.Profile.BindAddress, mux)
			if err != nil {
				Log.Fatalf("Could not listen: %s", err)
				os.Exit(1)
			}
		}()
	}
}

func (s *server) exportVarz() {
	if len(s.config.Varz.BindAddress) > 0 {
		mux := http.NewServeMux()
		varzHandler := func(w http.ResponseWriter, r *http.Request) {
			s.varzHandler(w, r)
		}
		mux.Handle("/varz", NewBasicAuthHandler(s.config.Varz.Users, varzHandler))
		Log.Infof("Starting /varz endpoint on: %s", s.config.Varz.BindAddress)
		go func() {
			err := http.ListenAndServe(s.config.Varz.BindAddress, mux)
			if err != nil {
				Log.Fatalf("Could not listen: %s", err)
				os.Exit(1)
			}
		}()
	}
}

func (s *server) loop() {
	for r := range s.commands {
		r.Process(s)
	}
}

func (s *server) processConn(nc net.Conn) {
	connections := atomic.AddInt64(&s.connections, 1)
	conn := NewConn(s, nc.(*net.TCPConn))
	if s.config.Limits.Connections > 0 && connections > int64(s.config.Limits.Connections) {
		conn.CloseWithError(ErrMaxConnsExceeded)
	}
	conn.Start()
	atomic.AddInt64(&s.connections, -1)
}

func (s *server) initLogger() (err error) {
	logOut := os.Stdout

	if len(s.config.Log.Out) > 0 {
		logOut, err = os.OpenFile(s.config.Log.Out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
	}

	logger, err := NewLogger(logOut, s.config.Log.MinLevel)
	if err != nil {
		return err
	}

	ReplaceLogger(logger)
	return nil
}

func (s *server) bindMetrics() {
	started := time.Now().UTC()
	started_ts := started.Unix()

	DefaultRegistry.NewStringVal("info.num_cpu", fmt.Sprint(runtime.NumCPU()))
	DefaultRegistry.NewStringVal("info.arch", runtime.GOARCH)
	DefaultRegistry.NewStringVal("info.os", runtime.GOOS)
	DefaultRegistry.NewStringVal("info.cmdline", strings.Join(os.Args, " "))
	DefaultRegistry.NewStringVal("info.started", started.Format(time.RFC1123Z))
	DefaultRegistry.NewGauge("info.uptime", func() string {
		return fmt.Sprint(time.Now().Unix() - started_ts)
	})

	DefaultRegistry.NewGauge("subscriptions", func() string {
		return fmt.Sprint(s.Subscriptions().Values())
	})
	DefaultRegistry.NewGauge("subscriptions.nodes", func() string {
		return fmt.Sprint(s.Subscriptions().Nodes())
	})

	for _, request := range REQUESTS {
		name := fmt.Sprintf("ops.%s", strings.ToLower(request))
		DefaultRegistry.NewCounter(name, s.Stats().ops[request])
		name = fmt.Sprintf("bad_ops.%s", strings.ToLower(request))
		DefaultRegistry.NewCounter(name, s.Stats().bad_ops[request])
	}

	DefaultRegistry.NewCounter("errors.bad_auth", &s.Stats().bad_auth)
	DefaultRegistry.NewCounter("errors.slow", &s.Stats().slow_consumer)
	DefaultRegistry.NewCounter("errors.payload_too_big", &s.Stats().payload_too_big)
	DefaultRegistry.NewCounter("errors.unknown", &s.Stats().unkown_ops)
	DefaultRegistry.NewCounter("errors.unresponsive", &s.Stats().unresponsive)

	DefaultRegistry.NewCounter("conns", &s.connections)
	DefaultRegistry.NewCounter("msg_recv", &s.stats.msg_recv)
	DefaultRegistry.NewCounter("msg_sent", &s.stats.msg_sent)
	DefaultRegistry.NewCounter("bytes_recv", &s.stats.bytes_recv)
	DefaultRegistry.NewCounter("bytes_sent", &s.stats.bytes_sent)

	DefaultRegistry.NewRates("msg_recv.rate", &s.stats.msg_recv, "10s", "1m", "5m")
	DefaultRegistry.NewRates("msg_sent.rate", &s.stats.msg_sent, "10s", "1m", "5m")
	DefaultRegistry.NewRates("bytes_recv.rate", &s.stats.bytes_recv, "10s", "1m", "5m")
	DefaultRegistry.NewRates("bytes_sent.rate", &s.stats.bytes_sent, "10s", "1m", "5m")

	DefaultRegistry.NewGauge("runtime.goroutines", func() string {
		return fmt.Sprint(runtime.NumGoroutine())
	})

	var memstats runtime.MemStats
	DefaultRegistry.NewGauge("runtime.alloc", func() string {
		return fmt.Sprint(memstats.Alloc)
	})
	DefaultRegistry.NewGauge("runtime.total_alloc", func() string {
		return fmt.Sprint(memstats.TotalAlloc)
	})
	DefaultRegistry.NewGauge("runtime.sys", func() string {
		return fmt.Sprint(memstats.Sys)
	})
	DefaultRegistry.NewGauge("runtime.heap_alloc", func() string {
		return fmt.Sprint(memstats.HeapAlloc)
	})
	DefaultRegistry.NewGauge("runtime.heap_idle", func() string {
		return fmt.Sprint(memstats.HeapIdle)
	})
	DefaultRegistry.NewGauge("runtime.heap_inuse", func() string {
		return fmt.Sprint(memstats.HeapInuse)
	})
	DefaultRegistry.NewGauge("runtime.heap_released", func() string {
		return fmt.Sprint(memstats.HeapReleased)
	})
	DefaultRegistry.NewGauge("runtime.total_pause", func() string {
		return fmt.Sprint(memstats.PauseTotalNs)
	})
	DefaultRegistry.NewGauge("runtime.num_gc", func() string {
		return fmt.Sprint(memstats.NumGC)
	})

	go func() {
		tick := time.Tick(10 * time.Second)
		for {
			select {
			case <-tick:
				runtime.ReadMemStats(&memstats)
			}
		}
	}()
}

func (s *server) varzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	DefaultRegistry.Metrics(func(metrics map[string]fmt.Stringer) {
		fmt.Fprintf(w, "{\n")
		keys := make([]string, 0, len(metrics))
		for key, _ := range metrics {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for index, key := range keys {
			if index > 0 {
				fmt.Fprintf(w, ",\n")
			}
			json_key, _ := json.Marshal(key)
			json_value, _ := json.Marshal(metrics[key].String())
			fmt.Fprintf(w, "  %s: %s", json_key, json_value)
		}
		fmt.Fprintf(w, "\n}\n")
	})
}

type BasicAuthHandler struct {
	users       map[string]bool
	handlerFunc http.HandlerFunc
}

func NewBasicAuthHandler(users map[string]string, handlerFunc http.HandlerFunc) *BasicAuthHandler {
	handler := &BasicAuthHandler{handlerFunc: handlerFunc}
	handler.users = make(map[string]bool)
	for user, pass := range users {
		plain := fmt.Sprintf("%s:%s", user, pass)
		encoded := base64.StdEncoding.EncodeToString([]byte(plain))
		padded := fmt.Sprintf("Basic %s", encoded)
		handler.users[padded] = true
	}
	return handler
}

func (h *BasicAuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if len(h.users) > 0 {
		auth := r.Header.Get("Authorization")
		if len(auth) > 0 {
			if h.users[auth] != true {
				w.Header().Add("WWW-Authenticate", "Basic")
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		} else {
			w.Header().Add("WWW-Authenticate", "Basic")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	h.handlerFunc(w, r)
}
