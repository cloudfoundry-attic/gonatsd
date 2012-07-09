// Copyright (c) 2012 VMware, Inc.

package main

import (
	"flag"
	log "github.com/cihub/seelog"
	"gonatsd/gonatsd"
	"math/rand"
	"time"
)

var configFilename = flag.String("config", "", "path to gonatsd config file")

func main() {
	defer log.Flush()

	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	config := gonatsd.ParseConfig(*configFilename)
	server := gonatsd.NewServer(config)
	server.Start()
}
