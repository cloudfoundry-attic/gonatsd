// Copyright (c) 2012 VMware, Inc.

package main

import (
	"flag"
	log "github.com/cihub/seelog"
	"gonatsd/gonatsd"
	"math/rand"
	"os"
	"time"
)

var configFilename = flag.String("config", "", "path to gonatsd config file")

func main() {
	defer log.Flush()

	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	config, err := gonatsd.ParseConfig(*configFilename)
	if err != nil {
		log.Critical(err.Error())
		os.Exit(1)
	}

	server := gonatsd.NewServer(config)
	server.Start()
}
