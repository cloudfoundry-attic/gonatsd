// Copyright (c) 2012 VMware, Inc.

package main

import (
	"flag"
	"fmt"
	"gonatsd/gonatsd"
	"math/rand"
	"os"
	"time"
)

var configFilename = flag.String("config", "", "path to gonatsd config file")

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	config, err := gonatsd.ParseConfig(*configFilename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %s\n", err.Error())
		os.Exit(1)
	}

	server, err := gonatsd.NewServer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %s\n", err.Error())
		os.Exit(1)
	}

	server.Start()
}
