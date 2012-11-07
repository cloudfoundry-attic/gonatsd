// Copyright (c) 2012 VMware, Inc.

package gonatsd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"launchpad.net/goyaml"
	"time"
)

const (
	DEFAULT_MAX_CONTROL = 1024
	DEFAULT_MAX_PAYLOAD = 1024 * 1024
	DEFAULT_MAX_PENDING = 10 * 1024 * 1024
)

type PingConfig struct {
	Interval         string `yaml:"interval"`
	IntervalDuration time.Duration
	MaxOutstanding   int `yaml:"max_outstanding"`
}

type ProfileConfig struct {
	BindAddress string `yaml:"bind_address"`
}

type VarzConfig struct {
	BindAddress string            `yaml:"bind_address"`
	Users       map[string]string `yaml:"users"`
}

type AuthConfig struct {
	Users           map[string]string `yaml:"users"`
	Timeout         string            `yaml:"timeout"`
	TimeoutDuration time.Duration
}

type LogConfig struct {
	MinLevel string `yaml:"level"`
	Out      string `yaml:"file"`
}

type LimitsConfig struct {
	Payload     int `yaml:"payload"`
	Pending     int `yaml:"pending"`
	ControlLine int `yaml:"control"`
	Connections int `yaml:"connections"`
}

type Config struct {
	BindAddress string        `yaml:"bind_address"`
	Ping        PingConfig    `yaml:"ping"`
	Profile     ProfileConfig `yaml:"pprof"`
	Varz        VarzConfig    `yaml:"varz"`
	Auth        AuthConfig    `yaml:"auth"`
	Log         LogConfig     `yaml:"logging"`
	Limits      LimitsConfig  `yaml:"limits"`
}

// Parse the server configuration. Will exit if there was an error.
func ParseConfig(filename string) (*Config, error) {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("Can't read configuration file '%s': %s", filename, err.Error())
	}

	config := &Config{}
	err = goyaml.Unmarshal(contents, &config)
	if err != nil {
		return nil, fmt.Errorf("Can't parse configuration file '%s': %s", filename, err.Error())
	}

	if len(config.BindAddress) == 0 {
		return nil, errors.New("bind_address is required")
	}

	if len(config.Auth.Timeout) > 0 {
		config.Auth.TimeoutDuration, err = time.ParseDuration(config.Auth.Timeout)
		if err != nil {
			return nil, fmt.Errorf("Invalid auth timeout '%s': %s", config.Auth.Timeout, err.Error())
		}
	}

	if len(config.Ping.Interval) > 0 {
		config.Ping.IntervalDuration, err = time.ParseDuration(config.Ping.Interval)
		if err != nil {
			return nil, fmt.Errorf("Invalid ping interval '%s': %s", config.Ping.Interval, err.Error())
		}
	}

	if config.Limits.ControlLine == 0 {
		config.Limits.ControlLine = DEFAULT_MAX_CONTROL
	}

	if config.Limits.Payload == 0 {
		config.Limits.Payload = DEFAULT_MAX_PAYLOAD
	}

	if config.Limits.Pending == 0 {
		config.Limits.Pending = DEFAULT_MAX_PENDING
	}

	return config, nil
}
