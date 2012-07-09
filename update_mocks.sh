#!/bin/sh

mockgen -package=mocks gonatsd/gonatsd Conn >gonatsd/mocks/mock_conn.go
mockgen -package=mocks gonatsd/gonatsd HeartbeatHelper >gonatsd/mocks/mock_heartbeat_helper.go
mockgen -package=mocks gonatsd/gonatsd Server >gonatsd/mocks/mock_server.go