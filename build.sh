#!/bin/bash
set -e
export VER=`git describe --dirty`
export CGO_ENABLED=0
go build -a -ldflags "-s -w -X main.version=${VER:-<notset>} --extldflags '-static'"
