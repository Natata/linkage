#!/bin/sh

protoc -I ./ ./job.proto --go_out=plugins=grpc:./
