#!/bin/bash

protoc --proto_path=. --go_out=out --go_opt=paths=source_relative communication.proto