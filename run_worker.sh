#!/bin/bash

param=$1

if [ "$param" -ge "1" ]; then
config="src/worker/config$param.json"
echo "config file: $config"
go run src/worker/worker.go src/worker/worker_part.go src/worker/manager_part.go --configPath=$config
#"src/worker/config1.json"
else
echo "worker number must be greater than 0"
exit 1
fi