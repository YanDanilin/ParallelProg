#!/bin/bash

go run src/worker/worker.go src/worker/worker_part.go src/worker/manager_part.go --configPath="src/worker/config.json"