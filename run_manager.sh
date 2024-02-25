#!/bin/bash

go run src/worker/worker.go src/worker/worker_part.go src/worker/manager_part.go --role=manager --configPath="src/worker/config_manager.json"