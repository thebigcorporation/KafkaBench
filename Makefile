# Copyright (C) 2020 Manetu Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ORG_NAME := manetu/examples
PROJECT_NAME := KafkaBench
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/ | grep -v _test.go)
OUTPUTDIR := target
BINARY := $(OUTPUTDIR)/$(PROJECT_NAME)

.PHONY: all build clean test lint goimports staticcheck

all: build

lint: ## Lint the files
	@golint -set_exit_status ./...

test: ## Run unittests
	@go test -cover

race: ## Run data race detector
	@go test -race -short .

staticcheck: ## Run data race detector
	@staticcheck -f stylish -unused.whole-program ./...

goimports: ## Run goimports
	$(eval goimportsdiffs = $(shell goimports -l .))
	@if [ -n "$(goimportsdiffs)" ]; then\
		echo "goimports shows diffs for these files:";\
		echo "$(goimportsdiffs)";\
		exit 1;\
	fi

%/$(PROJECT_NAME): $(GO_FILES) Makefile go.mod go.sum
	@go build -o $@ ./...

build: $(BINARY) ## Build the binary file

clean: ## Remove previous build
	@rm -rf $(OUTPUTDIR)

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
