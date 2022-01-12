# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

TOP := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

all: bin ;

bin:
	$(MAKE) -C src/bin/ all

clean:
	$(MAKE) -C src/bin/ clean

test: build
	$(MAKE) -C tests all

tests: test ;

install: bin
	$(MAKE) -C src/bin/ install

indent:
	citus_indent

build:
	docker build -t pgcopydb .

.PHONY: all
.PHONY: bin clean install
.PHONY: test tests
