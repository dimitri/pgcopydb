# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

TOP := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

all: bin ;

bin:
	$(MAKE) -C src/bin/ all

clean:
	$(MAKE) -C src/bin/ clean

docs:
	$(MAKE) -C docs clean man html

test: build
	$(MAKE) -C tests all

tests: test ;

install: bin
	$(MAKE) -C src/bin/ install

indent:
	citus_indent

build:
	docker build -t pgcopydb .

deb:
	docker build -f Dockerfile.debian -t pgcopydb_debian .

debsh: deb
	docker run --rm -it pgcopydb_debian bash

.PHONY: all
.PHONY: bin clean install docs
.PHONY: test tests
.PHONY: deb
