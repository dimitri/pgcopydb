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

tests/pagila: build
	$(MAKE) -C tests/pagila

tests/pagila-multi-steps: build
	$(MAKE) -C tests/pagila-multi-steps

tests/blobs: build
	$(MAKE) -C tests/blobs

install: bin
	$(MAKE) -C src/bin/ install

indent:
	citus_indent

build:
	docker build -t pgcopydb .

# debian packages built from the current sources
deb:
	docker build -f Dockerfile.debian -t pgcopydb_debian .

debsh: deb
	docker run --rm -it pgcopydb_debian bash

# debian packages built from latest tag, manually maintained in the Dockerfile
deb-qa:
	docker build -f Dockerfile.debian-qa -t pgcopydb_debian_qa .

debsh-qa: deb-qa
	docker run --rm -it pgcopydb_debian_qa bash

.PHONY: all
.PHONY: bin clean install docs
.PHONY: test tests tests/pagila tests/pagila-multi-steps tests/blobs
.PHONY: deb debsh deb-qa debsh-qa
