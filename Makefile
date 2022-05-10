# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

TOP := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

all: bin ;

GIT-VERSION-FILE:
	@$(SHELL_PATH) ./GIT-VERSION-GEN

-include GIT-VERSION-FILE

bin: GIT-VERSION-FILE
	$(MAKE) -C src/bin/ all

clean:
	rm -f GIT-VERSION-FILE
	$(MAKE) -C src/bin/ clean

docs:
	$(MAKE) -C docs clean man html

test: build
	$(MAKE) -C tests all

tests: test ;

tests/*: build
	$(MAKE) -C $@

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
.PHONY: test tests tests/*
.PHONY: deb debsh deb-qa debsh-qa
.PHONY: GIT-VERSION-FILE
