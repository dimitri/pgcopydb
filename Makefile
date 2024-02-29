# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

TOP := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
PGCOPYDB ?= $(TOP)src/bin/pgcopydb/pgcopydb

all: bin ;

GIT-VERSION-FILE:
	@$(SHELL_PATH) ./GIT-VERSION-GEN > /dev/null 2>&1

bin: GIT-VERSION-FILE
	$(MAKE) -C src/bin/ all

sqlite3:
	$(MAKE) -C src/bin/lib/sqlite $@

clean:
	rm -f GIT-VERSION-FILE
	$(MAKE) -C src/bin/ clean

maintainer-clean: clean
	rm -f version

docs:
	$(MAKE) -C docs clean man html

update-docs: bin
	bash ./docs/update-help-messages.sh

test: build
	$(MAKE) -C tests all

tests: test ;

tests/ci:
	sh ./ci/banned.h.sh

tests/*: build
	$(MAKE) -C tests $(notdir $@)

install: bin
	$(MAKE) -C src/bin/ install

indent:
	citus_indent

build: version
	docker build -t pgcopydb .

echo-version: GIT-VERSION-FILE
	@awk '{print $$3}' $<

version: GIT-VERSION-FILE
	@awk '{print $$3}' $< > $@
	@cat $@

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
.PHONY: bin clean install docs maintainer-clean update-docs
.PHONY: test tests tests/ci tests/*
.PHONY: deb debsh deb-qa debsh-qa
.PHONY: GIT-VERSION-FILE
