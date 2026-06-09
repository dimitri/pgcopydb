# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

TOP := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
PGCOPYDB ?= $(TOP)src/bin/pgcopydb/pgcopydb
PGVERSION ?= 16

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

maintainer-clean:
	rm -f GIT-VERSION-FILE
	$(MAKE) -C src/bin/ maintainer-clean
	rm -f version

docs:
	$(MAKE) -C docs clean man html

update-docs: bin
	bash ./docs/update-help-messages.sh

check-docs:
	cat Dockerfile ci/Dockerfile.docs.template > ci/Dockerfile.docs
	docker build --file=ci/Dockerfile.docs --tag test-docs .
	docker run test-docs

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
	@docker image inspect pgcopydb > /dev/null 2>&1 \
	  && echo "pgcopydb image already present — skipping build (run 'make force-build' to rebuild)" \
	  || docker build --build-arg PGVERSION=$(PGVERSION) -t pgcopydb .

# Force a full rebuild even when the pgcopydb image already exists locally
# (use after source-code changes or when switching PGVERSION)
force-build: version
	docker build --build-arg PGVERSION=$(PGVERSION) -t pgcopydb .

# Remove cached Docker images so the next build/test starts from scratch
clean-images:
	-docker rmi pgcopydb pagila 2>/dev/null; true

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
.PHONY: build force-build clean-images
.PHONY: deb debsh deb-qa debsh-qa
.PHONY: GIT-VERSION-FILE
