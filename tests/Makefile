# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

all: pagila pagila-multi-steps blobs unit filtering cdc follow extensions ;

pagila: build
	$(MAKE) -C $@

pagila-multi-steps: build
	$(MAKE) -C $@

blobs: build
	$(MAKE) -C $@

unit: build
	$(MAKE) -C $@

filtering: build
	$(MAKE) -C $@

cdc: build
	$(MAKE) -C $@

follow: build
	$(MAKE) -C $@

follow-9.6: build
	$(MAKE) -C $@

extensions: build
	$(MAKE) -C $@

build:
	docker build -t pagila -f Dockerfile.pagila .

.PHONY: all build
.PHONY: pagila pagila-multi-steps blobs unit filtering cdc follow extensions
