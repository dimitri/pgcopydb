# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

all: pagila pagila-multi-steps blobs unit filtering cdc ;

pagila: build
	$(MAKE) -C pagila

pagila-multi-steps: build
	$(MAKE) -C pagila-multi-steps

blobs: build
	$(MAKE) -C blobs

unit: build
	$(MAKE) -C unit

filtering: build
	$(MAKE) -C filtering

cdc: build
	$(MAKE) -C cdc

follow: build
	$(MAKE) -C cdc

extensions: build
	$(MAKE) -C extensions

build:
	docker build -t pagila -f Dockerfile.pagila .

.PHONY: all build
.PHONY: pagila pagila-multi-steps blobs unit filtering cdc follow extensions
