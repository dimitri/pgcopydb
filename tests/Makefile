# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

all: pagila pagila-multi-steps blobs unit filtering cdc ;

pagila: build
	$(MAKE) -C pagila

pagila-multi-steps: build
	$(MAKE) -C pagila-multi-steps

blobs:
	$(MAKE) -C blobs

unit:
	$(MAKE) -C unit

filtering:
	$(MAKE) -C filtering

cdc: build
	$(MAKE) -C cdc

follow: build
	$(MAKE) -C cdc

build:
	docker build -t pagila -f Dockerfile.pagila .

.PHONY: all build
.PHONY: pagila pagila-multi-steps blobs unit filtering cdc follow
