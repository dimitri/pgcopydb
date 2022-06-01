# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

all: pagila pagila-multi-steps blobs unit filtering cdc ;

pagila:
	$(MAKE) -C pagila

pagila-multi-steps:
	$(MAKE) -C pagila-multi-steps

blobs:
	$(MAKE) -C blobs

unit:
	$(MAKE) -C unit

filtering:
	$(MAKE) -C filtering

cdc:
	$(MAKE) -C cdc

.PHONY: all pagila pagila-multi-steps blobs unit filteering
