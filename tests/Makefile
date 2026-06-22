# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGVERSION ?= 17
export PGVERSION

# Auto-detect docker or podman
DOCKER ?= $(shell command -v docker 2>/dev/null || command -v podman 2>/dev/null)
export DOCKER

BUILD_ARGS = --build-arg PGVERSION=$(PGVERSION)

all: pagila pagila-multi-steps blobs unit filtering filtering-standby extensions \
	 cdc-wal2json cdc-test-decoding cdc-endpos-between-transaction cdc-low-level \
	 follow-wal2json follow-standby follow-9.6 follow-data-only follow-target-reconnect \
	 endpos-in-multi-wal-txn exclude-extension \
	 blob-snapshot-release follow-defer-indexes fk-not-valid defer-validate-fks \
	 follow-defer-validate-fks follow-sequence-reset;

pagila: build
	$(MAKE) -C $@

pagila-multi-steps: build
	$(MAKE) -C $@

pagila-standby: build
	$(MAKE) -C $@

blobs: build
	$(MAKE) -C $@

unit: build
	$(MAKE) -C $@

filtering: build
	$(MAKE) -C $@

filtering-standby: build
	$(MAKE) -C $@

extensions: build
	$(MAKE) -C $@

cdc-wal2json: build
	$(MAKE) -C $@

cdc-test-decoding: build
	$(MAKE) -C $@

cdc-endpos-between-transaction: build
	$(MAKE) -C $@

cdc-low-level: build
	$(MAKE) -C $@

follow-wal2json: build
	$(MAKE) -C $@

follow-standby: build
	$(MAKE) -C $@

follow-9.6: build
	$(MAKE) -C $@

follow-data-only: build
	$(MAKE) -C $@

follow-target-reconnect: build
	$(MAKE) -C $@

endpos-in-multi-wal-txn: build
	$(MAKE) -C $@

exclude-extension: build
	$(MAKE) -C $@

blob-snapshot-release:
	$(MAKE) -C $@

follow-defer-indexes: build
	$(MAKE) -C $@

fk-not-valid: build
	$(MAKE) -C $@

defer-validate-fks: build
	$(MAKE) -C $@

follow-defer-validate-fks: build
	$(MAKE) -C $@

follow-sequence-reset: build
	$(MAKE) -C $@

timescaledb: build
	$(MAKE) -C $@

build:
	cd .. && $(DOCKER) build $(BUILD_ARGS) -t pgcopydb:pg$(PGVERSION) -f Dockerfile .
	$(DOCKER) build $(BUILD_ARGS) --build-arg PGCOPYDB_IMAGE=pgcopydb:pg$(PGVERSION) -t pagila -f Dockerfile.pagila .

.PHONY: all build
.PHONY: pagila pagila-multi-steps blobs unit filtering filtering-standby extensions
.PHONY: cdc-wal2json cdc-test-decoding cdc-low-level
.PHONY: follow-wal2json follow-standby follow-9.6 follow-target-reconnect
.PHONY: endpos-in-multi-wal-txn exclude-extension
.PHONY: blob-snapshot-release follow-defer-indexes fk-not-valid defer-validate-fks
.PHONY: follow-defer-validate-fks follow-sequence-reset
