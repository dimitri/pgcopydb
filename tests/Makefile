# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGVERSION ?= 16
export PGVERSION
BUILD_ARGS = --build-arg PGVERSION=$(PGVERSION)

all: pagila pagila-multi-steps blobs unit filtering extensions \
	 partitioned-target \
	 cdc-wal2json cdc-test-decoding cdc-endpos-mid-txn cdc-low-level \
	 cdc-replica-identity-index cdc-partitioned-target \
	 cdc-endpos-in-multi-wal-txn cdc-file-rotation \
	 cdc-pgoutput cdc-filtering-pgoutput \
	 follow-wal2json follow-9.6 follow-data-only \
	 follow-pgoutput;

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

partitioned-target: build
	$(MAKE) -C $@

extensions: build
	$(MAKE) -C $@

cdc-wal2json: build
	$(MAKE) -C $@

cdc-test-decoding: build
	$(MAKE) -C $@

cdc-endpos-mid-txn: build
	$(MAKE) -C $@

cdc-low-level: build
	$(MAKE) -C $@

cdc-replica-identity-index: build
	$(MAKE) -C $@

cdc-partitioned-target: build
	$(MAKE) -C $@

follow-wal2json: build
	$(MAKE) -C $@

follow-9.6: build
	$(MAKE) -C $@

follow-data-only: build
	$(MAKE) -C $@

cdc-endpos-in-multi-wal-txn: build
	$(MAKE) -C $@

cdc-file-rotation: build
	$(MAKE) -C $@

cdc-pgoutput: build
	$(MAKE) -C $@

cdc-filtering-pgoutput: build
	$(MAKE) -C $@

follow-pgoutput: build
	$(MAKE) -C $@

timescaledb: build
	$(MAKE) -C $@

build:
	@docker image inspect pagila > /dev/null 2>&1 \
	  && echo "pagila image already present — skipping build (run 'make force-build' to rebuild)" \
	  || docker build $(BUILD_ARGS) -t pagila -f Dockerfile.pagila .

# Force a full rebuild even when the pagila image already exists locally
# (use after Dockerfile.pagila changes or when switching PGVERSION)
force-build:
	docker build $(BUILD_ARGS) -t pagila -f Dockerfile.pagila .

# Remove the cached pagila image so the next test starts from scratch
clean-images:
	-docker rmi pagila 2>/dev/null; true

.PHONY: all build force-build clean-images
.PHONY: pagila pagila-multi-steps blobs unit filtering partitioned-target extensions
.PHONY: cdc-wal2json cdc-test-decoding cdc-endpos-mid-txn cdc-low-level cdc-replica-identity-index cdc-partitioned-target
.PHONY: cdc-endpos-in-multi-wal-txn cdc-file-rotation
.PHONY: cdc-pgoutput cdc-filtering-pgoutput follow-pgoutput
.PHONY: follow-wal2json follow-9.6
