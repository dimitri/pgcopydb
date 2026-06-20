# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGVERSION ?= 16
export PGVERSION
BUILD_ARGS = --build-arg PGVERSION=$(PGVERSION)

all: pagila pagila-multi-steps blobs unit filtering extensions \
	 partitioned-target all-databases \
	 cdc-wal2json cdc-test-decoding cdc-endpos-mid-txn cdc-low-level \
	 cdc-replica-identity-index cdc-partitioned-target cdc-matview \
	 cdc-endpos-in-multi-wal-txn cdc-file-rotation \
	 cdc-pgoutput cdc-filtering-pgoutput \
	 follow-wal2json follow-9.6 follow-data-only \
	 follow-pgoutput follow-filtering;

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

cdc-matview: build
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

follow-filtering: build
	$(MAKE) -C $@

timescaledb: build
	$(MAKE) -C $@

build:
	@docker image inspect pagila > /dev/null 2>&1 \
	  && echo "pagila image already present — skipping build (run 'make force-build' to rebuild)" \
	  || docker build $(BUILD_ARGS) -t pagila -f Dockerfile.pagila .

build-multi:
	@docker image inspect multi > /dev/null 2>&1 \
	  && echo "multi image already present — skipping build (run 'make force-build' to rebuild)" \
	  || docker build $(BUILD_ARGS) -t multi -f Dockerfile.multi .

# Force a full rebuild even when the images already exist locally
# (use after Dockerfile changes or when switching PGVERSION)
force-build:
	docker build $(BUILD_ARGS) -t pagila -f Dockerfile.pagila .
	docker build $(BUILD_ARGS) -t multi -f Dockerfile.multi .

all-databases: build-multi
	$(MAKE) -C $@

three-databases: build-multi
	$(MAKE) -C $@

# Remove the cached images so the next test starts from scratch
clean-images:
	-docker rmi pagila multi 2>/dev/null; true

.PHONY: all build build-pagila build-multi force-build clean-images
.PHONY: pagila pagila-multi-steps blobs unit filtering partitioned-target extensions
.PHONY: all-databases three-databases
.PHONY: cdc-wal2json cdc-test-decoding cdc-endpos-mid-txn cdc-low-level cdc-replica-identity-index cdc-partitioned-target
.PHONY: cdc-endpos-in-multi-wal-txn cdc-file-rotation
.PHONY: cdc-pgoutput cdc-filtering-pgoutput follow-pgoutput follow-filtering
.PHONY: follow-wal2json follow-9.6
