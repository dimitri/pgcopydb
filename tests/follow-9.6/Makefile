# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGVERSION ?= 9.6
export PGVERSION

COMPOSE_EXIT = --exit-code-from=test --abort-on-container-exit

test: down run down ;

up: down build
	docker compose up $(COMPOSE_EXIT)

run: build fix-volumes
	docker compose run test

down:
	docker compose down --volumes --remove-orphans

build:
	docker compose build

PATH    = /var/run/pgcopydb
CNAME   = follow-9.6
VNAME   = follow-9.6
VOLUMES = -v $(VNAME):$(PATH)
OPTS    = --env-file=../paths.env $(VOLUMES)
CLEANUP = make -f /var/lib/postgres/cleanup.mk

fix-volumes:
	docker run --rm -it $(OPTS) $(CNAME) $(CLEANUP)

attach:
	docker run --rm -it $(OPTS) $(CNAME) bash

.PHONY: run down build test fix-volumes
