# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

COMPOSE_EXIT = --exit-code-from=test --abort-on-container-exit

test: down run down ;

up: down build
	$(DOCKER) compose up $(COMPOSE_EXIT)

run: build fix-volumes
	$(DOCKER) compose run test

down:
	$(DOCKER) compose down

build:
	$(DOCKER) compose build

VPATH   = /var/run/pgcopydb
CNAME   = follow-target-reconnect
VNAME   = follow-target-reconnect
VOLUMES = -v $(VNAME):$(VPATH)
OPTS    = --env-file=../paths.env $(VOLUMES)
CLEANUP = make -f /var/lib/postgres/cleanup.mk

fix-volumes:
	$(DOCKER) run --rm $(OPTS) $(CNAME) $(CLEANUP)

attach:
	$(DOCKER) run --rm -it $(OPTS) $(CNAME) bash

.PHONY: run down build test fix-volumes
