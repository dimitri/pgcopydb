# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

COMPOSE_EXIT = --exit-code-from=test --abort-on-container-exit

test: down run down ;

up: down build
	docker compose up --remove-orphans $(COMPOSE_EXIT)

run: build
	# --use-aliases gives the one-off `run` container its "test" network alias
	# so the inject container can reach the follow coordinator at test:5442.
	docker compose run --remove-orphans --use-aliases test

down:
	docker compose down --volumes --remove-orphans

build:
	docker compose build --quiet

VPATH   = /var/run/pgcopydb
CNAME   = follow-pgoutput
VNAME   = follow-pgoutput
VOLUMES = -v $(VNAME):$(VPATH)
OPTS    = --env-file=../paths.env $(VOLUMES)
CLEANUP = make -f /var/lib/postgres/cleanup.mk

fix-volumes:
	docker run --rm $(OPTS) $(CNAME) $(CLEANUP)

attach:
	docker run --rm -it $(OPTS) $(CNAME) bash

.PHONY: run down build test fix-volumes attach
