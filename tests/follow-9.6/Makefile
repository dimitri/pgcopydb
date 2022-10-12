# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGVERSION ?= 9.6
COMPOSE_EXIT = --exit-code-from=test --abort-on-container-exit

test: down run down ;

up: down build
	PGVERSION=$(PGVERSION) docker-compose up $(COMPOSE_EXIT)

run: build
	PGVERSION=$(PGVERSION) docker-compose run test

down:
	docker-compose down

build:
	PGVERSION=$(PGVERSION) docker-compose build

.PHONY: run down build test
