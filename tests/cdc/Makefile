# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

INTERACTIVE_OPTS = --abort-on-container-exit --exit-code-from pgcopydb

test: build down up down ;

up:
	docker-compose up $(INTERACTIVE_OPTS)

down:
	docker-compose down

build:
	$(MAKE) -C .. build
	docker-compose build

.PHONY: up down build test
