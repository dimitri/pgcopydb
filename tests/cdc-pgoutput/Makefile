# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

COMPOSE_EXIT = --exit-code-from=test --abort-on-container-exit

test: down run down ;

up: down build
	docker compose up $(COMPOSE_EXIT)

run: build
	docker compose run --rm test

down:
	docker compose down -v

build:
	docker compose build --quiet

attach:
	docker run --rm -it cdc-pgoutput bash

.PHONY: run down build test attach
