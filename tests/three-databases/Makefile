# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

test: down run down ;

run: build
	docker compose run --remove-orphans test

down:
	docker compose down -v --remove-orphans

build:
	docker compose build --quiet

.PHONY: down build test
