# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

test: down run down ;

run: build
	docker compose run test

down:
	docker compose down -v

build:
	docker compose build --quiet

.PHONY: down build test
