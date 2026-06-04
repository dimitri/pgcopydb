# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

test: down run down ;

run: build
	docker compose run test

down:
	docker compose down

build:
	docker compose build --quiet

.PHONY: run down build test
