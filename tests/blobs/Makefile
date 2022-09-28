# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

test: down run down ;

run: build
	docker-compose run test

down:
	docker-compose down

build:
	docker-compose build --quiet

import:
	psql --single-transaction --no-psqlrc -f import.sql


.PHONY: run down build test import
