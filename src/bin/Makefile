# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the PostgreSQL License.

all: pgcopydb ;

pgcopydb:
	$(MAKE) -C pgcopydb pgcopydb

clean:
	$(MAKE) -C pgcopydb clean

install: $(pgcopydb)
	$(MAKE) -C pgcopydb install

.PHONY: all pgcopydb install clean
