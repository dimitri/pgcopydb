# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

all: pgcopydb ;

pgcopydb:
	$(MAKE) -C pgcopydb pgcopydb

clean:
	$(MAKE) -C pgcopydb clean

install: $(pgcopydb)
	$(MAKE) -C pgcopydb install

.PHONY: all pgcopydb install clean
