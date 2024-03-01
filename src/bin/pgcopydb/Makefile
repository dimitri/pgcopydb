# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGCOPYDB = ./pgcopydb

SRC_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

-include $(SRC_DIR)../../../GIT-VERSION-FILE

DEPDIR = $(SRC_DIR)/.deps

SQLITE3_DIR = $(SRC_DIR)../lib/sqlite/
SQLITE3_LIB = $(SQLITE3_DIR)sqlite3.o

INCLUDES  = $(patsubst ${SRC_DIR}%.h,%.h,$(wildcard ${SRC_DIR}*.h))

SRC   = $(patsubst ${SRC_DIR}%.c,%.c,$(wildcard ${SRC_DIR}*.c))
OBJS  = $(patsubst %.c,%.o,$(SRC))
OBJS += lib-log.o lib-commandline.o lib-parson.o
OBJS += lib-snprintf.o lib-strerror.o lib-dumputils.o lib-string_utils.o
OBJS += lib-lookup3.o
OBJS += lib-sqlite3.o

PG_CONFIG ?= pg_config
BINDIR    ?= $(shell $(PG_CONFIG) --bindir)

PG_SNPRINTF     = $(wildcard ${SRC_DIR}../lib/pg/snprintf.*)
PG_DUMPUTILS    = $(wildcard ${SRC_DIR}../lib/pg/*utils.*)
LOG_SRC         = $(wildcard ${SRC_DIR}../lib/log/src/log.*)
COMMANDLINE_SRC = $(wildcard ${SRC_DIR}../lib/subcommands.c/commandline.*)
PARSON_SRC      = $(wildcard ${SRC_DIR}../lib/parson/parson.*)
JENKINS_SRC     = $(wildcard ${SRC_DIR}../lib/jenkins/lookup3.*)

COMMON_LIBS  = -I${SRC_DIR}../lib/pg
COMMON_LIBS += -I${SRC_DIR}../lib/log/src/
COMMON_LIBS += -I${SRC_DIR}../lib/subcommands.c/
COMMON_LIBS += -I${SRC_DIR}../lib/libs/
COMMON_LIBS += -I${SRC_DIR}../lib/parson/
COMMON_LIBS += -I${SRC_DIR}../lib/uthash/
COMMON_LIBS += -I${SRC_DIR}../lib/jenkins/
COMMON_LIBS += -I${SRC_DIR}../lib/sqlite/

CC = $(shell $(PG_CONFIG) --cc)

DEFAULT_CFLAGS = -std=c99 -D_GNU_SOURCE -g
DEFAULT_CFLAGS += -I $(shell $(PG_CONFIG) --includedir)
DEFAULT_CFLAGS += -I $(shell $(PG_CONFIG) --includedir-server)
DEFAULT_CFLAGS += -I $(shell $(PG_CONFIG) --pkgincludedir)/internal
DEFAULT_CFLAGS += $(shell $(PG_CONFIG) --cflags)
DEFAULT_CFLAGS += -Wformat
DEFAULT_CFLAGS += -Wall
DEFAULT_CFLAGS += -Werror=implicit-int
DEFAULT_CFLAGS += -Werror=implicit-function-declaration
DEFAULT_CFLAGS += -Werror=return-type
DEFAULT_CFLAGS += -Wno-declaration-after-statement

ifdef DEBUG
# Use optimization option that provides good debugging experience
# https://gcc.gnu.org/onlinedocs/gcc/Optimize-Options.html.
# It overrides '-O2' flag of pg_config.
DEFAULT_CFLAGS += -Og
endif

# Needed for FreeBSD
DEFAULT_CFLAGS += -D_WANT_SEMUN

# Needed for OSX
DEFAULT_CFLAGS += -Wno-missing-braces
DEFAULT_CFLAGS += $(COMMON_LIBS)

# Needed for ARM64 based OSX
ifeq ($(shell uname -s),Darwin)
	ifeq ($(shell uname -m),arm64)
		DEFAULT_CFLAGS += -I/opt/homebrew/include
	endif
endif


ifdef USE_SECURITY_FLAGS
# Flags taken from: https://liquid.microsoft.com/Web/Object/Read/ms.security/Requirements/Microsoft.Security.SystemsADM.10203#guide
SECURITY_CFLAGS=-fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2 -z noexecstack -fpie -Wl,-pie -Wl,-z,relro -Wl,-z,now -Wformat -Wformat-security -Werror=format-security
DEFAULT_CFLAGS += $(SECURITY_CFLAGS)
endif

override CFLAGS := $(DEFAULT_CFLAGS) $(CFLAGS)

JENKINS_CFLAGS = $(CFAGS) -Wno-implicit-fallthrough -Wno-format

LIBS  = -L $(shell $(PG_CONFIG) --pkglibdir)
LIBS += -L $(shell $(PG_CONFIG) --libdir)
LIBS += $(shell $(PG_CONFIG) --ldflags)
LIBS += $(shell $(PG_CONFIG) --libs)
LIBS += -lpq
LIBS += -lncurses
LIBS += -lgc

# Needed for ARM64 based OSX
ifeq ($(shell uname -s),Darwin)
	ifeq ($(shell uname -m),arm64)
		LIBS += -L/opt/homebrew/lib
	endif
endif

all: $(PGCOPYDB) $(SQLITE3) ;

# Based on Postgres Makefile for automatic dependency generation
# https://github.com/postgres/postgres/blob/1933ae629e7b706c6c23673a381e778819db307d/src/Makefile.global.in#L890-L924
%.o : %.c
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -o $@ $<

Po_files := $(wildcard $(DEPDIR)/*.Po)
ifneq (,$(Po_files))
include $(Po_files)
endif


$(PGCOPYDB): VERSION-FILE $(OBJS) $(INCLUDES)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LIBS) -o $@

$(SQLITE3_LIB):
	$(MAKE) -C $(SQLITE3_DIR) sqlite3.o

lib-sqlite3.o: $(SQLITE3_LIB)
	ln -sf $(SQLITE3_LIB) $@

lib-snprintf.o: $(PG_SNPRINTF)
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/pg/snprintf.c

lib-strerror.o: $(PG_SNPRINTF)
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/pg/strerror.c

lib-dumputils.o: $(PG_DUMPUTILS)
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/pg/dumputils.c

lib-string_utils.o: $(PG_DUMPUTILS)
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/pg/string_utils.c

lib-log.o: $(LOG_SRC)
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/log/src/log.c

lib-commandline.o: $(COMMANDLINE_SRC)
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/subcommands.c/commandline.c

lib-parson.o: $(PARSON_SRC)
	$(CC) $(CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/parson/parson.c

lib-lookup3.o: $(JENKINS_SRC)
	$(CC) $(JENKINS_CFLAGS) -c -MMD -MP -MF$(DEPDIR)/$(*F).Po -MT$@ -o $@ ${SRC_DIR}../lib/jenkins/lookup3.c

VERSION-FILE: git-version.h ;

git-version.h:
	echo "#define GIT_VERSION \""$(GIT_VERSION)"\"" > $@

clean:
	rm -f git-version.h
	rm -f $(OBJS) $(PGCOPYDB)
	$(MAKE) -C $(SQLITE3_DIR) clean
	rm -rf $(DEPDIR)

install: $(PGCOPYDB)
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 $(PGCOPYDB) $(DESTDIR)$(BINDIR)

.PHONY: all monitor clean
.PHONY: VERSION-FILE
