# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGCOPYDB = ./pgcopydb

SRC_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

-include $(SRC_DIR)../../../GIT-VERSION-FILE

DEPDIR = $(SRC_DIR)/.deps

SQLITE3_DIR = $(SRC_DIR)../lib/sqlite/
SQLITE3_LIB = $(SQLITE3_DIR)sqlite3.o

INCLUDES  = $(patsubst ${SRC_DIR}%.h,%.h,$(wildcard ${SRC_DIR}*.h))

# archive_toc_lexer.c/archive_toc_grammar.c are generated (see the flex/
# bison rules below) rather than hand-written, so they may not exist yet
# when this wildcard is evaluated on a fresh checkout; exclude them here
# and add archive_toc_lexer.o/archive_toc_grammar.o below unconditionally,
# instead of depending on the wildcard to find them.
SRC   = $(patsubst ${SRC_DIR}%.c,%.c,$(filter-out ${SRC_DIR}archive_toc_lexer.c ${SRC_DIR}archive_toc_grammar.c,$(wildcard ${SRC_DIR}*.c)))
OBJS  = $(patsubst %.c,%.o,$(SRC))
OBJS += archive_toc_lexer.o
OBJS += archive_toc_grammar.o
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
DEFAULT_CFLAGS += -Wformat
DEFAULT_CFLAGS += -I $(shell $(PG_CONFIG) --includedir)
DEFAULT_CFLAGS += -I $(shell $(PG_CONFIG) --includedir-server)
DEFAULT_CFLAGS += -I $(shell $(PG_CONFIG) --pkgincludedir)/internal
DEFAULT_CFLAGS += $(shell $(PG_CONFIG) --cflags)
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


# Flags taken from: https://liquid.microsoft.com/Web/Object/Read/ms.security/Requirements/Microsoft.Security.SystemsADM.10203#guide
SECURITY_CFLAGS = -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2
SECURITY_CFLAGS += -fpie -Wl,-pie
SECURITY_CFLAGS += -Wformat -Wformat-security -Werror=format-security

ifneq ($(shell uname -s),Darwin)
SECURITY_CFLAGS += -z noexecstack -Wl,-z,relro -Wl,-z,now
endif

DEFAULT_CFLAGS += $(SECURITY_CFLAGS)

override CFLAGS := $(DEFAULT_CFLAGS) $(CFLAGS)

JENKINS_CFLAGS = $(CFLAGS) -Wno-implicit-fallthrough

LIBS  = -L $(shell $(PG_CONFIG) --pkglibdir)
LIBS += -L $(shell $(PG_CONFIG) --libdir)
LIBS += $(shell $(PG_CONFIG) --ldflags)
LIBS += $(shell $(PG_CONFIG) --libs)
LIBS += -lpq
LIBS += -lncurses
LIBS += -lgc
# meson-built PostgreSQL leaves pg_config --libs empty (PG16 through PG19+);
# we must link libpgcommon and libpgport explicitly for strlcpy, pg_usleep, etc.
LIBS += -lpgcommon
LIBS += -lpgport
LIBS += -lm

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
	rm -rf $(DEPDIR)

install: $(PGCOPYDB)
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 $(PGCOPYDB) $(DESTDIR)$(BINDIR)

SQL_AWK   := $(SRC_DIR)sql/sql2c.awk
SQL_FILES := $(sort $(wildcard $(SRC_DIR)sql/*.sql))

$(SRC_DIR)sql/sql_queries_data.inc: $(SQL_FILES) $(SQL_AWK)
	$(MAKE) -C $(SRC_DIR)sql

# sql_queries.c #includes sql/sql_queries_data.inc
sql_queries.o: $(SRC_DIR)sql/sql_queries_data.inc

gen-sql: $(SRC_DIR)sql/sql_queries_data.inc

FLEX  ?= flex
BISON ?= bison

# archive_toc_grammar.c/.h are generated from archive_toc_grammar.y, and
# archive_toc_lexer.c from archive_toc_lexer.l, the same way PostgreSQL
# generates gram.c/scan.c from gram.y/scan.l: neither is tracked in git
# (see .gitignore) and both are rebuilt here whenever missing or older
# than their own source. Building from a checkout therefore requires
# flex and bison, same as PostgreSQL itself does.
#
# archive_toc_lexer.l #includes the bison-generated archive_toc_grammar.h
# (for the token codes archive_toc_lexer.c returns), so that one extra
# prerequisite below makes sure bison has already run before flex does,
# on a from-scratch build where neither generated file exists yet.
#
# Every target/prerequisite here must be spelled with the same (here,
# relative) names used elsewhere in this Makefile to build archive_toc_
# lexer.o/archive_toc_grammar.o -- $(SRC_DIR)-prefixed absolute paths are
# a different string to make, so the implicit %.o: %.c chain wouldn't
# find these rules, falling back to make's own built-in %.c: %.l and
# %.c: %.y rules (which run plain "lex"/"yacc", not $(FLEX)/$(BISON), and
# treat the result as a disposable intermediate file, deleting it right
# after the build).
archive_toc_grammar.c archive_toc_grammar.h: archive_toc_grammar.y
	$(BISON) -d -o archive_toc_grammar.c $<

archive_toc_lexer.c: archive_toc_lexer.l archive_toc_grammar.h
	$(FLEX) -o $@ $<

# archive_toc_parser.c and archive_toc_grammar.c itself also #include
# archive_toc_grammar.h directly; on a from-scratch parallel build (no
# .deps/*.Po files yet to carry this automatically) make has no other
# way to know archive_toc_parser.o needs bison to have run first, same
# ordering hazard the lexer rule above already guards against.
archive_toc_parser.o: archive_toc_grammar.h

gen-lexer: archive_toc_lexer.c archive_toc_grammar.c

.PHONY: all monitor clean gen-sql gen-lexer
.PHONY: VERSION-FILE
