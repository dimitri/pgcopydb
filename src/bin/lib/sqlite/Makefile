# See https://sqlite.org/howtocompile.html

SRC = $(wildcard *.c)
INC = $(wildcard *.h)
OBJS = $(patsubst %.c,%.o,$(SRC))

PG_CONFIG ?= pg_config

CC = $(shell $(PG_CONFIG) --cc)
DEFAULT_CFLAGS = $(shell $(PG_CONFIG) --cflags)

ifdef DEBUG
# Use optimization option that provides good debugging experience
# https://gcc.gnu.org/onlinedocs/gcc/Optimize-Options.html.
# It overrides '-O2' flag of pg_config.
DEFAULT_CFLAGS += -Og
endif

SQLITE_CFLAGS += -DSQLITE_THREADSAFE=0
SQLITE_CFLAGS += -DSQLITE_OMIT_LOAD_EXTENSION
SQLITE_CFLAGS += -DHAVE_READLINE
SQLITE_CFLAGS += -DSQLITE_ENABLE_MATH_FUNCTIONS
#SQLITE_CFLAGS += -DSQLITE_ENABLE_NORMALIZE

SQLITE_CFLAGS += -Wno-missing-prototypes
SQLITE_CFLAGS += -Wno-implicit-fallthrough

override CFLAGS := $(DEFAULT_CFLAGS) $(CFLAGS) $(SQLITE_CFLAGS)

LIBS += -lm
LIBS += -lreadline
LIBS += -lncurses

sqlite3: $(SRC) $(INC)
	$(CC) $(CFLAGS) $(LIBS) $(SRC) -o $@

sqlite3.o: sqlite3.c sqlite3.h sqlite3ext.h
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f $(OBJS) sqlite3
