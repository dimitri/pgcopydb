/*
 * Copyright (c) 2017 rxi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "snprintf.h"

#include "log.h"

static struct {
  void *udata;
  log_LockFn lock;
  FILE *fp;
  int level;
  int quiet;
  int showLineNumber;
  int useColors;
  char tformat[128];
} L = { 0 };


static const char *level_names[] = {
	"TRACE",
	"DEBUG",
	"SQL",
	"NOTICE",
	"INFO",
	"WARN",
	"ERROR",
	"FATAL"
};

static const char *level_colors[] = {
  "\x1b[90m",					/* TRACE:  bright black (light gray) */
  "\x1b[34m",					/* DEBUG:  blue */
  "\x1b[30m",					/* SQL:    black */
  "\x1b[36m",					/* NOTICE: cyan */
  "\x1b[32m",					/* INFO:   green */
  "\x1b[33m",					/* WARN:   yellow */
  "\x1b[31m",					/* ERROR:  red */
  "\x1b[35m"					/* FATAL:  magenta */
};


static void lock(void)   {
  if (L.lock) {
    L.lock(L.udata, 1);
  }
}


static void unlock(void) {
  if (L.lock) {
    L.lock(L.udata, 0);
  }
}


void log_set_udata(void *udata) {
  L.udata = udata;
}


void log_set_lock(log_LockFn fn) {
  L.lock = fn;
}


void log_set_fp(FILE *fp) {
  L.fp = fp;
}


void log_set_level(int level) {
  L.level = level;
}

int log_get_level(void) {
	return L.level;
}


void log_set_quiet(int enable) {
  L.quiet = enable ? 1 : 0;
}


void log_use_colors(int enable) {
  L.useColors = enable ? 1 : 0;
}


void log_show_file_line(int enable) {
  L.showLineNumber = enable ? 1 : 0;
}


void log_set_tformat(const char *tformat) {
  strlcpy(L.tformat, tformat, sizeof(L.tformat));
}


void log_log(int level, const char *file, int line, const char *fmt, ...)
{
  time_t t;
  struct tm *lt;

  if (level < L.level) {
    return;
  }

  if (fmt == NULL)
  {
	  return;
  }

  /* initialize L.tformat with default value, if necessary */
  if (L.tformat[0] == '\0')
  {
	  strlcpy(L.tformat, LOG_TFORMAT_LONG, sizeof(L.tformat));
  }

  /* Acquire lock */
  lock();

  /* Get current time */
  t = time(NULL);
  lt = localtime(&t);

  /* Log to stderr */
  if (!L.quiet) {
    va_list args;
    char buf[128] = { 0 };
	int showLineNumber = L.showLineNumber || L.level <= 1;

    buf[strftime(buf, sizeof(buf), L.tformat, lt)] = '\0';

	if (L.useColors)
	{
		pg_fprintf(stderr, "%s %d %s%-6s\x1b[0m ",
				   buf,
				   getpid(),
				   level_colors[level],
				   level_names[level]);

		if (showLineNumber)
		{
			pg_fprintf(stderr, "\x1b[90m%s:%d:\x1b[0m ", file, line);
		}
	}
	else
	{
		pg_fprintf(stderr, "%s %d %-6s ", buf, getpid(), level_names[level]);

		if (showLineNumber)
		{
			pg_fprintf(stderr, "%s:%d ", file, line);
		}
	}

    va_start(args, fmt);
    pg_vfprintf(stderr, fmt, args);
    va_end(args);
    pg_fprintf(stderr, "\n");
  }

  /* Log to file */
  if (L.fp) {
    va_list args;
    char buf[32];

	/* always use the long time format when writting to file */
    buf[strftime(buf, sizeof(buf), LOG_TFORMAT_LONG, lt)] = '\0';

	/* always add all the details when writting to file */
    pg_fprintf(L.fp, "%s %d %-6s %s:%d: ",
			   buf, getpid(), level_names[level], file, line);

    va_start(args, fmt);
    pg_vfprintf(L.fp, fmt, args);
    va_end(args);
    pg_fprintf(L.fp, "\n");
  }

  /* Release lock */
  unlock();
}
