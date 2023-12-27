/**
 * Copyright (c) 2017 rxi
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the MIT license. See `log.c` for details.
 */

#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <stdarg.h>

#define LOG_VERSION "0.1.3"

#define LOG_TFORMAT_LONG "%Y-%m-%d %H:%M:%S"
#define LOG_TFORMAT_SHORT "%H:%M:%S"

/* used for JSON logs format "log" message */
#define LOG_BUFSIZE 2048

typedef void (*log_LockFn)(void *udata, int lock);

enum {
	LOG_TRACE,
	LOG_SQLITE,
	LOG_DEBUG,
	LOG_SQL,
	LOG_NOTICE,
	LOG_INFO,
	LOG_WARN,
	LOG_ERROR,
	LOG_FATAL
};

#define log_trace(...) log_log(LOG_TRACE, __FILE__, __LINE__, __VA_ARGS__)
#define log_sqlite(...) log_log(LOG_SQLITE, __FILE__, __LINE__, __VA_ARGS__)
#define log_debug(...) log_log(LOG_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define log_sql(...) log_log(LOG_SQL, __FILE__, __LINE__, __VA_ARGS__)
#define log_notice(...)  log_log(LOG_NOTICE,  __FILE__, __LINE__, __VA_ARGS__)
#define log_info(...)  log_log(LOG_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define log_warn(...)  log_log(LOG_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define log_error(...) log_log(LOG_ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define log_fatal(...) log_log(LOG_FATAL, __FILE__, __LINE__, __VA_ARGS__)

#define log_level(level, ...) log_log(level, __FILE__, __LINE__, __VA_ARGS__)

void log_set_udata(void *udata);
void log_set_lock(log_LockFn fn);
void log_set_fp(FILE *fp);
void log_set_level(int level);
int log_get_level(void);
void log_set_quiet(int enable);
void log_use_colors(int enable);
void log_use_json(int enable);
void log_use_json_file(int enable);
void log_show_file_line(int enable);
void log_set_tformat(const char *tformat);

void log_log(int level, const char *file, int line, const char *fmt, ...)
 	__attribute__((format(printf, 4, 5)));

#endif
