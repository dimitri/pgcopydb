/*-------------------------------------------------------------------------
 *
 * port.h
 *	  Header for src/port/ compatibility functions.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SNPRINTF_H
#define PG_SNPRINTF_H

#include "postgres_fe.h"

#ifndef USE_REPL_SNPRINTF

int	pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args);
int	pg_snprintf(char *str, size_t count, const char *fmt,...)
 	__attribute__((format(printf, 3, 4)));
int	pg_vsprintf(char *str, const char *fmt, va_list args);
int	pg_sprintf(char *str, const char *fmt,...)
 	__attribute__((format(printf, 2, 3)));
int	pg_vfprintf(FILE *stream, const char *fmt, va_list args);
int	pg_fprintf(FILE *stream, const char *fmt,...)
 	__attribute__((format(printf, 2, 3)));
int	pg_vprintf(const char *fmt, va_list args);
int	pg_printf(const char *fmt,...)
 	__attribute__((format(printf, 1, 2)));	

/* This is also provided by snprintf.c */
int	pg_strfromd(char *str, size_t count, int precision, double value);

/* Replace strerror() with our own, somewhat more robust wrapper */
extern char *pg_strerror(int errnum);
#define strerror pg_strerror

/* Likewise for strerror_r(); note we prefer the GNU API for that */
extern char *pg_strerror_r(int errnum, char *buf, size_t buflen);
#define strerror_r pg_strerror_r
#define PG_STRERROR_R_BUFLEN 256	/* Recommended buffer size for strerror_r */

#endif	/* USE_REPL_SNPRINTF */

#endif	/* PG_SNPRINTF_H */
