/*-------------------------------------------------------------------------
 *
 * String-processing utility routines for frontend code
 *
 * Utility functions that interpret backend output or quote strings for
 * assorted contexts.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/fe_utils/string_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STRING_UTILS_H
#define STRING_UTILS_H

#include "libpq-fe.h"
#include "pqexpbuffer.h"

extern void appendStringLiteral(PQExpBuffer buf, const char *str,
								int encoding, bool std_strings);
extern void appendStringLiteralConn(PQExpBuffer buf, const char *str,
									PGconn *conn);

#endif							/* STRING_UTILS_H */
