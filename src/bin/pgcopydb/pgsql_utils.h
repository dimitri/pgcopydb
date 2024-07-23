/*
 * src/bin/pgcopydb/pgsql_utils.h
 *     Common helper functions for interacting with a postgres server
 */
#ifndef PGSQL_UTILS_H
#define PGSQL_UTILS_H


#include "libpq-fe.h"

/* pgsql.c */
bool is_response_ok(PGresult *result);
bool clear_results(PGSQL *pgsql);

#endif /* PGSQL_UTILS_H */
