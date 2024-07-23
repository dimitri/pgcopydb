/*
 * src/bin/pgcopydb/pgsql_utils.h
 *     Common helper functions for interacting with a postgres server
 */

#include "libpq-fe.h"

bool is_response_ok(PGresult *result);
bool clear_results(PGSQL *pgsql);
