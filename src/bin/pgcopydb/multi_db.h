/*
 * src/bin/pgcopydb/multi_db.h
 *	 Clone all databases from a source Postgres instance.
 */

#ifndef MULTI_DB_H
#define MULTI_DB_H

#include "copydb.h"
#include "schema.h"

bool clone_all_databases(CopyDataSpec *copySpecs);

bool multidb_build_uri_for_database(const char *pguri,
									const char *datname,
									char **result_uri);

#endif /* MULTI_DB_H */
