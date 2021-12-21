/*
 * src/bin/pgcopydb/schema.h
 *	 SQL queries to discover the source database schema
 */

#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdbool.h>

#include "pgsql.h"

/*
 * SourceTable caches the information we need about all the ordinary tables
 * found in the source database.
 */
typedef struct SourceTable
{
	uint32_t oid;
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
	int64_t reltuples;
} SourceTable;


typedef struct SourceTableArray
{
	int count;
	SourceTable *array;			/* malloc'ed area */
} SourceTableArray;


bool schema_list_ordinary_tables(PGSQL *pgsql, SourceTableArray *tableArray);

#endif /* SCHEMA_H */
