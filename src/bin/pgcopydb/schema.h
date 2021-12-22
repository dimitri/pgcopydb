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


/*
 * SourceIndex caches the information we need about all the indexes attached to
 * the ordinary tables found in the source database.
 */
typedef struct SourceIndex
{
	uint32_t indexOid;
	char indexNamespace[NAMEDATALEN];
	char indexRelname[NAMEDATALEN];
	uint32_t tableOid;
	char tableNamespace[NAMEDATALEN];
	char tableRelname[NAMEDATALEN];
	bool isPrimary;
	bool isUnique;
	char indexColumns[BUFSIZE];
	char indexDef[BUFSIZE];
	char constraintName[NAMEDATALEN];
	char constraintDef[BUFSIZE];
} SourceIndex;


typedef struct SourceIndexArray
{
	int count;
	SourceIndex *array;			/* malloc'ed area */
} SourceIndexArray;


bool schema_list_ordinary_tables(PGSQL *pgsql, SourceTableArray *tableArray);
bool schema_list_all_indexes(PGSQL *pgsql, SourceIndexArray *indexArray);

#endif /* SCHEMA_H */
