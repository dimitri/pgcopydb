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
	int64_t bytes;
	char bytesPretty[NAMEDATALEN]; /* pg_size_pretty */
} SourceTable;


typedef struct SourceTableArray
{
	int count;
	SourceTable *array;         /* malloc'ed area */
} SourceTableArray;


/*
 * SourceSequence caches the information we need about all the sequences found
 * in the source database.
 */
typedef struct SourceSequence
{
	uint32_t oid;
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
	int64_t lastValue;
	bool isCalled;
} SourceSequence;


typedef struct SourceSequenceArray
{
	int count;
	SourceSequence *array;         /* malloc'ed area */
} SourceSequenceArray;

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
	uint32_t constraintOid;
	char constraintName[NAMEDATALEN];
	char constraintDef[BUFSIZE];
} SourceIndex;


typedef struct SourceIndexArray
{
	int count;
	SourceIndex *array;         /* malloc'ed area */
} SourceIndexArray;


bool schema_list_ordinary_tables(PGSQL *pgsql, SourceTableArray *tableArray);

bool schema_list_ordinary_tables_without_pk(PGSQL *pgsql,
											SourceTableArray *tableArray);

bool schema_list_sequences(PGSQL *pgsql, SourceSequenceArray *seqArray);

bool schema_get_sequence_value(PGSQL *pgsql, SourceSequence *seq);
bool schema_set_sequence_value(PGSQL *pgsql, SourceSequence *seq);

bool schema_list_all_indexes(PGSQL *pgsql, SourceIndexArray *indexArray);

bool schema_list_table_indexes(PGSQL *pgsql,
							   const char *shemaName,
							   const char *tableName,
							   SourceIndexArray *indexArray);

#endif /* SCHEMA_H */
