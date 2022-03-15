/*
 * src/bin/pgcopydb/parsing.c
 *   API for parsing the output of some PostgreSQL server commands.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#ifndef PARSING_H
#define PARSING_H

#include <stdbool.h>

#include "pgsql.h"

char * regexp_first_match(const char *string, const char *re);

bool parse_version_number(const char *version_string,
						  char *pg_version_string,
						  size_t size,
						  int *pg_version);

bool parse_dotted_version_string(const char *pg_version_string,
								 int *pg_version);
bool parse_pg_version_string(const char *pg_version_string,
							 int *pg_version);
bool parse_bool(const char *value, bool *result);

#define boolToString(value) (value) ? "true" : "false"


/*
 * To parse Postgres URI we need to store keywords and values in separate
 * arrays of strings, because that's the libpq way of doing things.
 *
 * keywords and values are arrays of string and the arrays must be large enough
 * to fit all the connection parameters (of which we count 36 at the moment on
 * the Postgres documentation).
 *
 * See https://www.postgresql.org/docs/current/libpq-connect.html
 *
 * So here we use 64 entries each of MAXCONNINFO, to ensure we have enough room
 * to store all the parts of a typicallay MAXCONNINFO bounded full URI. That
 * amounts to 64kB of memory, so that's not even a luxury.
 */
typedef struct KeyVal
{
	int count;
	char keywords[64][MAXCONNINFO];
	char values[64][MAXCONNINFO];
} KeyVal;


/*
 * In our own internal processing of Postgres URIs, we want to have some of the
 * URL parts readily accessible by name rather than mixed in the KeyVal
 * structure.
 *
 * That's mostly becase we want to produce an URI with the following form:
 *
 *  postgres://user@host:port/dbname?opt=val
 */
typedef struct URIParams
{
	char username[MAXCONNINFO];
	char hostname[MAXCONNINFO];
	char port[MAXCONNINFO];
	char dbname[MAXCONNINFO];
	KeyVal parameters;
} URIParams;


typedef struct SafeURI
{
	char pguri[MAXCONNINFO];
	char password[MAXCONNINFO];
	URIParams uriParams;
} SafeURI;

bool parse_pguri_info_key_vals(const char *pguri,
							   KeyVal *overrides,
							   URIParams *uriParameters,
							   bool checkForCompleteURI);

bool buildPostgresURIfromPieces(URIParams *uriParams, char *pguri);

bool escapeWithPercentEncoding(const char *str, char *dst);

bool parse_and_scrub_connection_string(const char *pguri, char *scrubbedPguri);

bool extract_connection_string_password(const char *pguri, SafeURI *safeURI);

#endif /* PARSING_H */
