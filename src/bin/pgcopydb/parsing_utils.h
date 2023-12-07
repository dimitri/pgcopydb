/*
 * src/bin/pgcopydb/parsing.c
 *   API for parsing the output of some PostgreSQL server commands.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#ifndef PARSING_UTILS_H
#define PARSING_UTILS_H

#include <stdbool.h>

/*
 * Maximum connection info length as used in walreceiver.h
 */
#define MAXCONNINFO 1024

char * regexp_first_match(const char *string, const char *re);

bool parse_version_number(const char *version_string,
						  char *pg_version_string,
						  size_t size,
						  int *pg_version);

bool parse_dotted_version_string(const char *pg_version_string,
								 int *pg_version);
bool parse_pg_version_string(const char *pg_version_string,
							 int *pg_version);

bool parseLSN(const char *str, uint64_t *lsn);
bool parse_bool(const char *value, bool *result);

#define boolToString(value) (value) ? "true" : "false"

bool parse_pretty_printed_bytes(const char *value, uint64_t *result);


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
	char *keywords[64];
	char *values[64];
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
	char *username;
	char *hostname;
	char *port;
	char *dbname;
	KeyVal parameters;
} URIParams;


typedef struct SafeURI
{
	char *pguri;                /* malloc'ed area */
	char *password;             /* malloc'ed area */
	URIParams uriParams;
} SafeURI;


typedef struct ConnStrings
{
	char *source_pguri;         /* malloc'ed area */
	char *target_pguri;         /* malloc'ed area */
	char *logrep_pguri;         /* malloc'ed area */

	SafeURI safeSourcePGURI;
	SafeURI safeTargetPGURI;
} ConnStrings;


bool parse_pguri_info_key_vals(const char *pguri,
							   KeyVal *defaults,
							   KeyVal *overrides,
							   URIParams *uriParameters,
							   bool checkForCompleteURI);

bool buildPostgresBareURIfromPieces(URIParams *uriParams, char **pguri);
bool buildPostgresURIfromPieces(URIParams *uriParams, char **pguri);

bool escapeWithPercentEncoding(const char *str, char **dst);

bool parse_and_scrub_connection_string(const char *pguri, SafeURI *safeURI);

bool bareConnectionString(const char *pguri, SafeURI *safeURI);

void freeSafeURI(SafeURI *safeURI);
void freeURIParams(URIParams *params);
void freeKeyVal(KeyVal *parameters);

#endif /* PARSING_UTILS_H */
