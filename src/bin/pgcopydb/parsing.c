/*
 * src/bin/pgcopydb/parsing.c
 *   API for parsing the output of some PostgreSQL server commands.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#include <errno.h>
#include <inttypes.h>
#include <regex.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "parsing.h"
#include "file_utils.h"
#include "string_utils.h"

static bool parse_bool_with_len(const char *value, size_t len, bool *result);

#define RE_MATCH_COUNT 10


/*
 * Simple Regexp matching that returns the first matching element.
 */
char *
regexp_first_match(const char *string, const char *regex)
{
	regex_t compiledRegex;

	regmatch_t m[RE_MATCH_COUNT];

	if (string == NULL)
	{
		return NULL;
	}

	int status = regcomp(&compiledRegex, regex, REG_EXTENDED | REG_NEWLINE);

	if (status != 0)
	{
		/*
		 * regerror() returns how many bytes are actually needed to host the
		 * error message, and truncates the error message when it doesn't fit
		 * in given size. If the message has been truncated, then we add an
		 * ellispis to our log entry.
		 *
		 * We could also dynamically allocate memory for the error message, but
		 * the error might be "out of memory" already...
		 */
		char message[BUFSIZE];
		size_t bytes = regerror(status, &compiledRegex, message, BUFSIZE);

		log_error("Failed to compile regex \"%s\": %s%s",
				  regex, message, bytes < BUFSIZE ? "..." : "");

		regfree(&compiledRegex);

		return NULL;
	}

	/*
	 * regexec returns 0 if the regular expression matches; otherwise, it
	 * returns a nonzero value.
	 */
	int matchStatus = regexec(&compiledRegex, string, RE_MATCH_COUNT, m, 0);
	regfree(&compiledRegex);

	/* We're interested into 1. re matches 2. captured at least one group */
	if (matchStatus != 0 || m[0].rm_so == -1 || m[1].rm_so == -1)
	{
		return NULL;
	}
	else
	{
		regoff_t start = m[1].rm_so;
		regoff_t finish = m[1].rm_eo;
		int length = finish - start + 1;
		char *result = (char *) malloc(length * sizeof(char));

		if (result == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return NULL;
		}

		strlcpy(result, string + start, length);

		return result;
	}
	return NULL;
}


/*
 * Parse the version number output from pg_ctl --version:
 *    pg_ctl (PostgreSQL) 10.3
 */
bool
parse_version_number(const char *version_string,
					 char *pg_version_string,
					 size_t size,
					 int *pg_version)
{
	char *match = regexp_first_match(version_string, "([0-9.]+)");

	if (match == NULL)
	{
		log_error("Failed to parse Postgres version number \"%s\"",
				  version_string);
		return false;
	}

	/* first, copy the version number in our expected result string buffer */
	strlcpy(pg_version_string, match, size);

	if (!parse_pg_version_string(pg_version_string, pg_version))
	{
		/* errors have already been logged */
		free(match);
		return false;
	}

	free(match);
	return true;
}


/*
 * parse_dotted_version_string parses a major.minor dotted version string such
 * as "12.6" into a single number in the same format as the pg_control_version,
 * such as 1206.
 */
bool
parse_dotted_version_string(const char *pg_version_string, int *pg_version)
{
	/* now, parse the numbers into an integer, ala pg_control_version */
	bool dotFound = false;
	char major[INTSTRING_MAX_DIGITS] = { 0 };
	char minor[INTSTRING_MAX_DIGITS] = { 0 };

	int majorIdx = 0;
	int minorIdx = 0;

	if (pg_version_string == NULL)
	{
		log_debug("BUG: parse_pg_version_string got NULL");
		return false;
	}

	for (int i = 0; pg_version_string[i] != '\0'; i++)
	{
		if (pg_version_string[i] == '.')
		{
			if (dotFound)
			{
				log_error("Failed to parse Postgres version number \"%s\"",
						  pg_version_string);
				return false;
			}

			dotFound = true;
			continue;
		}

		if (dotFound)
		{
			minor[minorIdx++] = pg_version_string[i];
		}
		else
		{
			major[majorIdx++] = pg_version_string[i];
		}
	}

	/* Postgres alpha/beta versions report version "14" instead of "14.0" */
	if (!dotFound)
	{
		strlcpy(minor, "0", INTSTRING_MAX_DIGITS);
	}

	int maj = 0;
	int min = 0;

	if (!stringToInt(major, &maj) ||
		!stringToInt(minor, &min))
	{
		log_error("Failed to parse Postgres version number \"%s\"",
				  pg_version_string);
		return false;
	}

	/* transform "12.6" into 1206, that is 12 * 100 + 6 */
	*pg_version = (maj * 100) + min;

	return true;
}


/*
 * parse_pg_version_string parses a Postgres version string such as "12.6" into
 * a single number in the same format as the pg_control_version, such as 1206.
 */
bool
parse_pg_version_string(const char *pg_version_string, int *pg_version)
{
	return parse_dotted_version_string(pg_version_string, pg_version);
}


/*
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 *
 * Copied from PostgreSQL sources
 * file : src/backend/utils/adt/bool.c
 */
static bool
parse_bool_with_len(const char *value, size_t len, bool *result)
{
	switch (*value)
	{
		case 't':
		case 'T':
		{
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
				{
					*result = true;
				}
				return true;
			}
			break;
		}

		case 'f':
		case 'F':
		{
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
				{
					*result = false;
				}
				return true;
			}
			break;
		}

		case 'y':
		case 'Y':
		{
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
				{
					*result = true;
				}
				return true;
			}
			break;
		}

		case 'n':
		case 'N':
		{
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
				{
					*result = false;
				}
				return true;
			}
			break;
		}

		case 'o':
		case 'O':
		{
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
				{
					*result = true;
				}
				return true;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
				{
					*result = false;
				}
				return true;
			}
			break;
		}

		case '1':
		{
			if (len == 1)
			{
				if (result)
				{
					*result = true;
				}
				return true;
			}
			break;
		}

		case '0':
		{
			if (len == 1)
			{
				if (result)
				{
					*result = false;
				}
				return true;
			}
			break;
		}

		default:
		{
			break;
		}
	}

	if (result)
	{
		*result = false;        /* suppress compiler warning */
	}
	return false;
}


/*
 * parse_bool parses boolean text value (true/false/on/off/yes/no/1/0) and
 * puts the boolean value back in the result field if it is not NULL.
 * The function returns true on successful parse, returns false if any parse
 * error is encountered.
 */
bool
parse_bool(const char *value, bool *result)
{
	return parse_bool_with_len(value, strlen(value), result);
}


/*
 * parse_pguri_info_key_vals decomposes elements of a Postgres connection
 * string (URI) into separate arrays of keywords and values as expected by
 * PQconnectdbParams.
 */
bool
parse_pguri_info_key_vals(const char *pguri,
						  KeyVal *overrides,
						  URIParams *uriParameters,
						  bool checkForCompleteURI)
{
	char *errmsg;
	PQconninfoOption *conninfo, *option;

	bool foundHost = false;
	bool foundUser = false;
	bool foundPort = false;
	bool foundDBName = false;

	int paramIndex = 0;

	conninfo = PQconninfoParse(pguri, &errmsg);
	if (conninfo == NULL)
	{
		log_error("Failed to parse pguri \"%s\": %s", pguri, errmsg);
		PQfreemem(errmsg);
		return false;
	}

	for (option = conninfo; option->keyword != NULL; option++)
	{
		char *value = NULL;
		int ovIndex = 0;

		/*
		 * If the keyword is in our overrides array, use the value from the
		 * override values. Yeah that's O(n*m) but here m is expected to be
		 * something very small, like 3 (typically: sslmode, sslrootcert,
		 * sslcrl).
		 */
		for (ovIndex = 0; ovIndex < overrides->count; ovIndex++)
		{
			if (strcmp(overrides->keywords[ovIndex], option->keyword) == 0)
			{
				value = overrides->values[ovIndex];
			}
		}

		/* not found in the override, keep the original, or skip */
		if (value == NULL)
		{
			if (option->val == NULL || strcmp(option->val, "") == 0)
			{
				continue;
			}
			else
			{
				value = option->val;
			}
		}

		if (strcmp(option->keyword, "host") == 0 ||
			strcmp(option->keyword, "hostaddr") == 0)
		{
			foundHost = true;
			strlcpy(uriParameters->hostname, option->val, MAXCONNINFO);
		}
		else if (strcmp(option->keyword, "port") == 0)
		{
			foundPort = true;
			strlcpy(uriParameters->port, option->val, MAXCONNINFO);
		}
		else if (strcmp(option->keyword, "user") == 0)
		{
			foundUser = true;
			strlcpy(uriParameters->username, option->val, MAXCONNINFO);
		}
		else if (strcmp(option->keyword, "dbname") == 0)
		{
			foundDBName = true;
			strlcpy(uriParameters->dbname, option->val, MAXCONNINFO);
		}
		else if (!IS_EMPTY_STRING_BUFFER(value))
		{
			/* make a copy in our key/val arrays */
			strlcpy(uriParameters->parameters.keywords[paramIndex],
					option->keyword,
					MAXCONNINFO);

			strlcpy(uriParameters->parameters.values[paramIndex],
					value,
					MAXCONNINFO);

			++uriParameters->parameters.count;
			++paramIndex;
		}
	}

	PQconninfoFree(conninfo);

	/*
	 * Display an error message per missing field, and only then return false
	 * if we're missing any one of those.
	 */
	if (checkForCompleteURI)
	{
		if (!foundHost)
		{
			log_error("Failed to find hostname in the pguri \"%s\"", pguri);
		}

		if (!foundPort)
		{
			log_error("Failed to find port in the pguri \"%s\"", pguri);
		}

		if (!foundUser)
		{
			log_error("Failed to find username in the pguri \"%s\"", pguri);
		}

		if (!foundDBName)
		{
			log_error("Failed to find dbname in the pguri \"%s\"", pguri);
		}

		return foundHost && foundPort && foundUser && foundDBName;
	}
	else
	{
		return true;
	}
}


/*
 * buildPostgresURIfromPieces builds a Postgres connection string from keywords
 * and values, in a user friendly way. The pguri parameter should point to a
 * memory area that has been allocated by the caller and has at least
 * MAXCONNINFO bytes.
 */
bool
buildPostgresURIfromPieces(URIParams *uriParams, char *pguri)
{
	int index = 0;

	char escapedUsername[MAXCONNINFO] = { 0 };
	char escapedHostname[MAXCONNINFO] = { 0 };
	char escapedDBName[MAXCONNINFO] = { 0 };

	/*
	 * We want to escape uriParams username, hostname, and dbname, in exactly
	 * the same way, and also wish to report which URI parameter we failed to
	 * escape in case of errors, and we also want to avoid copy pasting the
	 * same code 3 times in a row.
	 *
	 * We would rather loop over an array where we would have the parameter
	 * name (keyword), and a pointer to the value to escape, and a pointer to
	 * where to store the escaped string bytes then.
	 */
	struct namedPair
	{
		char *name;
		char *raw;
		char *escaped;
	};

	struct namedPair args[] = {
		{ "username", uriParams->username, escapedUsername },
		{ "hostname", uriParams->hostname, escapedHostname },
		{ "dbname", uriParams->dbname, escapedDBName },
		{ NULL, NULL, NULL }
	};

	for (int i = 0; args[i].name != NULL; i++)
	{
		if (!escapeWithPercentEncoding(args[i].raw, args[i].escaped))
		{
			log_error("Failed to percent-escape URI %s", args[i].name);
			return false;
		}
	}

	/* prepare the mandatory part of the Postgres URI */
	sformat(pguri, MAXCONNINFO,
			"postgres://%s@%s:%s/%s?",
			escapedUsername,
			escapedHostname,
			uriParams->port,
			escapedDBName);

	/* now add optional parameters to the Postgres URI */
	for (index = 0; index < uriParams->parameters.count; index++)
	{
		char *keyword = uriParams->parameters.keywords[index];
		char *value = uriParams->parameters.values[index];

		char escapedValue[MAXCONNINFO] = { 0 };

		/* if we have password="****" then just keep that */
		if (streq(keyword, "password") && streq(value, PASSWORD_MASK))
		{
			strlcpy(escapedValue, value, sizeof(escapedValue));
		}
		else
		{
			if (!escapeWithPercentEncoding(value, escapedValue))
			{
				if (streq(keyword, "password"))
				{
					log_error("Failed to percent-escape URI parameter \"%s\"",
							  "password");
				}
				else
				{
					log_error("Failed to percent-escape URI parameter \"%s\" "
							  "value \"%s\"",
							  keyword, value);
				}
				return false;
			}
		}

		if (index == 0)
		{
			sformat(pguri, MAXCONNINFO,
					"%s%s=%s",
					pguri, keyword, escapedValue);
		}
		else
		{
			sformat(pguri, MAXCONNINFO,
					"%s&%s=%s",
					pguri, keyword, escapedValue);
		}
	}

	return true;
}


/*
 * escapeWithPercentEncoding applies percent-encoding as required by Postgres
 * URI parsing. The destination buffer must have been allocated already and be
 * of size MAXCONNINFO.
 *
 * See https://www.postgresql.org/docs/current/libpq-connect.html
 * See https://datatracker.ietf.org/doc/html/rfc3986#section-2.1
 */
bool
escapeWithPercentEncoding(const char *str, char *dst)
{
	const char empty[MAXCONNINFO] = { 0 };
	const char *hex = "0123456789abcdef";

	if (str == NULL || IS_EMPTY_STRING_BUFFER(str))
	{
		strlcpy(dst, empty, MAXCONNINFO);

		return true;
	}

	int pos = 0;
	int len = strlen(str);

	for (int i = 0; i < len; i++)
	{
		/*
		 * 2.3 Unreserved Characters
		 *
		 * https://datatracker.ietf.org/doc/html/rfc3986#section-2.3
		 *
		 *       unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
		 */
		if (isalpha(str[i]) || isdigit(str[i]) ||
			str[i] == '-' ||
			str[i] == '.' ||
			str[i] == '_' ||
			str[i] == '~')
		{
			if (MAXCONNINFO <= (pos + 1))
			{
				/* we really do not expect that to ever happen */
				log_error("BUG: percent-encoded Postgres URI does not fit "
						  "in MAXCONNINFO (%d) bytes", MAXCONNINFO);
				return false;
			}
			dst[pos++] = str[i];
		}

		/*
		 * 2.1 Percent-Encoding
		 *
		 * https://datatracker.ietf.org/doc/html/rfc3986#section-2.1
		 *
		 *       pct-encoded = "%" HEXDIG HEXDIG
		 */
		else
		{
			if (MAXCONNINFO <= (pos + 3))
			{
				/* we really do not expect that to ever happen */
				log_error("BUG: percent-encoded Postgres URI does not fit "
						  "in MAXCONNINFO (%d) bytes", MAXCONNINFO);
				return false;
			}

			dst[pos++] = '%';
			dst[pos++] = hex[str[i] >> 4];
			dst[pos++] = hex[str[i] & 15];
		}
	}

	return true;
}


/*
 * uri_contains_password takes a Postgres connection string and checks to see
 * if it contains a parameter called password. Returns true if a password
 * keyword is present in the connection string.
 */
static bool
uri_contains_password(const char *pguri)
{
	char *errmsg;
	PQconninfoOption *conninfo, *option;

	conninfo = PQconninfoParse(pguri, &errmsg);
	if (conninfo == NULL)
	{
		log_error("Failed to parse pguri: %s", errmsg);

		PQfreemem(errmsg);
		return false;
	}

	/*
	 * Look for a populated password connection parameter
	 */
	for (option = conninfo; option->keyword != NULL; option++)
	{
		if (strcmp(option->keyword, "password") == 0 &&
			option->val != NULL &&
			!IS_EMPTY_STRING_BUFFER(option->val))
		{
			PQconninfoFree(conninfo);
			return true;
		}
	}

	PQconninfoFree(conninfo);
	return false;
}


/*
 * parse_and_scrub_connection_string takes a Postgres connection string and
 * populates scrubbedPguri with the password replaced with **** for logging.
 * The scrubbedPguri parameter should point to a memory area that has been
 * allocated by the caller and has at least MAXCONNINFO bytes.
 */
bool
parse_and_scrub_connection_string(const char *pguri, char *scrubbedPguri)
{
	URIParams uriParams = { 0 };
	KeyVal overrides = { 0 };

	if (uri_contains_password(pguri))
	{
		overrides = (KeyVal) {
			.count = 1,
			.keywords = { "password" },
			.values = { PASSWORD_MASK }
		};
	}

	bool checkForCompleteURI = false;

	if (!parse_pguri_info_key_vals(pguri,
								   &overrides,
								   &uriParams,
								   checkForCompleteURI))
	{
		return false;
	}

	buildPostgresURIfromPieces(&uriParams, scrubbedPguri);

	return true;
}


/*
 * extract_connection_string_password parses the given connection string and if
 * it contains a password, then extracts it into the SafeURI structure and
 * provide a pguri without password instead.
 */
bool
extract_connection_string_password(const char *pguri, SafeURI *safeURI)
{
	char *errmsg;
	PQconninfoOption *conninfo, *option;

	conninfo = PQconninfoParse(pguri, &errmsg);
	if (conninfo == NULL)
	{
		log_error("Failed to parse pguri: %s", errmsg);

		PQfreemem(errmsg);
		return false;
	}

	for (option = conninfo; option->keyword != NULL; option++)
	{
		if (streq(option->keyword, "password"))
		{
			if (option->val != NULL)
			{
				strlcpy(safeURI->password, option->val, MAXCONNINFO);
			}
			continue;
		}
	}

	PQconninfoFree(conninfo);

	URIParams *uriParams = &(safeURI->uriParams);
	KeyVal overrides = {
		.count = 1,
		.keywords = { "password" },
		.values = { "" }
	};

	bool checkForCompleteURI = false;

	if (!parse_pguri_info_key_vals(pguri,
								   &overrides,
								   uriParams,
								   checkForCompleteURI))
	{
		return false;
	}

	buildPostgresURIfromPieces(uriParams, safeURI->pguri);

	return true;
}
