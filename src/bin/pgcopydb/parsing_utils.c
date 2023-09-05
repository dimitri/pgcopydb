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

#include "postgres_fe.h"
#include "libpq-fe.h"
#include "pqexpbuffer.h"

#include "copydb.h"
#include "defaults.h"
#include "log.h"
#include "parsing_utils.h"
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
		char message[BUFSIZE] = { 0 };
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
 * parseLSN is based on the Postgres code for pg_lsn_in_internal found at
 * src/backend/utils/adt/pg_lsn.c in the Postgres source repository. In the
 * pg_auto_failover context we don't need to typedef uint64 XLogRecPtr; so we
 * just use uint64_t internally.
 */
#define MAXPG_LSNCOMPONENT 8

bool
parseLSN(const char *str, uint64_t *lsn)
{
	int len1,
		len2;
	uint32 id,
		   off;

	/* Sanity check input format. */
	len1 = strspn(str, "0123456789abcdefABCDEF");
	if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/')
	{
		return false;
	}

	len2 = strspn(str + len1 + 1, "0123456789abcdefABCDEF");
	if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0')
	{
		return false;
	}

	/* Decode result. */
	id = (uint32) strtoul(str, NULL, 16);
	off = (uint32) strtoul(str + len1 + 1, NULL, 16);
	*lsn = ((uint64) id << 32) | off;

	return true;
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
 * parse_pretty_printed_bytes parses a pretty printed byte value and puts the
 * actual number of bytes in the result field if it is not NULL. The function
 * returns true on successful parse, returns false if any parse error is
 * encountered.
 */
bool
parse_pretty_printed_bytes(const char *value, uint64_t *result)
{
	if (result == NULL)
	{
		log_error("BUG: parse_pretty_printed_bytes called with NULL result");
		return false;
	}

	if (value == NULL)
	{
		log_error("BUG: parse_pretty_printed_bytes called with NULL value");
		return false;
	}

	if (strcmp(value, "") == 0)
	{
		log_error("Failed to parse empty string \"\" as a bytes value");
		return false;
	}

	char *digits = NULL;
	char *unit = NULL;
	char *ptr = (char *) value;

	/* skip front spaces if any */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
	{
		++ptr;
	}

	/* after spaces, we want to find digits, as in " 1234 kB " */
	digits = ptr;

	/* now skip digits in " 1234 kB" until another space or a unit is found */
	while (*ptr != '\0' && isdigit((unsigned char) *ptr))
	{
		++ptr;
	}

	/* not a digit anymore, copy digits into a zero-terminated string */
	char val[BUFSIZE] = { 0 };
	strlcpy(val, digits, ptr - digits + 1);

	uint64_t number = 0;

	if (!stringToUInt64(val, &number))
	{
		/* errors have already been logged */
		log_error("Failed to parse number \"%s\"", digits);
		return false;
	}

	/* now skip spaces again, we want to find the unit (kB, MB, etc) */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
	{
		++ptr;
	}

	unit = ptr;

	/* when we don't have a unit, take the number as it is */
	if (*unit == '\0')
	{
		*result = number;
		return true;
	}

	/* finally remove extra spaces at the end of the unit, if any */
	while (*ptr != '\0' && isalpha((unsigned char) *ptr))
	{
		++ptr;
	}

	*ptr = '\0';

	/* otherwise find the unit in our table and compute the result */
	const char *suffixes[7] = {
		"B",                    /* Bytes */
		"kB",                   /* Kilo */
		"MB",                   /* Mega */
		"GB",                   /* Giga */
		"TB",                   /* Tera */
		"PB",                   /* Peta */
		"EB"                    /* Exa */
	};

	uint64_t res = number;
	uint sIndex = 0;

	while (sIndex < 7 && strcmp(unit, suffixes[sIndex]) != 0)
	{
		++sIndex;

		/* first suffix/unit is "B" which is not a multiplier */
		if (sIndex > 0)
		{
			res = res * 1024;
		}
	}

	if (sIndex == 7)
	{
		log_error("Failed to parse bytes string \"%s\": unknown unit \"%s\"",
				  value,
				  unit);
	}

	*result = res;

	return true;
}


/*
 * parse_pguri_info_key_vals decomposes elements of a Postgres connection
 * string (URI) into separate arrays of keywords and values as expected by
 * PQconnectdbParams.
 */
bool
parse_pguri_info_key_vals(const char *pguri,
						  KeyVal *defaults,
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

		/*
		 * If the keyword is in our overrides array, use the value from the
		 * override values. Yeah that's O(n*m) but here m is expected to be
		 * something very small, like 3 (typically: sslmode, sslrootcert,
		 * sslcrl).
		 */
		for (int ovIndex = 0; ovIndex < overrides->count; ovIndex++)
		{
			if (streq(overrides->keywords[ovIndex], option->keyword))
			{
				value = overrides->values[ovIndex];
			}
		}

		/* now either take the given value or maybe skip the keyword */
		if (value == NULL)
		{
			if (option->val == NULL || streq(option->val, ""))
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
			uriParameters->hostname = strdup(option->val);
		}
		else if (strcmp(option->keyword, "port") == 0)
		{
			foundPort = true;
			uriParameters->port = strdup(option->val);
		}
		else if (strcmp(option->keyword, "user") == 0)
		{
			foundUser = true;
			uriParameters->username = strdup(option->val);
		}
		else if (strcmp(option->keyword, "dbname") == 0)
		{
			foundDBName = true;
			uriParameters->dbname = strdup(option->val);
		}
		else if (value != NULL && !streq(value, ""))
		{
			/* make a copy in our key/val arrays */
			uriParameters->parameters.keywords[paramIndex] =
				strdup(option->keyword);

			uriParameters->parameters.values[paramIndex] =
				strdup(value);

			++uriParameters->parameters.count;
			++paramIndex;
		}
	}

	/*
	 * Now add-in the default values that we have, unless they have been
	 * provided in the previous round.
	 */
	for (int defIndex = 0; defIndex < defaults->count; defIndex++)
	{
		char *keyword = defaults->keywords[defIndex];
		char *value = defaults->values[defIndex];

		bool found = false;

		for (option = conninfo; option->keyword != NULL; option++)
		{
			if (streq(keyword, option->keyword))
			{
				found = option->val != NULL && !streq(option->val, "");
				break;
			}
		}

		if (!found)
		{
			/* make a copy in our key/val arrays */
			uriParameters->parameters.keywords[paramIndex] = strdup(keyword);
			uriParameters->parameters.values[paramIndex] = strdup(value);

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
 * and values, in a user friendly way.
 */
bool
buildPostgresURIfromPieces(URIParams *uriParams, char **pguri)
{
	PQExpBuffer uri = createPQExpBuffer();

	int index = 0;

	/* prepare the mandatory part of the Postgres URI */
	appendPQExpBufferStr(uri, "postgres://");

	if (uriParams->username)
	{
		char *escaped = NULL;

		if (!escapeWithPercentEncoding(uriParams->username, &escaped))
		{
			log_error("Failed to percent-escape URI username \"%s\"",
					  uriParams->username);
			return false;
		}
		appendPQExpBuffer(uri, "%s@", escaped);
		free(escaped);
	}

	if (uriParams->hostname)
	{
		char *escaped = NULL;

		if (!escapeWithPercentEncoding(uriParams->hostname, &escaped))
		{
			log_error("Failed to percent-escape URI hostname \"%s\"",
					  uriParams->hostname);
			return false;
		}
		appendPQExpBuffer(uri, "%s", escaped);
		free(escaped);
	}

	if (uriParams->port)
	{
		appendPQExpBuffer(uri, ":%s", uriParams->port);
	}

	appendPQExpBufferStr(uri, "/");

	if (uriParams->dbname)
	{
		char *escaped = NULL;

		if (!escapeWithPercentEncoding(uriParams->dbname, &escaped))
		{
			log_error("Failed to percent-escape URI dbname \"%s\"",
					  uriParams->dbname);
			destroyPQExpBuffer(uri);
			return false;
		}
		appendPQExpBuffer(uri, "%s", escaped);
		free(escaped);
	}

	/* now add optional parameters to the Postgres URI */
	for (index = 0; index < uriParams->parameters.count; index++)
	{
		char *keyword = uriParams->parameters.keywords[index];
		char *value = uriParams->parameters.values[index];

		if (value != NULL && !streq(value, ""))
		{
			char *escapedValue = NULL;

			if (!escapeWithPercentEncoding(value, &escapedValue))
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

			appendPQExpBuffer(uri, "%s%s=%s",
							  index == 0 ? "?" : "&",
							  keyword,
							  escapedValue);

			free(escapedValue);
		}
		else
		{
			log_warn("buildPostgresURIfromPieces: %s is NULL", keyword);
		}
	}

	if (PQExpBufferBroken(uri))
	{
		log_error("Failed to build Postgres URI: out of memory");
		destroyPQExpBuffer(uri);
		return false;
	}

	*pguri = strdup(uri->data);
	destroyPQExpBuffer(uri);

	return true;
}


/*
 * charNeedsPercentEncoding returns true when a character needs special
 * encoding for percent-encoding. See escapeWithPercentEncoding() for
 * references.
 */
static inline bool
charNeedsPercentEncoding(char c)
{
	return !(isalpha(c) ||
			 isdigit(c) ||
			 c == '-' ||
			 c == '.' ||
			 c == '_' ||
			 c == '~');
}


/*
 * computePercentEncodedSize returns how many bytes are necessary to hold a
 * percent-encoded version of the given string.
 */
static inline size_t
computePercentEncodedSize(const char *str)
{
	/* prepare room for the terminal char '\0' */
	size_t size = 1;

	for (int i = 0; str[i] != '\0'; i++)
	{
		if (charNeedsPercentEncoding(str[i]))
		{
			size += 3;
		}
		else
		{
			++size;
		}
	}

	return size;
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
escapeWithPercentEncoding(const char *str, char **dst)
{
	const char *hex = "0123456789abcdef";

	if (str == NULL)
	{
		log_error("BUG: escapeWithPercentEncoding called with str == NULL");
		return false;
	}

	if (dst == NULL)
	{
		log_error("BUG: escapeWithPercentEncoding called with dst == NULL");
		return false;
	}

	size_t size = computePercentEncodedSize(str);
	char *escaped = (char *) calloc(size, sizeof(char));

	if (escaped == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	int pos = 0;

	for (int i = 0; str[i] != '\0'; i++)
	{
		/*
		 * 2.3 Unreserved Characters
		 *
		 * https://datatracker.ietf.org/doc/html/rfc3986#section-2.3
		 *
		 *       unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
		 */
		if (!charNeedsPercentEncoding(str[i]))
		{
			if (size <= (pos + 1))
			{
				/* we really do not expect that to ever happen */
				log_error("BUG: percent-encoded Postgres URI does not fit "
						  "in the computed size: %lld bytes",
						  (long long) size);
				return false;
			}

			escaped[pos++] = str[i];
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
			if (size <= (pos + 3))
			{
				/* we really do not expect that to ever happen */
				log_error("BUG: percent-encoded Postgres URI does not fit "
						  "in the computed size: %lld bytes",
						  (long long) size);
				return false;
			}

			escaped[pos++] = '%';
			escaped[pos++] = hex[str[i] >> 4];
			escaped[pos++] = hex[str[i] & 15];
		}
	}

	*dst = escaped;

	return true;
}


/*
 * uri_contains_password takes a Postgres connection string and checks to see
 * if it contains a parameter called password. Returns true if a password
 * keyword is present in the connection string.
 */
static bool
uri_grab_password(const char *pguri, SafeURI *safeURI)
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
		if (streq(option->keyword, "password") &&
			option->val != NULL &&
			!IS_EMPTY_STRING_BUFFER(option->val))
		{
			safeURI->password = strdup(option->val);

			if (safeURI->password == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			/* found the password field, break out of the loop */
			break;
		}
	}

	PQconninfoFree(conninfo);
	return true;
}


/*
 * parse_and_scrub_connection_string takes a Postgres connection string and
 * populates scrubbedPguri with the password replaced with **** for logging.
 * The scrubbedPguri parameter should point to a memory area that has been
 * allocated by the caller and has at least MAXCONNINFO bytes.
 */
bool
parse_and_scrub_connection_string(const char *pguri, SafeURI *safeURI)
{
	URIParams *uriParams = &(safeURI->uriParams);

	KeyVal overrides = {
		.count = 1,
		.keywords = { "password" },
		.values = { "" }
	};

	if (pguri == NULL)
	{
		safeURI->pguri = NULL;
		return true;
	}

	if (!uri_grab_password(pguri, safeURI))
	{
		/* errors have already been logged */
		return false;
	}

	bool checkForCompleteURI = false;

	if (!parse_pguri_info_key_vals(pguri,
								   &connStringDefaults,
								   &overrides,
								   uriParams,
								   checkForCompleteURI))
	{
		return false;
	}

	/* build the safe connection string with the overriden password */
	buildPostgresURIfromPieces(uriParams, &(safeURI->pguri));

	return true;
}


/*
 * freeSafeURI frees the dynamic memory allocated for handling the safe URI.
 */
void
freeSafeURI(SafeURI *safeURI)
{
	free(safeURI->pguri);
	free(safeURI->password);
	freeURIParams(&(safeURI->uriParams));

	safeURI->pguri = NULL;
	safeURI->password = NULL;
}


/*
 * freeURIParams frees the dynamic memory allocated for handling URI params.
 */
void
freeURIParams(URIParams *params)
{
	free(params->username);
	free(params->hostname);
	free(params->port);
	free(params->dbname);
	freeKeyVal(&(params->parameters));

	params->username = NULL;
	params->hostname = NULL;
	params->port = NULL;
	params->dbname = NULL;
}


/*
 * freeKeyVal frees the dynamic memory allocated for handling KeyVal parameters
 */
void
freeKeyVal(KeyVal *parameters)
{
	for (int i = 0; i < parameters->count; i++)
	{
		free(parameters->keywords[i]);
		free(parameters->values[i]);

		parameters->keywords[i] = NULL;
		parameters->values[i] = NULL;
	}

	parameters->count = 0;
}
