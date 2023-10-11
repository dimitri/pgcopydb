/*-------------------------------------------------------------------------
 *
 * Utility routines for SQL dumping
 *
 * Basically this is stuff that is useful in both pg_dump and pg_dumpall.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dumputils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <ctype.h>

#include "dumputils.h"
#include "string_utils.h"

/*
 * Detect whether the given GUC variable is of GUC_LIST_QUOTE type.
 *
 * It'd be better if we could inquire this directly from the backend; but even
 * if there were a function for that, it could only tell us about variables
 * currently known to guc.c, so that it'd be unsafe for extensions to declare
 * GUC_LIST_QUOTE variables anyway.  Lacking a solution for that, it doesn't
 * seem worth the work to do more than have this list, which must be kept in
 * sync with the variables actually marked GUC_LIST_QUOTE in guc_tables.c.
 */
bool
variable_is_guc_list_quote(const char *name)
{
	if (pg_strcasecmp(name, "local_preload_libraries") == 0 ||
		pg_strcasecmp(name, "search_path") == 0 ||
		pg_strcasecmp(name, "session_preload_libraries") == 0 ||
		pg_strcasecmp(name, "shared_preload_libraries") == 0 ||
		pg_strcasecmp(name, "temp_tablespaces") == 0 ||
		pg_strcasecmp(name, "unix_socket_directories") == 0)
		return true;
	else
		return false;
}

/*
 * SplitGUCList --- parse a string containing identifiers or file names
 *
 * This is used to split the value of a GUC_LIST_QUOTE GUC variable, without
 * presuming whether the elements will be taken as identifiers or file names.
 * See comparable code in src/backend/utils/adt/varlena.c.
 *
 * Inputs:
 *	rawstring: the input string; must be overwritable!	On return, it's
 *			   been modified to contain the separated identifiers.
 *	separator: the separator punctuation expected between identifiers
 *			   (typically '.' or ',').  Whitespace may also appear around
 *			   identifiers.
 * Outputs:
 *	namelist: receives a malloc'd, null-terminated array of pointers to
 *			  identifiers within rawstring.  Caller should free this
 *			  even on error return.
 *
 * Returns true if okay, false if there is a syntax error in the string.
 */
bool
SplitGUCList(char *rawstring, char separator,
			 char ***namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;
	char	  **nextptr;

	/*
	 * Since we disallow empty identifiers, this is a conservative
	 * overestimate of the number of pointers we could need.  Allow one for
	 * list terminator.
	 */
	*namelist = nextptr = (char **)
		pg_malloc((strlen(rawstring) / 2 + 2) * sizeof(char *));
	*nextptr = NULL;

	while (isspace((unsigned char) *nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !isspace((unsigned char) *nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */
		}

		while (isspace((unsigned char) *nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (isspace((unsigned char) *nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/*
		 * Finished isolating current name --- add it to output array
		 */
		*nextptr++ = curname;

		/* Loop back if we didn't reach end of string */
	} while (!done);

	*nextptr = NULL;
	return true;
}

/*
 * Helper function for dumping "ALTER DATABASE/ROLE SET ..." commands.
 *
 * Parse the contents of configitem (a "name=value" string), wrap it in
 * a complete ALTER command, and append it to buf.
 *
 * type is DATABASE or ROLE, and name is the name of the database or role.
 * If we need an "IN" clause, type2 and name2 similarly define what to put
 * there; otherwise they should be NULL.
 * conn is used only to determine string-literal quoting conventions.
 */
void
makeAlterConfigCommand(PGconn *conn, const char *configitem,
					   const char *type, const char *name,
					   const char *type2, const char *name2,
					   PQExpBuffer buf)
{
	char	   *mine;
	char	   *pos;

	/* Parse the configitem.  If we can't find an "=", silently do nothing. */
	mine = pg_strdup(configitem);
	pos = strchr(mine, '=');
	if (pos == NULL)
	{
		pg_free(mine);
		return;
	}
	*pos++ = '\0';

	/* Build the command, with suitable quoting for everything. */
	appendPQExpBuffer(buf, "ALTER %s \"%s\" ", type, name);
	if (type2 != NULL && name2 != NULL)
		appendPQExpBuffer(buf, "IN %s \"%s\" ", type2, name2);
	appendPQExpBuffer(buf, "SET \"%s\" TO ", mine);

	/*
	 * Variables that are marked GUC_LIST_QUOTE were already fully quoted by
	 * flatten_set_variable_args() before they were put into the setconfig
	 * array.  However, because the quoting rules used there aren't exactly
	 * like SQL's, we have to break the list value apart and then quote the
	 * elements as string literals.  (The elements may be double-quoted as-is,
	 * but we can't just feed them to the SQL parser; it would do the wrong
	 * thing with elements that are zero-length or longer than NAMEDATALEN.)
	 *
	 * Variables that are not so marked should just be emitted as simple
	 * string literals.  If the variable is not known to
	 * variable_is_guc_list_quote(), we'll do that; this makes it unsafe to
	 * use GUC_LIST_QUOTE for extension variables.
	 */
	if (variable_is_guc_list_quote(mine))
	{
		char	  **namelist;
		char	  **nameptr;

		/* Parse string into list of identifiers */
		/* this shouldn't fail really */
		if (SplitGUCList(pos, ',', &namelist))
		{
			for (nameptr = namelist; *nameptr; nameptr++)
			{
				if (nameptr != namelist)
					appendPQExpBufferStr(buf, ", ");
				appendStringLiteralConn(buf, *nameptr, conn);
			}
		}
		pg_free(namelist);
	}
	else
		appendStringLiteralConn(buf, pos, conn);

	appendPQExpBufferStr(buf, ";\n");

	pg_free(mine);
}
