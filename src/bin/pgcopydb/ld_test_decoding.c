/*
 * src/bin/pgcopydb/ld_wal2json.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

#include "parson.h"

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "env_utils.h"
#include "ld_stream.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


typedef struct TestDecodingHeader
{
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
	StreamAction action;
	int offset;                 /* end of metadata section */
	int pos;
	bool eom;              /* set to true when parser reaches end-of-message */
} TestDecodingHeader;


typedef struct TestDecodingColumns
{
	char *colnameStart;
	int colnameLen;
	char *valueStart;
	int valueLen;

	struct TestDecodingColumns *next;
} TestDecodingColumns;


static bool parseTestDecodingMessageHeader(TestDecodingHeader *header,
										   const char *message);

static bool SetColumnNamesAndValues(LogicalMessageTuple *tuple,
									TestDecodingHeader *header,
									const char *message);

static bool parseNextColumn(TestDecodingColumns *cols,
							TestDecodingHeader *header,
							const char *message);

static bool listToTuple(LogicalMessageTuple *tuple,
						TestDecodingColumns *cols,
						int count);

/*
 * prepareWal2jsonMessage prepares our internal JSON entry from a test_decoding
 * message. At this stage we only escape the message as a proper JSON string.
 */
bool
prepareTestDecodingMessage(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/* jsonify the message as-is */
	JSON_Value *js = json_value_init_object();
	JSON_Object *jsobj = json_value_get_object(js);

	json_object_set_string(jsobj, "message", context->buffer);

	char *jsonstr =
		json_serialize_to_string(
			json_object_get_value(jsobj, "message"));

	privateContext->metadata.jsonBuffer = strdup(jsonstr);

	if (privateContext->metadata.jsonBuffer == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	json_value_free(js);
	json_free_serialized_string(jsonstr);

	return true;
}


/*
 * parseTestDecodingMessageActionAndXid retrieves the XID from the logical
 * replication message found in the buffer as received from the test_decoding
 * output plugin.
 *
 * Not all messages are supposed to have the XID information.
 *
 *  INPUT: test_decoding raw message
 * OUTPUT: pgcopydb LogicalMessageMetadata structure
 */
bool
parseTestDecodingMessageActionAndXid(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	char *begin = strstr(context->buffer, "BEGIN ");
	char *commit = strstr(context->buffer, "COMMIT ");
	char *table = strstr(context->buffer, "table ");

	if (context->buffer == begin)
	{
		metadata->action = STREAM_ACTION_BEGIN;

		int s = strlen("BEGIN ");

		if (!stringToUInt32(begin + s, &(metadata->xid)))
		{
			log_error("Failed to parse XID \"%s\"", begin + s);
			return false;
		}
	}
	else if (context->buffer == commit)
	{
		metadata->action = STREAM_ACTION_COMMIT;

		int s = strlen("COMMIT ");

		if (!stringToUInt32(commit + s, &(metadata->xid)))
		{
			log_error("Failed to parse XID \"%s\"", commit + s);
			return false;
		}
	}
	else if (context->buffer == table)
	{
		TestDecodingHeader header = { 0 };

		if (!parseTestDecodingMessageHeader(&header, context->buffer))
		{
			/* errors have already been logged */
			return false;
		}

		if (strcmp(header.nspname, "pgcopydb") == 0)
		{
			log_debug("Filtering out message for schema \"%s\": %s",
					  header.nspname,
					  context->buffer);
			metadata->filterOut = true;
		}

		metadata->action = header.action;
	}
	else
	{
		log_error("Failed to parse test_decoding message: %s", context->buffer);
		return false;
	}

	return true;
}


#define TD_OLD_KEY "old-key: "
#define TD_OLD_KEY_LEN strlen(TD_OLD_KEY)
#define TD_FOUND_OLD_KEY(ptr) (strncmp(ptr, TD_OLD_KEY, TD_OLD_KEY_LEN) == 0)

#define TD_NEW_TUPLE "new-tuple: "
#define TD_NEW_TUPLE_LEN strlen(TD_NEW_TUPLE)
#define TD_FOUND_NEW_TUPLE(ptr) (strncmp(ptr, TD_NEW_TUPLE, TD_NEW_TUPLE_LEN) == 0)


/*
 * parseTestDecodingMessage parses a message as emitted by test_decoding into
 * our own internal representation, that can be later output as SQL text.
 *
 * The test_decoding message is found in the "message" key of the given JSON
 * object, and the metadata parts of the message have been parsed previously
 * and are available in the pgcopydb JSON keys (action, xid, lsn, timestamp).
 *
 * In this function (parseTestDecodingMessage) we parse the message part.
 *
 *  INPUT: pgcopydb's own JSON format (action, xid, lsn, timestamp, message)
 * OUTPUT: pgcopydb LogicalTransactionStatement structure
 */
bool
parseTestDecodingMessage(LogicalTransactionStatement *stmt,
						 LogicalMessageMetadata *metadata,
						 char *message,
						 JSON_Value *json)
{
	JSON_Object *jsobj = json_value_get_object(json);
	TestDecodingHeader header = { 0 };

	/* extract the test_decoding raw message */
	const char *td_message = json_object_get_string(jsobj, "message");

	if (!parseTestDecodingMessageHeader(&header, td_message))
	{
		/* errors have already been logged */
		return false;
	}

	switch (metadata->action)
	{
		case STREAM_ACTION_BEGIN:
		case STREAM_ACTION_COMMIT:
		case STREAM_ACTION_SWITCH:
		case STREAM_ACTION_KEEPALIVE:
		{
			log_error("BUG: parseTestDecodingMessage received action %c",
					  metadata->action);
			return false;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			strlcpy(stmt->stmt.truncate.nspname, header.nspname, NAMEDATALEN);
			strlcpy(stmt->stmt.truncate.relname, header.relname, NAMEDATALEN);

			break;
		}

		case STREAM_ACTION_INSERT:
		{
			strlcpy(stmt->stmt.insert.nspname, header.nspname, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, header.relname, NAMEDATALEN);

			stmt->stmt.insert.new.count = 1;
			stmt->stmt.insert.new.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.insert.new.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			header.pos = header.offset;

			LogicalMessageTuple *tuple = &(stmt->stmt.insert.new.array[0]);

			if (!SetColumnNamesAndValues(tuple, &header, td_message))
			{
				log_error("Failed to parse INSERT columns for logical "
						  "message %s",
						  message);
				return false;
			}

			break;
		}

		case STREAM_ACTION_UPDATE:
		{
			strlcpy(stmt->stmt.update.nspname, header.nspname, NAMEDATALEN);
			strlcpy(stmt->stmt.update.relname, header.relname, NAMEDATALEN);

			stmt->stmt.update.old.count = 1;
			stmt->stmt.update.new.count = 1;

			stmt->stmt.update.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			stmt->stmt.update.new.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.update.old.array == NULL ||
				stmt->stmt.update.new.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			/*
			 * test_decoding UPDATE message starts with old-key: entries.
			 */
			if (!TD_FOUND_OLD_KEY(td_message + header.offset))
			{
				log_error("Failed to find old-key in UPDATE message: %s",
						  td_message);
				return false;
			}

			header.pos = header.offset + TD_OLD_KEY_LEN;

			LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);

			if (!SetColumnNamesAndValues(old, &header, td_message))
			{
				log_error("Failed to parse UPDATE old-key columns for logical "
						  "message %s",
						  td_message);
				return false;
			}

			/*
			 * test_decoding UPDATE message then has "new-tuple: " entries.
			 */
			if (!TD_FOUND_NEW_TUPLE(td_message + header.pos))
			{
				log_error("Failed to find new-tuple in UPDATE message: %s",
						  td_message);
				return false;
			}

			header.pos = header.pos + TD_NEW_TUPLE_LEN;

			LogicalMessageTuple *new = &(stmt->stmt.update.new.array[0]);

			if (!SetColumnNamesAndValues(new, &header, td_message))
			{
				log_error("Failed to parse UPDATE new-tuple columns for logical "
						  "message %s",
						  td_message);
				return false;
			}

			break;
		}

		case STREAM_ACTION_DELETE:
		{
			strlcpy(stmt->stmt.delete.nspname, header.nspname, NAMEDATALEN);
			strlcpy(stmt->stmt.delete.relname, header.relname, NAMEDATALEN);

			stmt->stmt.delete.old.count = 1;
			stmt->stmt.delete.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.update.old.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			header.pos = header.offset;

			LogicalMessageTuple *tuple = &(stmt->stmt.delete.old.array[0]);

			if (!SetColumnNamesAndValues(tuple, &header, td_message))
			{
				log_error("Failed to parse DELETE columns for logical "
						  "message %s",
						  td_message);
				return false;
			}

			break;
		}

		default:
		{
			log_error("Unknown message action %d", metadata->action);
			return false;
		}
	}

	return true;
}


/*
 * parseTestDecodingMessageHeader parses a raw test_decoding message to find
 * the header information only. It stops after having parsed the target table
 * qualified name and the action type (INSERT/UPDATE/DELETE/TRUNCATE), and
 * registers the offset when the rest of the message starts.
 */
static bool
parseTestDecodingMessageHeader(TestDecodingHeader *header, const char *message)
{
	/*
	 * Parse the test_decoding message "header" only at the moment:
	 *
	 * table public.payment_p2022_07: UPDATE:
	 *       ^     ^                ^ ^     ^
	 *      idp   dot             sep acp   end
	 */
	char *idp = (char *) message + strlen("table ");
	char *dot = strchr(idp, '.');
	char *sep = strchr(idp, ':');
	char *acp = sep + 2;    /* skip ": " */
	char *end = strchr(acp, ':');

	/* skip the last ":" of the header in the offset */
	header->offset = (end - message + 1) + 1;

	bool quoted = *idp == '"';

	if (quoted)
	{
		char ident[BUFSIZE] = { 0 };
		strlcpy(ident, idp, sep - idp);

		log_error("Failed to parse quoted qualified identifer %s", ident);
		return false;
	}

	/* grab the table schema.name */
	strlcpy(header->nspname, idp, dot - idp + 1);
	strlcpy(header->relname, dot + 1, sep - dot);

	/* now grab the action */
	char action[BUFSIZE] = { 0 };
	strlcpy(action, acp, end - acp + 1);

	if (strcmp(action, "INSERT") == 0)
	{
		header->action = STREAM_ACTION_INSERT;
	}
	else if (strcmp(action, "UPDATE") == 0)
	{
		header->action = STREAM_ACTION_UPDATE;
	}
	else if (strcmp(action, "DELETE") == 0)
	{
		header->action = STREAM_ACTION_DELETE;
	}
	else if (strcmp(action, "TRUNCATE") == 0)
	{
		header->action = STREAM_ACTION_TRUNCATE;
	}
	else
	{
		log_error("Failed to parse unknown test_decoding "
				  "message action \"%s\" in: %s",
				  action,
				  message);
		return false;
	}

	return true;
}


/*
 * SetColumnNames parses the "columns" (or "identity") JSON object from a
 * wal2json logical replication message and fills-in our internal
 * representation for a tuple.
 */
static bool
SetColumnNamesAndValues(LogicalMessageTuple *tuple,
						TestDecodingHeader *header,
						const char *message)
{
	log_trace("SetColumnNamesAndValues: %c %s",
			  header->action,
			  message + header->pos);

	TestDecodingColumns *cols =
		(TestDecodingColumns *) calloc(1, sizeof(TestDecodingColumns));

	if (cols == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	cols->next = NULL;

	TestDecodingColumns *cur = cols;
	int count = 0;

	while (!header->eom)
	{
		if (!parseNextColumn(cur, header, message))
		{
			/* errors have already been logged */
			return false;
		}

		/* when we find "new-tuple: " */
		if (!header->eom && cur->colnameStart == NULL)
		{
			break;
		}

		++count;

		/* that might have been the last column */
		if (header->eom)
		{
			break;
		}

		/* if that was not the last column, prepare the next one */
		TestDecodingColumns *next =
			(TestDecodingColumns *) calloc(1, sizeof(TestDecodingColumns));

		if (next == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		next->next = NULL;
		cur->next = next;
		cur = next;
	}

	/*
	 * Transform the internal TestDecodingColumns linked-list into our internal
	 * representation for DML tuples, which is output plugin independant.
	 */
	if (!listToTuple(tuple, cols, count))
	{
		log_error("Failed to convert test_decoding column to tuple");
		return false;
	}

	/*
	 * Free the TestDecodingColumns memory that we allocated: only the
	 * structure itself, the rest is just a bunch of pointers to parts of the
	 * messages.
	 */
	TestDecodingColumns *c = cols;

	for (; c != NULL;)
	{
		TestDecodingColumns *next = c->next;

		free(c);
		c = next;
	}

	return true;
}


/*
 * parseNextColumn parses the next test_decoding column value from the raw
 * message. The parsing starts at the current header->pos offset, and updates
 * the header->pos to the end of the section parsed.
 *
 *  payment_id[integer]:23757
 *  payment_date[timestamp with time zone]:'2022-02-11 03:52:25.634006+00'
 */
static bool
parseNextColumn(TestDecodingColumns *cols,
				TestDecodingHeader *header,
				const char *message)
{
	char *ptr = (char *) (message + header->pos);

	if (ptr == NULL || *ptr == '\0')
	{
		header->eom = true;
		return true;
	}

	/* we need to be careful and not parse "new-tuple: " as a column name */
	if (header->action == STREAM_ACTION_UPDATE && TD_FOUND_NEW_TUPLE(ptr))
	{
		/* return true with colnameStart still set to NULL */
		return true;
	}

	/* search for data type name separators (open/close, or A/B) */
	char *typA = strchr(ptr, '[');
	char *typB = typA != NULL ? strchr(typA, ']') : NULL;

	if (typA == NULL || typB == NULL)
	{
		log_error("Failed to parse test_decoding column name and "
				  "type at offset %d in message:", header->pos);

		log_error("%s", message);
		log_error("%*s", header->pos, "^");

		return false;
	}

	cols->colnameStart = ptr;
	cols->colnameLen = typA - ptr;

	log_trace("parseNextColumn: %.*s", cols->colnameLen, cols->colnameStart);

	/* skip the typename and the closing ] and the following : */
	ptr = typB + 1 + 1;
	header->pos = ptr - message;

	/*
	 * Parse standard-conforming string.
	 */
	if (*ptr == '\'')
	{
		/* skip the opening single-quote now */
		char *cur = ptr + 1;

		for (; *cur != '\0'; cur++)
		{
			char *nxt = cur + 1;

			if (*cur == '\'' && *nxt == '\'')
			{
				++cur;
			}
			else if (*cur == '\'')
			{
				break;
			}
		}

		if (*cur == '\0')
		{
			log_error("Failed to parse quoted value "
					  "for column \"%.*s\" in message: %s",
					  cols->colnameLen,
					  cols->colnameStart,
					  message);
			return false;
		}

		/* now skip closing single quote */
		++cur;

		cols->valueStart = ptr;
		cols->valueLen = cur - ptr + 1;

		/* advance the ptr to past the value, skip the next space */
		ptr = cur;
		header->pos = ptr - message + 1;

		log_trace("parseNextColumn: quoted value: %.*s %s",
				  cols->valueLen,
				  cols->valueStart,
				  *ptr == '\0' ? "(eom)" : "");
	}

	/*
	 * Parse BITOID or VARBITOID string literals
	 */
	else if (*ptr == 'B')
	{
		/* skip B and ' */
		char *start = ptr + 2;
		char *end = strchr(start, '\'');

		if (end == NULL)
		{
			log_error("Failed to parse bit string literal: %s", ptr);
			return false;
		}

		cols->valueStart = start;
		cols->valueLen = end - start + 1;

		/* advance to past the value, skip the next space */
		ptr = end + 1;
		header->pos = ptr - message;

		log_trace("parseNextColumn: bit string value: %.*s",
				  cols->valueLen,
				  cols->valueStart);
	}
	else
	{
		cols->valueStart = ptr;

		/*
		 * All columns (but the last one) are separated by a space character.
		 */
		char *spc = strchr(ptr, ' ');

		if (spc != NULL)
		{
			header->pos = spc - message + 1;
			cols->valueLen = spc - ptr + 1;
		}
		else
		{
			/* last column */
			header->eom = true;

			header->pos = strlen(message) - 1;
			cols->valueLen = strlen(cols->valueStart);
		}

		/* advance to past the value, skip the next space */
		ptr = (char *) (message + header->pos + 1);

		log_trace("parseNextColumn: raw value: %.*s",
				  cols->valueLen,
				  cols->valueStart);
	}

	if (*ptr == '\0')
	{
		header->eom = true;
	}

	return true;
}


/*
 * listToTuple transforms the test_decoding linked-list output from the parser
 * into our internal data structure for a tuple.
 */
static bool
listToTuple(LogicalMessageTuple *tuple, TestDecodingColumns *cols, int count)
{
	tuple->cols = count;
	tuple->columns = (char **) calloc(count, sizeof(char *));

	if (tuple->columns == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Allocate the tuple values, an array of VALUES, as in SQL.
	 *
	 * TODO: actually support multi-values clauses (single column names array,
	 * multiple VALUES matching the same metadata definition). At the moment
	 * it's always a single VALUES entry: VALUES(a, b, c).
	 *
	 * The goal is to be able to represent VALUES(a1, b1, c1), (a2, b2, c2).
	 */
	LogicalMessageValuesArray *valuesArray = &(tuple->values);

	valuesArray->count = 1;
	valuesArray->array =
		(LogicalMessageValues *) calloc(1, sizeof(LogicalMessageValues));

	if (valuesArray->array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* allocate one VALUES entry */
	LogicalMessageValues *values = &(tuple->values.array[0]);
	values->cols = count;
	values->array =
		(LogicalMessageValue *) calloc(count, sizeof(LogicalMessageValue));

	if (values->array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Now that our memory areas are allocated and initialized to zeroes, fill
	 * them in with the values from the JSON message.
	 */
	int i = 0;
	TestDecodingColumns *cur = cols;

	for (; i < count && cur != NULL; cur = cur->next, i++)
	{
		LogicalMessageValue *valueColumn = &(values->array[i]);

		tuple->columns[i] = strndup(cur->colnameStart, cur->colnameLen);
		valueColumn->oid = TEXTOID;

		if (cur->valueStart == NULL)
		{
			log_error("BUG: listToTuple current value is NULL for \"%s\"",
					  tuple->columns[i]);
			return false;
		}

		valueColumn->val.str = strndup(cur->valueStart, cur->valueLen);
		valueColumn->isQuoted = true;

		if (valueColumn->val.str == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		/* strlen("null") == 4 */
		if (strncmp(cur->valueStart, "null", 4) == 0)
		{
			valueColumn->isNull = true;
		}
	}

	return true;
}
