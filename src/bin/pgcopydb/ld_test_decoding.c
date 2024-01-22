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

#include "catalog.h"
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
	const char *message;
	char qname[PG_NAMEDATALEN_FQ];
	LogicalMessageRelation table;
	StreamAction action;
	int offset;                 /* end of metadata section */
	int pos;
	bool eom;              /* set to true when parser reaches end-of-message */
} TestDecodingHeader;


typedef struct TestDecodingColumns
{
	uint32_t oid;
	char *colnameStart;
	int colnameLen;
	char *valueStart;
	int valueLen;

	struct TestDecodingColumns *next;
} TestDecodingColumns;


static bool parseTestDecodingMessageHeader(TestDecodingHeader *header,
										   const char *message);

static bool parseTestDecodingInsertMessage(StreamContext *privateContext,
										   TestDecodingHeader *header);

static bool parseTestDecodingUpdateMessage(StreamContext *privateContext,
										   TestDecodingHeader *header);

static bool parseTestDecodingDeleteMessage(StreamContext *privateContext,
										   TestDecodingHeader *header);

static bool SetColumnNamesAndValues(LogicalMessageTuple *tuple,
									TestDecodingHeader *header);

static bool parseNextColumn(TestDecodingColumns *cols,
							TestDecodingHeader *header);

static bool listToTuple(LogicalMessageTuple *tuple,
						TestDecodingColumns *cols,
						int count);

static bool prepareUpdateTuppleArrays(StreamContext *privateContext,
									  TestDecodingHeader *header);


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

		if (strcmp(header.table.nspname, "pgcopydb") == 0)
		{
			log_debug("Filtering out message for schema \"%s\": %s",
					  header.table.nspname,
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
parseTestDecodingMessage(StreamContext *privateContext,
						 char *message,
						 JSON_Value *json)
{
	LogicalTransactionStatement *stmt = privateContext->stmt;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

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
			stmt->stmt.truncate.table = header.table;

			break;
		}

		case STREAM_ACTION_INSERT:
		{
			stmt->stmt.insert.table = header.table;
			if (!parseTestDecodingInsertMessage(privateContext, &header))
			{
				log_error("Failed to parse test_decoding INSERT message: %s",
						  header.message);
				return false;
			}

			break;
		}

		case STREAM_ACTION_UPDATE:
		{
			stmt->stmt.update.table = header.table;
			if (!parseTestDecodingUpdateMessage(privateContext, &header))
			{
				log_error("Failed to parse test_decoding UPDATE message: %s",
						  header.message);
				return false;
			}
			break;
		}

		case STREAM_ACTION_DELETE:
		{
			stmt->stmt.delete.table = header.table;
			if (!parseTestDecodingDeleteMessage(privateContext, &header))
			{
				log_error("Failed to parse test_decoding DELETE message: %s",
						  header.message);
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
	header->message = message;

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

	/*
	 * The table schema.name is already escaped by the plugin using PostgreSQL's
	 * internal quote_identifier function (see
	 * https://github.com/postgres/postgres/blob/8793c600/contrib/test_decoding/test_decoding.c#L627-L632).
	 * The result slightly differs from that of PQescapeIdentifier, as it does
	 * not add quotes around the schema.name when they are not necessary. Here
	 * are some possible outputs:
	 * - public.hello
	 * - "Public".hello
	 * - "sp $cial"."t ablE"
	 */
	header->table.nspname = strndup(idp, dot - idp);
	header->table.relname = strndup(dot + 1, sep - dot - 1);
	header->table.pqMemory = false;

	sformat(header->qname, sizeof(header->qname), "%s.%s",
			header->table.nspname,
			header->table.relname);

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
 * parseTestDecodingInsertMessage is called to parse an INSERT message from the
 * test_decoding logical decoding plugin.
 */
static bool
parseTestDecodingInsertMessage(StreamContext *privateContext,
							   TestDecodingHeader *header)
{
	LogicalTransactionStatement *stmt = privateContext->stmt;

	stmt->stmt.insert.new.count = 1;
	stmt->stmt.insert.new.array =
		(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

	if (stmt->stmt.insert.new.array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	header->pos = header->offset;

	LogicalMessageTuple *tuple = &(stmt->stmt.insert.new.array[0]);

	if (!SetColumnNamesAndValues(tuple, header))
	{
		log_error("Failed to parse INSERT columns for logical "
				  "message %s",
				  header->message);
		return false;
	}

	return true;
}


/*
 * parseTestDecodingUpdateMessage is called to parse an UPDATE message from the
 * test_decoding logical decoding plugin.
 */
static bool
parseTestDecodingUpdateMessage(StreamContext *privateContext,
							   TestDecodingHeader *header)
{
	LogicalTransactionStatement *stmt = privateContext->stmt;

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
	 * test_decoding UPDATE message may starts with old-key: entries.
	 */
	if (TD_FOUND_OLD_KEY(header->message + header->offset))
	{
		header->pos = header->offset + TD_OLD_KEY_LEN;

		LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);

		if (!SetColumnNamesAndValues(old, header))
		{
			log_error("Failed to parse UPDATE old-key columns for logical "
					  "message %s",
					  header->message);
			return false;
		}

		/*
		 * test_decoding UPDATE message then has "new-tuple: " entries.
		 */
		if (!TD_FOUND_NEW_TUPLE(header->message + header->pos))
		{
			log_error("Failed to find new-tuple in UPDATE message: %s",
					  header->message);
			return false;
		}

		header->pos = header->pos + TD_NEW_TUPLE_LEN;

		LogicalMessageTuple *new = &(stmt->stmt.update.new.array[0]);

		if (!SetColumnNamesAndValues(new, header))
		{
			log_error("Failed to parse UPDATE new-tuple columns for logical "
					  "message %s",
					  header->message);
			return false;
		}
	}
	else
	{
		/*
		 * Here we have an update message without old-key: entries.
		 *
		 * We have to look-up the table by nspname.relname in our internal
		 * catalogs, and then figure out which columns in the UPDATE message
		 * are a pkey column (WHERE clause) and which are not (SET clause).
		 */
		header->pos = header->offset;

		if (!prepareUpdateTuppleArrays(privateContext, header))
		{
			log_error("Failed to parse UPDATE new-tuple columns for logical "
					  "message %s",
					  header->message);
			return false;
		}

		return true;
	}

	return true;
}


/*
 * parseTestDecodingDeleteMessage is called to parse an DELETE message from the
 * test_decoding logical decoding plugin.
 */
static bool
parseTestDecodingDeleteMessage(StreamContext *privateContext,
							   TestDecodingHeader *header)
{
	LogicalTransactionStatement *stmt = privateContext->stmt;

	stmt->stmt.delete.old.count = 1;
	stmt->stmt.delete.old.array =
		(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

	if (stmt->stmt.update.old.array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	header->pos = header->offset;

	LogicalMessageTuple *tuple = &(stmt->stmt.delete.old.array[0]);

	if (!SetColumnNamesAndValues(tuple, header))
	{
		log_error("Failed to parse DELETE columns for logical "
				  "message %s",
				  header->message);
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
SetColumnNamesAndValues(LogicalMessageTuple *tuple, TestDecodingHeader *header)
{
	log_trace("SetColumnNamesAndValues: %c %s",
			  header->action,
			  header->message + header->pos);

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
		if (!parseNextColumn(cur, header))
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
				TestDecodingHeader *header)
{
	char *ptr = (char *) (header->message + header->pos);

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

	/*
	 * Postgres array data types are spelled like: "text[]". In test_decoding
	 * we might then see data types like in the following example:
	 *
	 *   f2[text[]]:'{incididunt,ut,labore,et,dolore,magna}'
	 */
	if (typB != NULL && *typB != '\0' && *(typB - 1) == '[' && *(typB + 1) == ']')
	{
		/* skip [], go to the next one in "[text[]]" */
		++typB;
	}

	if (typA == NULL || typB == NULL)
	{
		log_error("Failed to parse test_decoding column name and "
				  "type at offset %d in message:", header->pos);

		log_error("%s", header->message);
		log_error("%*s", header->pos, "^");

		return false;
	}

	/*
	 * At the moment we specialize our processing only for text strings, which
	 * we receive single-quoted and following C-Style Escapes, but without the
	 * E prefix.
	 */
	char *typStart = typA + 1;
	int typLen = (int) ((typB - typA) - 1);
	char typname[PG_NAMEDATALEN] = { 0 };

	sformat(typname, sizeof(typname), "%.*s", typLen, typStart);

	if (streq(typname, "text"))
	{
		cols->oid = TEXTOID;
	}

	cols->colnameStart = ptr;
	cols->colnameLen = typA - ptr;

	log_trace("parseNextColumn[%s]: %.*s",
			  typname,
			  cols->colnameLen,
			  cols->colnameStart);

	/* skip the typename and the closing ] and the following : */
	ptr = typB + 1 + 1;
	header->pos = ptr - header->message;

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
					  header->message);
			return false;
		}

		/* now skip closing single quote */
		++cur;

		/* do not capture the quotes */
		cols->valueStart = ptr + 1;
		cols->valueLen = (cur - 1) - (ptr + 1);

		/* advance the ptr to past the value, skip the next space */
		ptr = cur;
		header->pos = ptr - header->message + 1;

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

		/* do not capture the quotes */
		cols->valueStart = start;
		cols->valueLen = end - start;

		/* advance to past the value, skip the next space */
		ptr = end + 1;
		header->pos = ptr - header->message;

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
			header->pos = spc - header->message + 1;
			cols->valueLen = spc - ptr;
		}
		else
		{
			/* last column */
			header->eom = true;

			header->pos = strlen(header->message) - 1;
			cols->valueLen = strlen(cols->valueStart);
		}

		/* advance to past the value, skip the next space */
		ptr = (char *) (header->message + header->pos + 1);

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
	if (!AllocateLogicalMessageTuple(tuple, count))
	{
		/* errors have already been logged */
		return false;
	}

	LogicalMessageValues *values = &(tuple->values.array[0]);

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

		/* strlen("null") == 4 */
		if (strncmp(cur->valueStart, "null", 4) == 0)
		{
			valueColumn->isNull = true;
		}
		else if (cur->oid == TEXTOID)
		{
			/*
			 * Internally store the string non-quoted, so that the ld_transform
			 * module has a chance of preparing the quoted string with C-Style
			 * escapes correctly.
			 *
			 * The test-decoding module escapes the single-quotes the standard
			 * way by doubling them. Unescape the single-quotes here.
			 */
			valueColumn->isQuoted = false;

			int len = cur->valueLen;
			valueColumn->val.str = (char *) calloc(len + 1, sizeof(char));

			if (valueColumn->val.str == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			/* copy the string contents without the surrounding quotes */
			for (int pidx = 0, vidx = 0; pidx < cur->valueLen; pidx++)
			{
				char *ptr = cur->valueStart + pidx;
				char *nxt = cur->valueStart + pidx + 1;

				/* unescape the single-quotes */
				if (*ptr == '\'' && *nxt == '\'')
				{
					continue;
				}

				valueColumn->val.str[vidx++] = *ptr;
			}
		}
		else
		{
			valueColumn->val.str = strndup(cur->valueStart, cur->valueLen);
			valueColumn->isQuoted = true;

			if (valueColumn->val.str == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}
		}
	}

	return true;
}


/*
 * prepareUpdateTuppleArrays prepares an UPDATE message Tuple Arrays when we
 * parse an UPDATE message that does not have old-key: and new-key: elements.
 * We then need to look-up our catalogs to see which columns are part of the
 * identity (WHERE clause) and which columns should be in the SET clause.
 */
static bool
prepareUpdateTuppleArrays(StreamContext *privateContext,
						  TestDecodingHeader *header)
{
	LogicalTransactionStatement *stmt = privateContext->stmt;

	/*
	 * First parse all the columns of the UPDATE message in a single
	 * LogicalMessageTuple. Then we can lookup for column attributes.
	 */
	LogicalMessageTuple *cols =
		(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

	if (!SetColumnNamesAndValues(cols, header))
	{
		log_error("Failed to parse UPDATE columns for logical message %s",
				  header->message);
		return false;
	}

	/*
	 * Now lookup our internal catalogs to find out for every column if it is
	 * part of the pkey definition (WHERE clause) or not (SET clause).
	 */
	DatabaseCatalog *sourceDB = privateContext->sourceDB;

	SourceTable *table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_table_by_name(sourceDB,
										header->table.nspname,
										header->table.relname,
										table))
	{
		/* errors have already been logged */
		return false;
	}

	if (table->oid == 0)
	{
		log_error("Failed to parse decoding message for UPDATE on "
				  "table %s which is not in our catalogs",
				  table->qname);
		return false;
	}

	/* FIXME: lookup for the attribute in the SQLite database directly */
	if (!catalog_s_table_fetch_attrs(sourceDB, table))
	{
		log_error("Failed to fetch table %s attribute list, "
				  "see above for details",
				  table->qname);
		return false;
	}

	int columnCount = cols->values.array[0].cols;
	bool *pkeyArray = NULL;

	int oldCount = 0;
	int newCount = 0;

	if (0 < columnCount)
	{
		pkeyArray = (bool *) calloc(columnCount, sizeof(bool));

		if (pkeyArray == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		for (int c = 0; c < columnCount; c++)
		{
			const char *colname = cols->columns[c];

			SourceTableAttribute attribute = { 0 };

			if (!catalog_lookup_s_attr_by_name(sourceDB,
											   table->oid,
											   colname,
											   &attribute))
			{
				log_error("Failed to lookup for table %s attribute %s in our "
						  "internal catalogs, see above for details",
						  table->qname,
						  colname);
				return false;
			}

			if (attribute.attnum > 0)
			{
				pkeyArray[c] = attribute.attisprimary;
			}

			if (pkeyArray[c])
			{
				++oldCount;
			}
			else
			{
				++newCount;
			}
		}
	}

	if (oldCount == 0)
	{
		log_error("Failed to parse decoding message for UPDATE on "
				  "table %s: WHERE clause columns not found",
				  table->qname);
		return false;
	}

	if (newCount == 0)
	{
		log_error("Failed to parse decoding message for UPDATE on "
				  "table %s: SET clause columns not found",
				  table->qname);
		return false;
	}

	/*
	 * Now that we know for each key if it's a pkey (identity, WHERE
	 * clause, old-key) or a new value (columns, SET clause), dispatch the
	 * columns accordingly.
	 */
	LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);
	LogicalMessageTuple *new = &(stmt->stmt.update.new.array[0]);

	if (!AllocateLogicalMessageTuple(old, oldCount) ||
		!AllocateLogicalMessageTuple(new, newCount))
	{
		/* errors have already been logged */
		return false;
	}

	int oldPos = 0;
	int newPos = 0;

	for (int c = 0; c < columnCount; c++)
	{
		const char *colname = cols->columns[c];

		/* we lack multi-values support at the moment, so... */
		if (cols->values.count != 1)
		{
			log_error("BUG in prepareUpdateTuppleArrays: cols->values.count"
					  "is %d",
					  cols->values.count);
			return false;
		}

		if (pkeyArray[c])
		{
			old->columns[oldPos] = strdup(colname);
			old->values.array[0].array[oldPos] = cols->values.array[0].array[c];

			++oldPos;
		}
		else
		{
			new->columns[newPos] = strdup(colname);
			new->values.array[0].array[newPos] = cols->values.array[0].array[c];

			++newPos;
		}

		/* avoid double-free now */
		cols->values.array[0].array[c].val.str = NULL;
	}

	return true;
}
