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
} TestDecodingHeader;


static bool parseTestDecodingMessageHeader(TestDecodingHeader *header,
										   const char *message);

static bool SetColumnNamesAndValues(LogicalMessageTuple *tuple,
									TestDecodingHeader *header,
									const char *message);

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

	privateContext->jsonBuffer = strdup(jsonstr);

	if (privateContext->jsonBuffer == NULL)
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

			break;
		}

		default:
		{
			log_error("Unknown message action %d", metadata->action);
			return false;
		}
	}

	log_error("parseTestDecodingMessage is not implemented yet");
	return false;
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
	log_info("SetColumnNamesAndValues: %s", message + header->offset);

	log_error("SetColumnNamesAndValues is not implemented yet");
	return false;
}
