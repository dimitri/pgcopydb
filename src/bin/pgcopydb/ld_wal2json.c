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


static bool SetColumnNamesAndValues(LogicalMessageTuple *tuple,
									const char *message,
									JSON_Array *jscols);


/*
 * prepareWal2jsonMessage prepares our internal JSON entry from a wal2json
 * message. Because wal2json emits proper JSON already, we just return the
 * content as-is.
 */
bool
prepareWal2jsonMessage(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	privateContext->metadata.jsonBuffer = strdup(context->buffer);

	return true;
}


/*
 * parseWal2jsonMessageActionAndXid retrieves the XID from the logical
 * replication message found in the buffer as received from the wal2jspon
 * output plugin.
 *
 * Not all messages are supposed to have the XID information.
 *
 *  INPUT: wal2json raw message
 * OUTPUT: pgcopydb LogicalMessageMetadata structure
 */
bool
parseWal2jsonMessageActionAndXid(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	JSON_Value *json = json_parse_string(context->buffer);
	JSON_Object *jsobj = json_value_get_object(json);

	char *action = (char *) json_object_get_string(jsobj, "action");

	if (action == NULL || strlen(action) != 1)
	{
		log_error("Failed to parse action \"%s\" in JSON message: %s",
				  action ? "NULL" : action,
				  context->buffer);
		return false;
	}

	metadata->action = StreamActionFromChar(action[0]);

	if (metadata->action == STREAM_ACTION_UNKNOWN)
	{
		/* errors have already been logged */
		return false;
	}

	if (json_object_has_value(jsobj, "xid"))
	{
		double xid = json_object_get_number(jsobj, "xid");
		metadata->xid = (uint32_t) xid;
	}

	json_value_free(json);

	return true;
}


/*
 * parseWal2jsonMessage parses a JSON message as emitted by wal2json into our
 * own internal representation, that can be later output as SQL text.
 *
 *  INPUT: pgcopydb's own JSON format (action, xid, lsn, timestamp, message)
 * OUTPUT: pgcopydb LogicalTransactionStatement structure
 */
bool
parseWal2jsonMessage(StreamContext *privateContext,
					 char *message,
					 JSON_Value *json)
{
	LogicalTransactionStatement *stmt = privateContext->stmt;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	/* most actions share a need for "schema" and "table" properties */
	JSON_Object *jsobj = json_value_get_object(json);

	char *schema = NULL;
	char *table = NULL;

	schema = (char *) json_object_dotget_string(jsobj, "message.schema");
	table = (char *) json_object_dotget_string(jsobj, "message.table");

	if (schema == NULL || table == NULL)
	{
		log_error("Failed to parse truncated message missing "
				  "schema or table property: %s",
				  message);
		return false;
	}

	switch (metadata->action)
	{
		case STREAM_ACTION_BEGIN:
		case STREAM_ACTION_COMMIT:
		case STREAM_ACTION_SWITCH:
		case STREAM_ACTION_KEEPALIVE:
		{
			log_error("BUG: parseWal2jsonMessage received action %c",
					  metadata->action);
			return false;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			strlcpy(stmt->stmt.truncate.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.truncate.relname, table, NAMEDATALEN);

			break;
		}

		case STREAM_ACTION_INSERT:
		{
			JSON_Array *jscols =
				json_object_dotget_array(jsobj, "message.columns");

			strlcpy(stmt->stmt.insert.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, table, NAMEDATALEN);

			stmt->stmt.insert.new.count = 1;
			stmt->stmt.insert.new.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.insert.new.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			LogicalMessageTuple *tuple = &(stmt->stmt.insert.new.array[0]);

			if (!SetColumnNamesAndValues(tuple, message, jscols))
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
			strlcpy(stmt->stmt.update.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.update.relname, table, NAMEDATALEN);

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

			LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);
			JSON_Array *jsids =
				json_object_dotget_array(jsobj, "message.identity");

			if (!SetColumnNamesAndValues(old, message, jsids))
			{
				log_error("Failed to parse UPDATE identity (old) for logical "
						  "message %s",
						  message);
				return false;
			}

			LogicalMessageTuple *new = &(stmt->stmt.update.new.array[0]);
			JSON_Array *jscols =
				json_object_dotget_array(jsobj, "message.columns");

			if (!SetColumnNamesAndValues(new, message, jscols))
			{
				log_error("Failed to parse UPDATE columns (new) for logical "
						  "message %s",
						  message);
				return false;
			}

			break;
		}

		case STREAM_ACTION_DELETE:
		{
			strlcpy(stmt->stmt.delete.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.delete.relname, table, NAMEDATALEN);

			stmt->stmt.delete.old.count = 1;
			stmt->stmt.delete.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.update.old.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);
			JSON_Array *jsids =
				json_object_dotget_array(jsobj, "message.identity");

			if (!SetColumnNamesAndValues(old, message, jsids))
			{
				log_error("Failed to parse DELETE identity (old) for logical "
						  "message %s",
						  message);
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

	/* keep compiler happy */
	return true;
}


/*
 * SetColumnNames parses the "columns" (or "identity") JSON object from a
 * wal2json logical replication message and fills-in our internal
 * representation for a tuple.
 */
static bool
SetColumnNamesAndValues(LogicalMessageTuple *tuple,
						const char *message,
						JSON_Array *jscols)
{
	int count = json_array_get_count(jscols);

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
	for (int i = 0; i < tuple->cols; i++)
	{
		LogicalMessageValue *valueColumn = &(values->array[i]);

		JSON_Object *jscol = json_array_get_object(jscols, i);
		const char *colname = json_object_get_string(jscol, "name");

		if (jscol == NULL || colname == NULL)
		{
			log_debug("cols[%d]: count = %d, jscols %p, "
					  "json_array_get_count(jscols) == %lld",
					  i,
					  count,
					  jscols,
					  (long long) json_array_get_count(jscols));

			log_error("Failed to parse JSON columns array");
			return false;
		}

		tuple->columns[i] = strndup(colname, NAMEDATALEN);

		if (tuple->columns[i] == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		JSON_Value *jsval = json_object_get_value(jscol, "value");

		switch (json_value_get_type(jsval))
		{
			case JSONNull:
			{
				/* default to TEXTOID to send NULLs over the wire */
				valueColumn->oid = TEXTOID;
				valueColumn->isNull = true;
				break;
			}

			case JSONBoolean:
			{
				bool x = json_value_get_boolean(jsval);

				valueColumn->oid = BOOLOID;
				valueColumn->val.boolean = x;
				valueColumn->isNull = false;
				break;
			}

			case JSONNumber:
			{
				double x = json_value_get_number(jsval);

				valueColumn->oid = FLOAT8OID;
				valueColumn->val.float8 = x;
				valueColumn->isNull = false;
				break;
			}

			case JSONString:
			{
				const char *x = json_value_get_string(jsval);
				const char *t = json_object_get_string(jscol, "type");

				if (json_object_has_value(jscol, "type") && streq(t, "bytea"))
				{
					/*
					 * wal2json has the following processing of bytea values:
					 *
					 * string is "\x54617069727573", start after \x
					 *
					 * so we put back the \x prefix here.
					 */
					int slen = strlen(x);
					int blen = slen + 3;

					valueColumn->oid = BYTEAOID;
					valueColumn->val.str = (char *) calloc(blen, sizeof(char));

					if (valueColumn->val.str == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}

					sformat(valueColumn->val.str, blen, "\\x%s", x);

					valueColumn->isNull = false;
					valueColumn->isQuoted = false;
				}
				else
				{
					valueColumn->oid = TEXTOID;
					valueColumn->val.str = strdup(x);
					valueColumn->isNull = false;
					valueColumn->isQuoted = false;
				}
				break;
			}

			default:
			{
				log_error("Failed to parse column \"%s\" "
						  "JSON type for \"value\": %s",
						  colname,
						  message);
				return false;
			}
		}
	}

	return true;
}
