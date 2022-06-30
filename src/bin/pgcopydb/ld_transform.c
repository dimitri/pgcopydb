/*
 * src/bin/pgcopydb/ld_transform.c
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
#include "lock_utils.h"
#include "log.h"
#include "parsing.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "stream.h"
#include "string_utils.h"
#include "summary.h"


static bool SetColumnNamesAndValues(LogicalMessageTuple *tuple,
									const char *message,
									JSON_Array *jscols);

static bool streamLogicalTransactionAppendStatement(LogicalTransaction *txn,
													LogicalTransactionStatement *stmt
													);


/*
 * stream_transform_file transforms a JSON formatted file as received from the
 * wal2json logical decoding plugin into an SQL file ready for applying to the
 * target database.
 */
bool
stream_transform_file(char *jsonfilename, char *sqlfilename)
{
	StreamContent content = { 0 };
	long size = 0L;

	strlcpy(content.filename, jsonfilename, sizeof(content.filename));

	if (!read_file(content.filename, &(content.buffer), &size))
	{
		/* errors have already been logged */
		return false;
	}

	content.count =
		splitLines(content.buffer, content.lines, MAX_STREAM_CONTENT_COUNT);

	if (content.count >= MAX_STREAM_CONTENT_COUNT)
	{
		log_error("Failed to split file \"%s\" in lines: pgcopydb support only "
				  "files with up to %d lines, and more were found",
				  content.filename,
				  MAX_STREAM_CONTENT_COUNT);
		free(content.buffer);
		return false;
	}

	log_debug("stream_transform_file: read %d lines from \"%s\"",
			  content.count,
			  content.filename);

	int maxTxnsCount = content.count / 2; /* {action: B} {action: C} */
	LogicalTransactionArray txns = { 0 };

	/* the actual count is maintained in the for loop below */
	txns.count = 0;
	txns.array =
		(LogicalTransaction *) calloc(maxTxnsCount, sizeof(LogicalTransaction));

	if (txns.array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Read the JSON-lines file that we received from streaming logical
	 * decoding messages with wal2json, and parse the JSON messages into our
	 * internal representation structure.
	 */
	int currentTxIndex = 0;
	LogicalTransaction *currentTx = &(txns.array[currentTxIndex]);

	for (int i = 0; i < content.count; i++)
	{
		char *message = content.lines[i];
		LogicalMessageMetadata *metadata = &(content.messages[i]);

		log_debug("stream_transform_file[%d]: %s", i, message);

		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json, false))
		{
			/* errors have already been logged */
			json_value_free(json);
			return false;
		}

		if (!parseMessage(currentTx, metadata, message, json))
		{
			log_error("Failed to parse JSON message: %s", message);
			json_value_free(json);
			return false;
		}

		json_value_free(json);

		/* it is time to close the current transaction and prepare a new one? */
		if (metadata->action == STREAM_ACTION_COMMIT)
		{
			++txns.count;
			++currentTxIndex;

			if ((maxTxnsCount - 1) < currentTxIndex)
			{
				log_error("Parsing transaction %d, which is more than the "
						  "maximum allocated transaction count %d",
						  currentTxIndex + 1,
						  maxTxnsCount);
				return false;
			}

			currentTx = &(txns.array[currentTxIndex]);
		}
	}

	log_debug("stream_transform_file read %d transactions", txns.count);

	/*
	 * Now that we have read and parsed the JSON file into our internal
	 * structure that represents SQL transactions with statements, output the
	 * content in the SQL format.
	 */
	FILE *sql = fopen_with_umask(sqlfilename, "w", FOPEN_FLAGS_W, 0644);

	if (sql == NULL)
	{
		log_error("Failed to create and open file \"%s\"", sqlfilename);
		return false;
	}

	for (int i = 0; i < txns.count; i++)
	{
		LogicalTransaction *currentTx = &(txns.array[i]);

		if (!stream_write_transaction(sql, currentTx))
		{
			/* errors have already been logged */
			return false;
		}

		(void) FreeLogicalTransaction(currentTx);
	}

	if (fclose(sql) == EOF)
	{
		log_error("Failed to write file \"%s\"", sqlfilename);
		return false;
	}

	return true;
}


/*
 * parseMessage parses a JSON message as emitted by wal2json into our own
 * internal representation, that can be later output as SQL text.
 */
bool
parseMessage(LogicalTransaction *txn,
			 LogicalMessageMetadata *metadata,
			 char *message,
			 JSON_Value *json)
{
	if (txn == NULL)
	{
		log_error("BUG: parseMessage called with a NULL LogicalTransaction");
		return false;
	}

	if (metadata == NULL)
	{
		log_error("BUG: parseMessage called with a NULL LogicalMessageMetadata");
		return false;
	}

	if (message == NULL)
	{
		log_error("BUG: parseMessage called with a NULL message");
		return false;
	}

	if (json == NULL)
	{
		log_error("BUG: parseMessage called with a NULL JSON_Value");
		return false;
	}

	if (txn->xid > 0 && txn->xid != metadata->xid)
	{
		log_debug("%s", message);
		log_error("BUG: logical message xid is %lld, which is different "
				  "from the current transaction xid %lld",
				  (long long) metadata->xid,
				  (long long) txn->xid);

		return false;
	}

	/* common entries for supported statements */
	LogicalTransactionStatement *stmt = NULL;

	JSON_Object *jsobj = json_value_get_object(json);
	char *schema = NULL;
	char *table = NULL;

	if (metadata->action == STREAM_ACTION_TRUNCATE ||
		metadata->action == STREAM_ACTION_INSERT ||
		metadata->action == STREAM_ACTION_UPDATE ||
		metadata->action == STREAM_ACTION_DELETE)
	{
		stmt = (LogicalTransactionStatement *)
			   malloc(sizeof(LogicalTransactionStatement));

		if (stmt == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		stmt->action = metadata->action;

		schema = (char *) json_object_get_string(jsobj, "schema");
		table = (char *) json_object_get_string(jsobj, "table");

		if (schema == NULL || table == NULL)
		{
			log_error("Failed to parse truncate message missing "
					  "schema or table property: %s",
					  message);
			return false;
		}
	}

	switch (metadata->action)
	{
		case STREAM_ACTION_BEGIN:
		{
			txn->xid = metadata->xid;
			txn->beginLSN = metadata->lsn;
			strlcpy(txn->timestamp, metadata->timestamp, sizeof(txn->timestamp));
			txn->first = NULL;

			break;
		}

		case STREAM_ACTION_COMMIT:
		{
			txn->commitLSN = metadata->lsn;

			break;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			stmt->action = metadata->action;
			strlcpy(stmt->stmt.truncate.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.truncate.relname, table, NAMEDATALEN);

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		case STREAM_ACTION_INSERT:
		{
			JSON_Array *jscols = json_object_get_array(jsobj, "columns");

			stmt->action = metadata->action;

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

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		case STREAM_ACTION_UPDATE:
		{
			stmt->action = metadata->action;
			strlcpy(stmt->stmt.insert.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, table, NAMEDATALEN);

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
			JSON_Array *jsids = json_object_get_array(jsobj, "identity");

			if (!SetColumnNamesAndValues(old, message, jsids))
			{
				log_error("Failed to parse UPDATE identity (old) for logical "
						  "message %s",
						  message);
				return false;
			}

			LogicalMessageTuple *new = &(stmt->stmt.update.new.array[0]);
			JSON_Array *jscols = json_object_get_array(jsobj, "columns");

			if (!SetColumnNamesAndValues(new, message, jscols))
			{
				log_error("Failed to parse UPDATE columns (new) for logical "
						  "message %s",
						  message);
				return false;
			}

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		case STREAM_ACTION_DELETE:
		{
			stmt->action = metadata->action;
			strlcpy(stmt->stmt.insert.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, table, NAMEDATALEN);

			stmt->stmt.delete.old.count = 1;
			stmt->stmt.delete.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.update.old.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);
			JSON_Array *jsids = json_object_get_array(jsobj, "identity");

			if (!SetColumnNamesAndValues(old, message, jsids))
			{
				log_error("Failed to parse DELETE identity (old) for logical "
						  "message %s",
						  message);
				return false;
			}

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

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
 * streamLogicalTransactionAppendStatement appends a statement to the current
 * transaction.
 *
 * There are two ways to append a statement to an existing transaction:
 *
 *  1. it's a new statement altogether, we just append to the linked-list
 *
 *  2. it's the same statement as the previous one, we only add an entry to the
 *     already existing tuple array created on the previous statement
 *
 * This allows to then generate multi-values insert commands, for instance.
 *
 * TODO: at the moment we don't pack several statements that look alike into
 * the same one.
 */
static bool
streamLogicalTransactionAppendStatement(LogicalTransaction *txn,
										LogicalTransactionStatement *stmt)
{
	if (txn == NULL)
	{
		log_error("BUG: streamLogicalTransactionAppendStatement "
				  "called with a NULL LogicalTransaction");
		return false;
	}

	if (stmt == NULL)
	{
		log_error("BUG: streamLogicalTransactionAppendStatement "
				  "called with a NULL LogicalTransactionStatement");
		return false;
	}

	if (txn->first == NULL)
	{
		txn->first = stmt;
		txn->last = stmt;

		stmt->prev = NULL;
		stmt->next = NULL;
	}
	else
	{
		if (txn->last != NULL)
		{
			/* update the current last entry of the linked-list */
			txn->last->next = stmt;
		}

		/* the new statement now becomes the last entry of the linked-list */
		stmt->prev = txn->last;
		stmt->next = NULL;
		txn->last = stmt;
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

				valueColumn->oid = TEXTOID;
				valueColumn->val.str = strdup(x);
				valueColumn->isNull = false;
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


/*
 * FreeLogicalTransaction frees the malloc'ated memory areas of a
 * LogicalTransaction.
 */
void
FreeLogicalTransaction(LogicalTransaction *tx)
{
	LogicalTransactionStatement *currentStmt = tx->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
	{
		switch (currentStmt->action)
		{
			case STREAM_ACTION_INSERT:
			{
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.insert.new));
				break;
			}

			case STREAM_ACTION_UPDATE:
			{
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.update.old));
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.update.new));
				break;
			}

			case STREAM_ACTION_DELETE:
			{
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.delete.old));
				break;
			}

			/* no malloc'ated area in a BEGIN, COMMIT, or TRUNCATE statement */
			default:
			{
				break;
			}
		}
	}
}


/*
 * FreeLogicalMessageTupleArray frees the malloc'ated memory areas of a
 * LogicalMessageTupleArray.
 */
void
FreeLogicalMessageTupleArray(LogicalMessageTupleArray *tupleArray)
{
	for (int s = 0; s < tupleArray->count; s++)
	{
		LogicalMessageTuple *stmt = &(tupleArray->array[s]);

		free(stmt->columns);

		for (int r = 0; r < stmt->values.count; r++)
		{
			LogicalMessageValues *values = &(stmt->values.array[r]);

			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (value->oid == TEXTOID)
				{
					free(value->val.str);
				}
			}

			free(stmt->values.array);
		}
	}
}


/*
 * stream_write_transaction writes the LogicalTransaction statements as SQL to
 * the already open out stream.
 */
bool
stream_write_transaction(FILE *out, LogicalTransaction *tx)
{
	fformat(out,
			"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			OUTPUT_BEGIN,
			(long long) tx->xid,
			LSN_FORMAT_ARGS(tx->beginLSN),
			tx->timestamp);

	LogicalTransactionStatement *currentStmt = tx->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
	{
		switch (currentStmt->action)
		{
			case STREAM_ACTION_INSERT:
			{
				if (!stream_write_insert(out, &(currentStmt->stmt.insert)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_UPDATE:
			{
				if (!stream_write_update(out, &(currentStmt->stmt.update)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_DELETE:
			{
				if (!stream_write_delete(out, &(currentStmt->stmt.delete)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_TRUNCATE:
			{
				if (!stream_write_truncate(out, &(currentStmt->stmt.truncate)))
				{
					return false;
				}
				break;
			}

			default:
			{
				log_error("BUG: Failed to write SQL action %d",
						  currentStmt->action);
				return false;
			}
		}
	}

	fformat(out, "%s{\"xid\": %lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			OUTPUT_COMMIT,
			(long long) tx->xid,
			LSN_FORMAT_ARGS(tx->commitLSN),
			tx->timestamp);

	return true;
}


/*
 * stream_write_insert writes an INSERT statement to the already open out
 * stream.
 */
bool
stream_write_insert(FILE *out, LogicalMessageInsert *insert)
{
	/* loop over INSERT statements targeting the same table */
	for (int s = 0; s < insert->new.count; s++)
	{
		LogicalMessageTuple *stmt = &(insert->new.array[s]);

		fformat(out, "INSERT INTO \"%s\".\"%s\" ",
				insert->nspname,
				insert->relname);

		/* loop over column names and add them to the out stream */
		fformat(out, "(");
		for (int c = 0; c < stmt->cols; c++)
		{
			fformat(out, "%s%s", c > 0 ? ", " : "", stmt->columns[c]);
		}
		fformat(out, ")");

		/* now loop over VALUES rows */
		fformat(out, " VALUES ");

		for (int r = 0; r < stmt->values.count; r++)
		{
			LogicalMessageValues *values = &(stmt->values.array[r]);

			/* now loop over column values for this VALUES row */
			fformat(out, "%s(", r > 0 ? ", " : "");
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				fformat(out, "%s", v > 0 ? ", " : "");

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}

			fformat(out, ")");
		}

		fformat(out, ";\n");
	}

	return true;
}


/*
 * stream_write_update writes an UPDATE statement to the already open out
 * stream.
 */
bool
stream_write_update(FILE *out, LogicalMessageUpdate *update)
{
	if (update->old.count != update->new.count)
	{
		log_error("Failed to write UPDATE statement "
				  "with %d old rows and %d new rows",
				  update->old.count,
				  update->new.count);
		return false;
	}

	/* loop over UPDATE statements targeting the same table */
	for (int s = 0; s < update->old.count; s++)
	{
		LogicalMessageTuple *old = &(update->old.array[s]);
		LogicalMessageTuple *new = &(update->new.array[s]);

		fformat(out, "UPDATE \"%s\".\"%s\" ", update->nspname, update->relname);

		if (old->values.count != new->values.count ||
			old->values.count != 1 ||
			new->values.count != 1)
		{
			log_error("Failed to write multi-values UPDATE statement "
					  "with %d old rows and %d new rows",
					  old->values.count,
					  new->values.count);
			return false;
		}

		fformat(out, "SET ");

		for (int r = 0; r < new->values.count; r++)
		{
			LogicalMessageValues *values = &(new->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (new->cols <= v)
				{
					log_error("Failed to write UPDATE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  new->cols);
					return false;
				}

				fformat(out, "%s", v > 0 ? ", " : "");
				fformat(out, "\"%s\" = ", new->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		fformat(out, " WHERE ");

		for (int r = 0; r < old->values.count; r++)
		{
			LogicalMessageValues *values = &(old->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (old->cols <= v)
				{
					log_error("Failed to write UPDATE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  old->cols);
					return false;
				}

				fformat(out, "%s", v > 0 ? " and " : "");
				fformat(out, "\"%s\" = ", old->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		fformat(out, ";\n");
	}

	return true;
}


/*
 * stream_write_delete writes an DELETE statement to the already open out
 * stream.
 */
bool
stream_write_delete(FILE *out, LogicalMessageDelete *delete)
{
	/* loop over DELETE statements targeting the same table */
	for (int s = 0; s < delete->old.count; s++)
	{
		LogicalMessageTuple *old = &(delete->old.array[s]);

		fformat(out,
				"DELETE FROM \"%s\".\"%s\"",
				delete->nspname,
				delete->relname);

		fformat(out, " WHERE ");

		for (int r = 0; r < old->values.count; r++)
		{
			LogicalMessageValues *values = &(old->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (old->cols <= v)
				{
					log_error("Failed to write DELETE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  old->cols);
					return false;
				}

				fformat(out, "%s", v > 0 ? " and " : "");
				fformat(out, "\"%s\" = ", old->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		fformat(out, ";\n");
	}

	return true;
}


/*
 * stream_write_truncate writes an TRUNCATE statement to the already open out
 * stream.
 */
bool
stream_write_truncate(FILE *out, LogicalMessageTruncate *truncate)
{
	fformat(out, "TRUNCATE ONLY %s.%s\n", truncate->nspname, truncate->relname);

	return true;
}


/*
 * stream_write_value writes the given LogicalMessageValue to the out stream.
 */
bool
stream_write_value(FILE *out, LogicalMessageValue *value)
{
	if (value == NULL)
	{
		log_error("BUG: stream_write_value value is NULL");
		return false;
	}

	if (value->isNull)
	{
		fformat(out, "NULL");
	}
	else
	{
		switch (value->oid)
		{
			case BOOLOID:
			{
				fformat(out, "'%s' ", value->val.boolean ? "t" : "f");
				break;
			}

			case INT8OID:
			{
				fformat(out, "%lld", (long long) value->val.int8);
				break;
			}

			case FLOAT8OID:
			{
				fformat(out, "%g", value->val.float8);
				break;
			}

			case TEXTOID:
			{
				fformat(out, "'%s'", value->val.str);
				break;
			}

			default:
			{
				log_error("BUG: stream_write_insert value with oid %d",
						  value->oid);
				return false;
			}
		}
	}

	return true;
}
