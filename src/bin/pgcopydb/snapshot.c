/*
 * src/bin/pgcopydb/snapshot.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <inttypes.h>

#include "copydb.h"
#include "log.h"

/*
 * copydb_copy_snapshot initializes a new TransactionSnapshot from another
 * snapshot that's been exported already, copying the connection string and the
 * snapshot identifier.
 */
bool
copydb_copy_snapshot(CopyDataSpec *specs, TransactionSnapshot *snapshot)
{
	PGSQL pgsql = { 0 };
	TransactionSnapshot *source = &(specs->sourceSnapshot);

	/* copy our source snapshot data into the new snapshot instance */
	snapshot->pgsql = pgsql;
	snapshot->connectionType = source->connectionType;

	/* this is set at set/export/CREATE_REPLICATION_SLOT time */
	snapshot->kind = SNAPSHOT_KIND_UNKNOWN;

	/* remember if the replication slot has been created already */
	snapshot->exportedCreateSlotSnapshot = source->exportedCreateSlotSnapshot;
	snapshot->pguri = strdup(source->pguri);
	strlcpy(snapshot->snapshot, source->snapshot, sizeof(snapshot->snapshot));

	return true;
}


/*
 * copydb_open_snapshot opens a snapshot on the given connection.
 *
 * This is needed in the main process, so that COPY processes can then re-use
 * the snapshot, and thus we get a consistent view of the database all along.
 */
bool
copydb_export_snapshot(TransactionSnapshot *snapshot)
{
	PGSQL *pgsql = &(snapshot->pgsql);

	log_debug("copydb_export_snapshot");

	snapshot->kind = SNAPSHOT_KIND_SQL;

	if (!pgsql_init(pgsql, snapshot->pguri, snapshot->connectionType))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * As Postgres docs for SET TRANSACTION SNAPSHOT say:
	 *
	 * Furthermore, the transaction must already be set to SERIALIZABLE or
	 * REPEATABLE READ isolation level (otherwise, the snapshot would be
	 * discarded immediately, since READ COMMITTED mode takes a new snapshot
	 * for each command).
	 *
	 * When --filters are used, pgcopydb creates TEMP tables on the source
	 * database to then implement the filtering as JOINs with the Postgres
	 * catalogs. And even TEMP tables need read-write transaction.
	 */
	IsolationLevel level = ISOLATION_SERIALIZABLE;
	bool readOnly = false;
	bool deferrable = true;

	if (!pgsql_set_transaction(pgsql, level, readOnly, deferrable))
	{
		/* errors have already been logged */
		(void) pgsql_finish(pgsql);
		return false;
	}

	if (!pgsql_export_snapshot(pgsql,
							   snapshot->snapshot,
							   sizeof(snapshot->snapshot)))
	{
		/* errors have already been logged */
		(void) pgsql_finish(pgsql);
		return false;
	}

	snapshot->state = SNAPSHOT_STATE_EXPORTED;

	log_info("Exported snapshot \"%s\" from the source database",
			 snapshot->snapshot);

	/* also set our GUC values for the source connection */
	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	GUC *settings =
		pgsql->pgversion_num < 90600 ? srcSettings95 : srcSettings;

	if (!pgsql_set_gucs(pgsql, settings))
	{
		log_fatal("Failed to set our GUC settings on the source connection, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_set_snapshot opens a transaction and set it to re-use an existing
 * snapshot.
 */
bool
copydb_set_snapshot(CopyDataSpec *copySpecs)
{
	TransactionSnapshot *snapshot = &(copySpecs->sourceSnapshot);
	PGSQL *pgsql = &(snapshot->pgsql);

	snapshot->kind = SNAPSHOT_KIND_SQL;

	if (!pgsql_init(pgsql, snapshot->pguri, snapshot->connectionType))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	if (copySpecs->consistent)
	{
		/*
		 * As Postgres docs for SET TRANSACTION SNAPSHOT say:
		 *
		 * Furthermore, the transaction must already be set to SERIALIZABLE or
		 * REPEATABLE READ isolation level (otherwise, the snapshot would be
		 * discarded immediately, since READ COMMITTED mode takes a new
		 * snapshot for each command).
		 *
		 * When --filters are used, pgcopydb creates TEMP tables on the source
		 * database to then implement the filtering as JOINs with the Postgres
		 * catalogs. And even TEMP tables need read-write transaction.
		 */
		IsolationLevel level = ISOLATION_REPEATABLE_READ;
		bool readOnly = false;
		bool deferrable = true;

		if (!pgsql_set_transaction(pgsql, level, readOnly, deferrable))
		{
			/* errors have already been logged */
			(void) pgsql_finish(pgsql);
			return false;
		}

		if (!pgsql_set_snapshot(pgsql, snapshot->snapshot))
		{
			/* errors have already been logged */
			(void) pgsql_finish(pgsql);
			return false;
		}

		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_SET;
	}
	else
	{
		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_NOT_CONSISTENT;
	}

	/* also set our GUC values for the source connection */
	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	GUC *settings =
		pgsql->pgversion_num < 90600 ? srcSettings95 : srcSettings;

	if (!pgsql_set_gucs(pgsql, settings))
	{
		log_fatal("Failed to set our GUC settings on the source connection, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_close_snapshot closes the snapshot on Postgres by committing the
 * transaction and finishing the connection.
 */
bool
copydb_close_snapshot(CopyDataSpec *copySpecs)
{
	TransactionSnapshot *snapshot = &(copySpecs->sourceSnapshot);
	PGSQL *pgsql = &(snapshot->pgsql);

	if (snapshot->state == SNAPSHOT_STATE_SET ||
		snapshot->state == SNAPSHOT_STATE_EXPORTED ||
		snapshot->state == SNAPSHOT_STATE_NOT_CONSISTENT)
	{
		/* we might need to close our logical stream connection, if any */
		if (snapshot->kind == SNAPSHOT_KIND_LOGICAL)
		{
			(void) pgsql_finish(&(snapshot->stream.pgsql));
		}
		else if (snapshot->kind == SNAPSHOT_KIND_SQL)
		{
			/* only COMMIT sql snapshot kinds, no need for logical rep ones */
			if (!pgsql_commit(pgsql))
			{
				log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
						  snapshot->snapshot,
						  snapshot->safeURI.pguri);
				return false;
			}
		}

		(void) pgsql_finish(pgsql);
	}

	copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_CLOSED;

	if (snapshot->state == SNAPSHOT_STATE_EXPORTED)
	{
		if (!unlink_file(copySpecs->cfPaths.snfile))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_prepare_snapshot connects to the source database and either export a
 * new Postgres snapshot, or set the transaction's snapshot to the given
 * already exported snapshot (see --snapshot and PGCOPYDB_SNAPSHOT).
 */
bool
copydb_prepare_snapshot(CopyDataSpec *copySpecs)
{
	/* when --not-consistent is used, we have nothing to do here */
	if (!copySpecs->consistent)
	{
		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_SKIPPED;
		log_debug("copydb_prepare_snapshot: --not-consistent, skipping");
		return true;
	}

	/*
	 * First, we need to open a snapshot that we're going to re-use in all our
	 * connections to the source database. When the --snapshot option has been
	 * used, instead of exporting a new snapshot, we can just re-use it.
	 */
	TransactionSnapshot *sourceSnapshot = &(copySpecs->sourceSnapshot);

	if (IS_EMPTY_STRING_BUFFER(sourceSnapshot->snapshot))
	{
		if (!copydb_export_snapshot(sourceSnapshot))
		{
			log_fatal("Failed to export a snapshot on \"%s\"",
					  sourceSnapshot->pguri);
			return false;
		}
	}
	else
	{
		if (!copydb_set_snapshot(copySpecs))
		{
			log_fatal("Failed to use given --snapshot \"%s\"",
					  sourceSnapshot->snapshot);
			return false;
		}

		log_info("[SNAPSHOT] Using snapshot \"%s\" on the source database",
				 sourceSnapshot->snapshot);
	}

	/* store the snapshot in a file, to support --resume --snapshot ... */
	if (!write_file(sourceSnapshot->snapshot,
					strlen(sourceSnapshot->snapshot),
					copySpecs->cfPaths.snfile))
	{
		log_fatal("Failed to create the snapshot file \"%s\"",
				  copySpecs->cfPaths.snfile);
		return false;
	}

	log_notice("Wrote snapshot \"%s\" to file \"%s\"",
			   sourceSnapshot->snapshot,
			   copySpecs->cfPaths.snfile);

	return true;
}


/*
 * copydb_create_logical_replication_slot uses Postgres logical replication
 * protocol command CREATE_REPLICATION_SLOT to create a replication slot on the
 * source database, and exports a snapshot while doing so.
 */
bool
copydb_create_logical_replication_slot(CopyDataSpec *copySpecs,
									   const char *logrep_pguri,
									   ReplicationSlot *slot)
{
	TransactionSnapshot *sourceSnapshot = &(copySpecs->sourceSnapshot);

	/*
	 * Now is the time to check if a previous command such as
	 *
	 *   pgcopydb snapshot --follow --plugin ... --slot-name ...
	 *
	 * did create the replication slot for us while exporting the snapshot. we
	 * can then re-use the replication slot and the exported snapshot here.
	 *
	 * On the other hand, if a snapshot was exported without the --follow
	 * option then we can't re-use that snapshot.
	 */
	if (slot->lsn != InvalidXLogRecPtr &&
		!IS_EMPTY_STRING_BUFFER(slot->snapshot))
	{
		log_info("Re-using replication slot \"%s\" "
				 "created at %X/%X with snapshot \"%s\"",
				 slot->slotName,
				 LSN_FORMAT_ARGS(slot->lsn),
				 slot->snapshot);
		return true;
	}
	else if (!IS_EMPTY_STRING_BUFFER(sourceSnapshot->snapshot))
	{
		log_fatal("Failed to use --snapshot \"%s\" which was not created by "
				  "the replication protocol command CREATE_REPLICATION_SLOT",
				  sourceSnapshot->snapshot);
		log_info("Consider using pgcopydb snapshot --follow");
		return false;
	}

	sourceSnapshot->kind = SNAPSHOT_KIND_LOGICAL;

	LogicalStreamClient *stream = &(sourceSnapshot->stream);

	if (!pgsql_init_stream(stream,
						   logrep_pguri,
						   slot->plugin,
						   slot->slotName,
						   InvalidXLogRecPtr,
						   InvalidXLogRecPtr))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_create_logical_replication_slot(stream, slot))
	{
		log_error("Failed to create a logical replication slot "
				  "and export a snapshot, see above for details");
		return false;
	}

	/* expose the replication slot snapshot as the main transaction snapshot */
	strlcpy(sourceSnapshot->snapshot,
			slot->snapshot,
			sizeof(sourceSnapshot->snapshot));

	sourceSnapshot->state = SNAPSHOT_STATE_EXPORTED;
	sourceSnapshot->exportedCreateSlotSnapshot = true;

	/* store the snapshot in a file, to support --resume --snapshot ... */
	if (!write_file(sourceSnapshot->snapshot,
					strlen(sourceSnapshot->snapshot),
					copySpecs->cfPaths.snfile))
	{
		log_fatal("Failed to create the snapshot file \"%s\"",
				  copySpecs->cfPaths.snfile);
		return false;
	}

	/* store the replication slot information in a file, same reasons */
	if (!snapshot_write_slot(copySpecs->cfPaths.cdc.slotfile, slot))
	{
		log_fatal("Failed to create the slot file \"%s\"",
				  copySpecs->cfPaths.cdc.slotfile);
		return false;
	}

	return true;
}


/*
 * snapshot_write_slot writes a replication slot information to file.
 */
bool
snapshot_write_slot(const char *filename, ReplicationSlot *slot)
{
	PQExpBuffer contents = createPQExpBuffer();

	appendPQExpBuffer(contents, "%s\n", slot->slotName);
	appendPQExpBuffer(contents, "%X/%X\n", LSN_FORMAT_ARGS(slot->lsn));
	appendPQExpBuffer(contents, "%s\n", slot->snapshot);
	appendPQExpBuffer(contents, "%s\n", OutputPluginToString(slot->plugin));

	if (PQExpBufferBroken(contents))
	{
		log_error("Failed to allocate memory");
		destroyPQExpBuffer(contents);
		return false;
	}

	if (!write_file(contents->data, contents->len, filename))
	{
		log_fatal("Failed to create slot file \"%s\"", filename);

		destroyPQExpBuffer(contents);
		return false;
	}

	destroyPQExpBuffer(contents);
	return true;
}


/*
 * snapshot_read_slot reads a replication slot information from file.
 */
bool
snapshot_read_slot(const char *filename, ReplicationSlot *slot)
{
	char *contents = NULL;
	long fileSize = 0L;

	log_trace("snapshot_read_slot: %s", filename);

	if (!read_file(filename, &contents, &fileSize))
	{
		/* errors have already been logged */
		return false;
	}

	/* make sure to use only the first line of the file, without \n */
	char *lines[BUFSIZE] = { 0 };
	int lineCount = splitLines(contents, lines, BUFSIZE);

	if (lineCount != 4)
	{
		log_error("Failed to parse replication slot file \"%s\"", filename);
		free(contents);
		return false;
	}

	/* 1. slotName */
	int length = strlcpy(slot->slotName, lines[0], sizeof(slot->slotName));

	if (length >= sizeof(slot->slotName))
	{
		log_error("Failed to read replication slot name \"%s\" from file \"%s\", "
				  "length is %lld bytes which exceeds maximum %lld bytes",
				  lines[0],
				  filename,
				  (long long) strlen(lines[0]),
				  (long long) sizeof(slot->slotName));
		free(contents);
		return false;
	}

	/* 2. LSN (consistent_point) */
	if (!parseLSN(lines[1], &(slot->lsn)))
	{
		log_error("Failed to parse LSN \"%s\" from file \"%s\"",
				  lines[1],
				  filename);
		free(contents);
		return false;
	}

	/* 3. snapshot */
	length = strlcpy(slot->snapshot, lines[2], sizeof(slot->snapshot));

	if (length >= sizeof(slot->snapshot))
	{
		log_error("Failed to read replication snapshot \"%s\" from file \"%s\", "
				  "length is %lld bytes which exceeds maximum %lld bytes",
				  lines[2],
				  filename,
				  (long long) strlen(lines[2]),
				  (long long) sizeof(slot->snapshot));
		free(contents);
		return false;
	}

	/* 4. plugin */
	slot->plugin = OutputPluginFromString(lines[3]);

	if (slot->plugin == STREAM_PLUGIN_UNKNOWN)
	{
		log_error("Failed to read plugin \"%s\" from file \"%s\"",
				  lines[3],
				  filename);
		free(contents);
		return false;
	}

	free(contents);

	log_notice("Read replication slot file \"%s\" with snapshot \"%s\", "
			   "slot \"%s\", lsn %X/%X, and plugin \"%s\"",
			   filename,
			   slot->snapshot,
			   slot->slotName,
			   LSN_FORMAT_ARGS(slot->lsn),
			   OutputPluginToString(slot->plugin));

	return true;
}
