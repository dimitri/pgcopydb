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

	strlcpy(snapshot->pguri, source->pguri, sizeof(snapshot->pguri));
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
				char pguri[MAXCONNINFO] = { 0 };

				(void) parse_and_scrub_connection_string(snapshot->pguri, pguri);

				log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
						  snapshot->snapshot,
						  pguri);
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

	return true;
}


/*
 * copydb_create_logical_replication_slot uses Postgres logical replication
 * protocol command CREATE_REPLICATION_SLOT to create a replication slot on the
 * source database, and exports a snapshot while doing so.
 *
 * This is a Postgres 9.6 compatibility function, because in Postgres 9.6
 * creating a logical replication slot always exports a new snapshot.
 */
bool
copydb_create_logical_replication_slot(CopyDataSpec *copySpecs,
									   const char *logrep_pguri,
									   StreamOutputPlugin plugin,
									   const char *slotName)
{
	TransactionSnapshot *sourceSnapshot = &(copySpecs->sourceSnapshot);

	/*
	 * We can't re-use --snapshot here, sorry.
	 */
	if (!IS_EMPTY_STRING_BUFFER(sourceSnapshot->snapshot))
	{
		log_fatal("Failed to use provided --snapshot. "
				  "The source Postgres server is running version %s, "
				  "where it's not possible to use both --follow and --snapshot",
				  sourceSnapshot->pgsql.pgversion);
		return false;
	}

	sourceSnapshot->kind = SNAPSHOT_KIND_LOGICAL;

	LogicalStreamClient *stream = &(sourceSnapshot->stream);

	if (!pgsql_init_stream(stream,
						   logrep_pguri,
						   plugin,
						   slotName,
						   InvalidXLogRecPtr,
						   InvalidXLogRecPtr))
	{
		/* errors have already been logged */
		return false;
	}

	uint64_t lsn = 0;

	if (!pgsql_create_logical_replication_slot(stream,
											   &lsn,
											   sourceSnapshot->snapshot,
											   sizeof(sourceSnapshot->snapshot)))
	{
		log_error("Failed to create a logical replication slot "
				  "and export a snapshot, see above for details");
		return false;
	}

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

	log_info("Created logical replication slot \"%s\" with plugin \"%s\" "
			 "at %X/%X and exported snapshot %s",
			 slotName,
			 OutputPluginToString(plugin),
			 LSN_FORMAT_ARGS(lsn),
			 sourceSnapshot->snapshot);

	return true;
}
