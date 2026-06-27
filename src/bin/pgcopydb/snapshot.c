/*
 * src/bin/pgcopydb/snapshot.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <inttypes.h>

#include "catalog.h"
#include "copydb.h"
#include "ld_stream.h"
#include "log.h"
#include "pgsql_timeline.h"

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
	snapshot->isReadOnly = source->isReadOnly;

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
	IsolationLevel level = ISOLATION_REPEATABLE_READ;
	bool deferrable = true;

	if (!pgsql_is_in_recovery(pgsql, &(snapshot->isReadOnly)))
	{
		/* errors have already been logged */
		(void) pgsql_finish(pgsql);
		return false;
	}

	if (!pgsql_begin(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_set_transaction(pgsql, level, snapshot->isReadOnly, deferrable))
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
		bool deferrable = true;

		if (!pgsql_set_transaction(pgsql, level, snapshot->isReadOnly, deferrable))
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
	/*
	 * Allow this function to be called within a context where a snapshot has
	 * already been prepared. Typically copydb_fetch_schema_and_prepare_specs
	 * needs to prepare the snapshot, but some higher-level functions already
	 * did.
	 */
	if (copySpecs->sourceSnapshot.state != SNAPSHOT_STATE_UNKNOWN &&
		copySpecs->sourceSnapshot.state != SNAPSHOT_STATE_CLOSED)
	{
		log_debug("copydb_prepare_snapshot: snapshot \"%s\" already prepared, "
				  "skipping",
				  copySpecs->sourceSnapshot.snapshot);
		return true;
	}

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
	if (!file_exists(copySpecs->cfPaths.snfile))
	{
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
	}

	return true;
}


/*
 * copydb_should_export_snapshot returns true when a snapshot should be
 * exported to be able to implement the command.
 */
bool
copydb_should_export_snapshot(CopyDataSpec *copySpecs)
{
	/* when --not-consistent is used, we have nothing to do here */
	if (!copySpecs->consistent)
	{
		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_SKIPPED;
		log_debug("copydb_prepare_snapshot: --not-consistent, skipping");
		return false;
	}

	/*
	 * When the --snapshot option has been used, instead of exporting a new
	 * snapshot, we can just re-use it.
	 */
	TransactionSnapshot *sourceSnapshot = &(copySpecs->sourceSnapshot);

	return IS_EMPTY_STRING_BUFFER(sourceSnapshot->snapshot);
}


/*
 * copydb_create_logical_replication_slot uses Postgres logical replication
 * protocol command CREATE_REPLICATION_SLOT to create a replication slot on the
 * source database, and exports a snapshot while doing so.
 */
bool
copydb_create_logical_replication_slot(CopyDataSpec *copySpecs,
									   const char *logrep_pguri,
									   ReplicationSlot *slot,
									   char *cdcPathDir)
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

	StreamSpecs specs = { 0 };
	LogicalStreamClient *stream = &(sourceSnapshot->stream);

	if (!pgsql_init_stream(stream,
						   logrep_pguri,
						   cdcPathDir,
						   slot->plugin,
						   slot->slotName,
						   InvalidXLogRecPtr,
						   InvalidXLogRecPtr))
	{
		/* errors have already been logged */
		return false;
	}

	/* for pgoutput, create publication before the slot */
	if (slot->plugin == STREAM_PLUGIN_PGOUTPUT && slot->publicationAutoManaged)
	{
		PGSQL src = { 0 };

		if (!pgsql_init(&src,
						copySpecs->connStrings.source_pguri,
						PGSQL_CONN_SOURCE))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_create_publication(&src, slot->publicationName,
									  &copySpecs->filters))
		{
			log_error("Failed to create publication \"%s\"",
					  slot->publicationName);
			pgsql_finish(&src);
			return false;
		}

		pgsql_finish(&src);
	}

	/* now create the replication slot, exporting the snapshot */
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

	/* store the replication slot information in the SQLite source catalog */
	if (!catalog_write_replication_slot(&(copySpecs->catalogs.source), slot))
	{
		log_fatal("Failed to store replication slot in catalog \"%s\"",
				  copySpecs->catalogs.source.dbfile);
		return false;
	}

	/* initialize catalog timeline history and create the output.db SQLite file */
	specs.paths = copySpecs->cfPaths.cdc;
	specs.sourceDB = &(copySpecs->catalogs.source);
	specs.outputDB = &(copySpecs->catalogs.output);
	specs.replayDB = &(copySpecs->catalogs.replay);
	specs.private.startpos = slot->lsn;

	if (!stream_init_timeline(&specs, stream))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}
