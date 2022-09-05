/*
 * src/bin/pgcopydb/copydb.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parson.h"

#include "cli_common.h"
#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"

#define COMMON_GUC_SETTINGS \
	{ "client_encoding", "'UTF-8'" },

GUC srcSettings[] = {
	COMMON_GUC_SETTINGS
	{ "tcp_keepalives_idle", "'60s'" },
	{ NULL, NULL },
};


GUC dstSettings[] = {
	COMMON_GUC_SETTINGS
	{ "maintenance_work_mem", "'1 GB'" },
	{ "synchronous_commit", "'off'" },
	{ NULL, NULL },
};


/*
 * Not used at the moment. We would like to ensure those settings have values
 * well-suited for the bulk loading operation, but we can't change those
 * settings on the fly.
 */
GUC serverSetttings[] = {
	{ "checkpoint_timeout", "'1 h'" },
	{ "max_wal_size", "' 20 GB'" },
	{ NULL, NULL },
};


/*
 * copydb_init_tempdir initialises the file paths that are going to be used to
 * store temporary information while the pgcopydb process is running.
 */
bool
copydb_init_workdir(CopyDataSpec *copySpecs,
					char *dir,
					bool restart,
					bool resume,
					bool auxilliary)
{
	CopyFilePaths *cfPaths = &(copySpecs->cfPaths);
	DirectoryState *dirState = &(copySpecs->dirState);

	pid_t pid = getpid();

	if (!copydb_prepare_filepaths(cfPaths, dir, auxilliary))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Using work dir \"%s\"", cfPaths->topdir);

	/* check to see if there is already another pgcopydb running */
	if (directory_exists(cfPaths->topdir))
	{
		pid_t onFilePid = 0;

		if (file_exists(cfPaths->pidfile))
		{
			/*
			 * Only implement the "happy path": read_pidfile removes the file
			 * when if fails to read it, or when the pid contained in there in
			 * a stale pid (doesn't belong to any currently running process).
			 */
			if (read_pidfile(cfPaths->pidfile, &onFilePid))
			{
				log_fatal("Working directory \"%s\" already exists and "
						  "contains a pidfile for process %d, "
						  "which is currently running",
						  cfPaths->topdir,
						  onFilePid);
				return false;
			}
		}
	}

	bool removeDir = false;

	if (!copydb_inspect_workdir(cfPaths, dirState))
	{
		/* errors have already been logged */
		return false;
	}

	if (dirState->directoryExists)
	{
		if (restart)
		{
			removeDir = true;
		}

		/* if we did nothing yet, just act as if --resume was used */
		else if (!dirState->schemaDumpIsDone)
		{
			log_debug("schema dump has not been done yet, just continue");
		}

		/* if --resume has been used, we just continue */
		else if (resume)
		{
			/* no-op */
			(void) 0;
		}
		else if (dirState->allDone)
		{
			log_fatal("Please use --restart to allow for removing files "
					  "that belong to a completed previous run.");
			return false;
		}
		else if (!resume)
		{
			log_fatal("Please use --resume --not-consistent to allow "
					  "for resuming from the previous run, "
					  "which failed before completion.");
			return false;
		}

		/*
		 * Here we should have restart true or resume true or we didn't even do
		 * the schema dump on the previous run.
		 */
	}

	/* warn about trashing data from a previous run */
	if (removeDir && !restart)
	{
		log_info("Inspection of \"%s\" shows that it is safe "
				 "to remove it and continue",
				 cfPaths->topdir);
	}

	if (removeDir)
	{
		log_info("Removing directory \"%s\"", cfPaths->topdir);
	}

	/* make sure the directory exists, possibly making it empty */
	if (!copydb_rmdir_or_mkdir(cfPaths->topdir, removeDir))
	{
		/* errors have already been logged */
		return false;
	}

	/* now populate our pidfile */
	if (!create_pidfile(cfPaths->pidfile, pid))
	{
		return false;
	}

	/* and now for the other sub-directories */
	const char *dirs[] = {
		cfPaths->schemadir,
		cfPaths->rundir,
		cfPaths->tbldir,
		cfPaths->idxdir,
		cfPaths->cdc.dir,
		NULL
	};

	for (int i = 0; dirs[i] != NULL; i++)
	{
		if (!copydb_rmdir_or_mkdir(dirs[i], removeDir))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_inspect_workdir inspects the given target directory to see what work
 * has been tracked in there. From the doneFile(s) and the lockFile(s) that we
 * can list in the directory, we can have a good idea of why the command is
 * attempted to be run again.
 */
bool
copydb_inspect_workdir(CopyFilePaths *cfPaths, DirectoryState *dirState)
{
	dirState->directoryExists = directory_exists(cfPaths->topdir);

	if (!dirState->directoryExists)
	{
		return true;
	}

	/* the directory exists, checks if our expected components are there */
	bool foundAllComponents = true;

	const char *dirs[] = {
		cfPaths->schemadir,
		cfPaths->rundir,
		cfPaths->tbldir,
		cfPaths->idxdir,
		NULL
	};

	for (int i = 0; dirs[i] != NULL; i++)
	{
		foundAllComponents = foundAllComponents && directory_exists(dirs[i]);
	}

	if (!foundAllComponents)
	{
		log_debug("copydb_inspect_workdir: not all components found");
		dirState->directoryIsReady = false;
		return true;
	}

	dirState->schemaDumpIsDone =
		file_exists(cfPaths->done.preDataDump) &&
		file_exists(cfPaths->done.postDataDump);

	dirState->schemaPreDataHasBeenRestored =
		file_exists(cfPaths->done.preDataRestore);

	dirState->schemaPostDataHasBeenRestored =
		file_exists(cfPaths->done.postDataRestore);

	dirState->tableCopyIsDone = file_exists(cfPaths->done.tables);
	dirState->indexCopyIsDone = file_exists(cfPaths->done.indexes);
	dirState->sequenceCopyIsDone = file_exists(cfPaths->done.sequences);
	dirState->blobsCopyIsDone = file_exists(cfPaths->done.blobs);

	dirState->allDone =
		dirState->schemaDumpIsDone &&
		dirState->schemaPreDataHasBeenRestored &&
		dirState->schemaPostDataHasBeenRestored &&
		dirState->tableCopyIsDone &&
		dirState->indexCopyIsDone &&
		dirState->sequenceCopyIsDone &&
		dirState->blobsCopyIsDone;

	/* let's be verbose about our inspection results */
	log_info("Work directory \"%s\" already exists", cfPaths->topdir);

	if (dirState->allDone)
	{
		log_info("A previous run has run through completion");
		return true;
	}

	if (dirState->schemaDumpIsDone)
	{
		log_info("Schema dump for pre-data and post-data section have been done");
	}

	if (dirState->schemaPreDataHasBeenRestored)
	{
		log_info("Pre-data schema has been restored on the target instance");
	}

	if (dirState->tableCopyIsDone)
	{
		log_info("All the table data has been copied to the target instance");
	}

	if (dirState->indexCopyIsDone)
	{
		log_info("All the indexes have been copied to the target instance");
	}

	if (dirState->sequenceCopyIsDone)
	{
		log_info("All the sequences have been copied to the target instance");
	}

	if (dirState->blobsCopyIsDone)
	{
		log_info("All the large objects have been copied to the target instance");
	}

	if (dirState->schemaPostDataHasBeenRestored)
	{
		log_info("Post-data schema has been restored on the target instance");
	}

	return true;
}


/*
 * copydb_prepare_filepaths computes all the path components that are needed
 * for top-level operations.
 */
bool
copydb_prepare_filepaths(CopyFilePaths *cfPaths, const char *dir, bool auxilliary)
{
	char topdir[MAXPGPATH] = { 0 };

	if (dir != NULL && !IS_EMPTY_STRING_BUFFER(dir))
	{
		strlcpy(topdir, dir, sizeof(topdir));
	}
	else
	{
		char tmpdir[MAXPGPATH] = { 0 };

		if (!get_env_copy_with_fallback("TMPDIR",
										tmpdir,
										sizeof(tmpdir),
										"/tmp"))
		{
			/* errors have already been logged */
			return false;
		}

		sformat(topdir, sizeof(topdir), "%s/pgcopydb", tmpdir);
	}

	/* first copy the top directory */
	strlcpy(cfPaths->topdir, topdir, sizeof(cfPaths->topdir));

	/* auxilliary processes use a different pidfile */
	if (auxilliary)
	{
		sformat(cfPaths->pidfile, MAXPGPATH, "%s/pgcopydb.aux.pid", cfPaths->topdir);
	}
	else
	{
		sformat(cfPaths->pidfile, MAXPGPATH, "%s/pgcopydb.pid", cfPaths->topdir);
	}

	/* now that we have our topdir, prepare all the others from there */
	sformat(cfPaths->snfile, MAXPGPATH, "%s/snapshot", cfPaths->topdir);
	sformat(cfPaths->schemadir, MAXPGPATH, "%s/schema", cfPaths->topdir);
	sformat(cfPaths->rundir, MAXPGPATH, "%s/run", cfPaths->topdir);
	sformat(cfPaths->tbldir, MAXPGPATH, "%s/run/tables", cfPaths->topdir);
	sformat(cfPaths->idxdir, MAXPGPATH, "%s/run/indexes", cfPaths->topdir);

	/* prepare also the name of the schema file (JSON) */
	sformat(cfPaths->schemafile, MAXPGPATH, "%s/schema.json", cfPaths->topdir);

	/* now prepare the done files */
	struct pair
	{
		char *dst;
		char *fmt;
	};

	struct pair donePaths[] = {
		{ (char *) &(cfPaths->done.preDataDump), "%s/run/dump-pre.done" },
		{ (char *) &(cfPaths->done.postDataDump), "%s/run/dump-post.done" },
		{ (char *) &(cfPaths->done.preDataRestore), "%s/run/restore-pre.done" },
		{ (char *) &(cfPaths->done.postDataRestore), "%s/run/restore-post.done" },

		{ (char *) &(cfPaths->done.tables), "%s/run/tables.done" },
		{ (char *) &(cfPaths->done.indexes), "%s/run/indexes.done" },
		{ (char *) &(cfPaths->done.sequences), "%s/run/sequences.done" },
		{ (char *) &(cfPaths->done.blobs), "%s/run/blobs.done" },
		{ NULL, NULL }
	};

	for (int i = 0; donePaths[i].dst != NULL; i++)
	{
		sformat(donePaths[i].dst, MAXPGPATH, donePaths[i].fmt, cfPaths->topdir);
	}

	/*
	 * Now prepare the Change Data Capture (logical decoding) intermediate
	 * files directory. This needs more care than the transient files that
	 * default to the TMPDIR (or /tmp), and we're using XDG_DATA_HOME this time
	 * (/var, or ~/.local/share).
	 *
	 * When a directory has been provided, use a sub-directory there to store
	 * the Change Data Capture date. Otherwise, use a pgcopydb specific
	 * directory in ~/.local/share or XDG_DATA_HOME.
	 */
	if (dir != NULL && !IS_EMPTY_STRING_BUFFER(dir))
	{
		sformat(cfPaths->cdc.dir, MAXPGPATH, "%s/cdc", cfPaths->topdir);
	}
	else
	{
		char homedir[MAXPGPATH] = { 0 };
		char datadir[MAXPGPATH] = { 0 };
		char fallback[MAXPGPATH] = { 0 };

		if (!get_env_copy("HOME", homedir, MAXPGPATH))
		{
			/* errors have already been logged */
			return false;
		}

		join_path_components(fallback, homedir, ".local/share");

		if (!get_env_copy_with_fallback("XDG_DATA_HOME",
										datadir,
										sizeof(datadir),
										fallback))
		{
			/* errors have already been logged */
			return false;
		}

		sformat(cfPaths->cdc.dir, MAXPGPATH, "%s/pgcopydb", datadir);
	}

	log_debug("Change Data Capture data is managed at \"%s\"",
			  cfPaths->cdc.dir);

	/* now prepare the originfile and timelinehistfile path */
	sformat(cfPaths->cdc.originfile, MAXPGPATH,
			"%s/origin",
			cfPaths->cdc.dir);

	sformat(cfPaths->cdc.tlihistfile, MAXPGPATH,
			"%s/tli.history",
			cfPaths->cdc.dir);

	sformat(cfPaths->cdc.tlifile, MAXPGPATH,
			"%s/tli",
			cfPaths->cdc.dir);

	sformat(cfPaths->cdc.walsegsizefile, MAXPGPATH,
			"%s/wal_segment_size",
			cfPaths->cdc.dir);

	return true;
}


/*
 * copydb_prepare_dump_paths computes the paths for the pg_dump and pg_restore
 * activities.
 */
bool
copydb_prepare_dump_paths(CopyFilePaths *cfPaths, DumpPaths *dumpPaths)
{
	sformat(dumpPaths->rolesFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "roles.sql");

	sformat(dumpPaths->preFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "pre.dump");

	sformat(dumpPaths->postFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "post.dump");

	sformat(dumpPaths->preListFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "pre.list");

	sformat(dumpPaths->postListFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "post.list");

	return true;
}


/*
 * copydb_rmdir_or_mkdir ensure that given directory is empty. For that it
 * either uses rm -rf on an existing directory or just mkdir -p on a possibly
 * existing directory, depending on the removeDir argument.
 */
bool
copydb_rmdir_or_mkdir(const char *dir, bool removeDir)
{
	if (removeDir)
	{
		log_debug("rm -rf \"%s\" && mkdir -p \"%s\"", dir, dir);

		if (!ensure_empty_dir(dir, 0700))
		{
			return false;
		}
	}
	else
	{
		log_debug("mkdir -p \"%s\"", dir);

		if (pg_mkdir_p((char *) dir, 0700) == -1)
		{
			log_fatal("Failed to create directory \"%s\"", dir);
			return false;
		}
	}

	return true;
}


/*
 * copydb_init_specs prepares a CopyDataSpec structure from its pieces, and
 * initialises files paths necessary for collecting a Postgres dump splitted in
 * pre-data and post-data section, and then also a pg_restore --list output
 * file.
 */
bool
copydb_init_specs(CopyDataSpec *specs,
				  char *source_pguri,
				  char *target_pguri,
				  int tableJobs,
				  int indexJobs,
				  uint64_t splitTablesLargerThan,
				  char *splitTablesLargerThanPretty,
				  CopyDataSection section,
				  char *snapshot,
				  RestoreOptions restoreOptions,
				  bool roles,
				  bool skipLargeObjects,
				  bool restart,
				  bool resume,
				  bool consistent)
{
	/* fill-in a structure with the help of the C compiler */
	CopyDataSpec tmpCopySpecs = {
		.cfPaths = specs->cfPaths,
		.pgPaths = specs->pgPaths,

		.source_pguri = { 0 },
		.target_pguri = { 0 },

		.sourceSnapshot = {
			.pgsql = { 0 },
			.pguri = { 0 },
			.connectionType = PGSQL_CONN_SOURCE,
			.snapshot = { 0 }
		},

		.section = section,
		.restoreOptions = restoreOptions,
		.roles = roles,
		.skipLargeObjects = skipLargeObjects,

		.restart = restart,
		.resume = resume,
		.consistent = consistent,

		.tableJobs = tableJobs,
		.indexJobs = indexJobs,
		.splitTablesLargerThan = splitTablesLargerThan,

		.indexSemaphore = { 0 }
	};

	/* initialize the connection strings */
	if (source_pguri != NULL)
	{
		strlcpy(tmpCopySpecs.source_pguri, source_pguri, MAXCONNINFO);
		strlcpy(tmpCopySpecs.sourceSnapshot.pguri, source_pguri, MAXCONNINFO);
	}

	if (target_pguri != NULL)
	{
		strlcpy(tmpCopySpecs.target_pguri, target_pguri, MAXCONNINFO);
	}

	if (snapshot != NULL && !IS_EMPTY_STRING_BUFFER(snapshot))
	{
		strlcpy(tmpCopySpecs.sourceSnapshot.snapshot,
				snapshot,
				sizeof(tmpCopySpecs.sourceSnapshot.snapshot));
	}

	strlcpy(tmpCopySpecs.splitTablesLargerThanPretty,
			splitTablesLargerThanPretty,
			sizeof(tmpCopySpecs.splitTablesLargerThanPretty));

	/* copy the structure as a whole memory area to the target place */
	*specs = tmpCopySpecs;

	/* now compute some global paths that are needed for pgcopydb */
	if (!copydb_prepare_dump_paths(&(specs->cfPaths), &(specs->dumpPaths)))
	{
		/* errors have already been logged */
		return false;
	}

	/* create the table semaphore (critical section, one at a time please) */
	specs->tableSemaphore.initValue = 1;

	if (!semaphore_create(&(specs->tableSemaphore)))
	{
		log_error("Failed to create the table concurrency semaphore "
				  "to orchestrate %d TABLE DATA COPY jobs",
				  tableJobs);
		return false;
	}

	/* create the index semaphore (allow jobs to start) */
	specs->indexSemaphore.initValue = indexJobs;

	if (!semaphore_create(&(specs->indexSemaphore)))
	{
		log_error("Failed to create the index concurrency semaphore "
				  "to orchestrate up to %d CREATE INDEX jobs at the same time",
				  indexJobs);
		return false;
	}

	/* we only respect the --skip-blobs option in pgcopydb copy-db command */
	if (specs->section != DATA_SECTION_ALL)
	{
		specs->skipLargeObjects = true;
	}

	return true;
}


/*
 * copydb_init_table_specs prepares a CopyTableDataSpec structure from its
 * pieces and also initialises files paths necessary for the orchestration of
 * the per-table processes and their summary files.
 */
bool
copydb_init_table_specs(CopyTableDataSpec *tableSpecs,
						CopyDataSpec *specs,
						SourceTable *source,
						int partNumber)
{
	/* fill-in a structure with the help of the C compiler */
	CopyTableDataSpec tmpTableSpecs = {
		.cfPaths = &(specs->cfPaths),
		.pgPaths = &(specs->pgPaths),

		.source_pguri = { 0 },
		.target_pguri = { 0 },
		.sourceSnapshot = {
			.pgsql = { 0 },
			.pguri = { 0 },
			.connectionType = specs->sourceSnapshot.connectionType,
			.snapshot = { 0 }
		},

		.section = specs->section,
		.resume = specs->resume,

		.sourceTable = { 0 },
		.indexArray = NULL,
		.summary = NULL,

		.tableJobs = specs->tableJobs,
		.indexJobs = specs->indexJobs,
		.indexSemaphore = &(specs->indexSemaphore)
	};

	/* initialize the connection strings */
	strlcpy(tmpTableSpecs.source_pguri, specs->source_pguri, MAXCONNINFO);
	strlcpy(tmpTableSpecs.target_pguri, specs->target_pguri, MAXCONNINFO);

	/* initialize the sourceSnapshot buffers */
	if (!copydb_copy_snapshot(specs, &(tmpTableSpecs.sourceSnapshot)))
	{
		/* errors have already been logged */
		return false;
	}

	/* copy the SourceTable into our memory area */
	tmpTableSpecs.sourceTable = *source;

	/* copy the structure as a whole memory area to the target place */
	*tableSpecs = tmpTableSpecs;

	/* compute the table fully qualified name */
	sformat(tableSpecs->qname, sizeof(tableSpecs->qname),
			"\"%s\".\"%s\"",
			tableSpecs->sourceTable.nspname,
			tableSpecs->sourceTable.relname);

	/* This CopyTableDataSpec might be for a partial COPY */
	if (source->partsArray.count >= 1)
	{
		CopyTableDataPartSpec part = {
			.partNumber = partNumber,
			.partCount = source->partsArray.array[partNumber].partCount,
			.min = source->partsArray.array[partNumber].min,
			.max = source->partsArray.array[partNumber].max
		};

		tableSpecs->part = part;

		strlcpy(tableSpecs->part.partKey, source->partKey, NAMEDATALEN);

		/*
		 * Prepare the COPY command.
		 *
		 * The way schema_list_partitions prepares the boundaries is non
		 * overlapping, so we can use the BETWEEN operator to select our source
		 * rows in the COPY sub-query.
		 */
		sformat(tableSpecs->part.copyQuery, sizeof(tableSpecs->part.copyQuery),
				"(SELECT * FROM %s"
				" WHERE \"%s\" BETWEEN %lld AND %lld)",
				tableSpecs->qname,
				tableSpecs->part.partKey,
				(long long) tableSpecs->part.min,
				(long long) tableSpecs->part.max);

		/* now compute the table-specific paths we are using in copydb */
		if (!copydb_init_tablepaths_for_part(tableSpecs,
											 &(tableSpecs->tablePaths),
											 partNumber))
		{
			log_error("Failed to prepare pathnames for partition %d of table %s",
					  partNumber,
					  tableSpecs->qname);
			return false;
		}

		/* used only by one process, the one finishing a partial COPY last */
		sformat(tableSpecs->tablePaths.idxListFile, MAXPGPATH, "%s/%u.idx",
				tableSpecs->cfPaths->tbldir,
				source->oid);

		/*
		 * And now the truncateLockFile and truncateDoneFile, which are used to
		 * provide a critical section to the same-table concurrent processes.
		 */
		sformat(tableSpecs->tablePaths.truncateDoneFile, MAXPGPATH,
				"%s/%u.truncate",
				tableSpecs->cfPaths->tbldir,
				source->oid);
	}
	else
	{
		/* No partition found, so this should be a full table COPY */
		if (partNumber > 0)
		{
			log_error("BUG: copydb_init_table_specs partNumber is %d and "
					  "source table partArray.count is %d",
					  partNumber,
					  source->partsArray.count);
			return false;
		}

		/* now compute the table-specific paths we are using in copydb */
		sformat(tableSpecs->tablePaths.lockFile, MAXPGPATH, "%s/%d",
				tableSpecs->cfPaths->rundir,
				source->oid);

		sformat(tableSpecs->tablePaths.doneFile, MAXPGPATH, "%s/%d.done",
				tableSpecs->cfPaths->tbldir,
				source->oid);

		sformat(tableSpecs->tablePaths.idxListFile, MAXPGPATH, "%s/%u.idx",
				tableSpecs->cfPaths->tbldir,
				source->oid);
	}

	return true;
}


/*
 * copydb_init_tablepaths_for_part computes the lockFile and doneFile pathnames
 * for a given COPY partition of a table.
 */
bool
copydb_init_tablepaths_for_part(CopyTableDataSpec *tableSpecs,
								TableFilePaths *tablePaths,
								int partNumber)
{
	sformat(tablePaths->lockFile, MAXPGPATH, "%s/%d.%d",
			tableSpecs->cfPaths->rundir,
			tableSpecs->sourceTable.oid,
			partNumber);

	sformat(tablePaths->doneFile, MAXPGPATH, "%s/%d.%d.done",
			tableSpecs->cfPaths->tbldir,
			tableSpecs->sourceTable.oid,
			partNumber);

	return true;
}


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
	if (!pgsql_set_gucs(pgsql, srcSettings))
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

	if (copySpecs->sourceSnapshot.state == SNAPSHOT_STATE_SET ||
		copySpecs->sourceSnapshot.state == SNAPSHOT_STATE_EXPORTED ||
		copySpecs->sourceSnapshot.state == SNAPSHOT_STATE_NOT_CONSISTENT)
	{
		if (!pgsql_commit(pgsql))
		{
			char pguri[MAXCONNINFO] = { 0 };

			(void) parse_and_scrub_connection_string(snapshot->pguri, pguri);

			log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
					  snapshot->snapshot,
					  pguri);
			return false;
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

	/* also set our GUC values for the source connection */
	if (!pgsql_set_gucs(&(sourceSnapshot->pgsql), srcSettings))
	{
		log_fatal("Failed to set our GUC settings on the source connection, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_start_vacuum_table runs VACUUM ANALYSE on the given table.
 */
bool
copydb_start_vacuum_table(CopyTableDataSpec *tableSpecs)
{
	if (tableSpecs->section != DATA_SECTION_VACUUM &&
		tableSpecs->section != DATA_SECTION_ALL)
	{
		return true;
	}

	/*
	 * Flush stdio channels just before fork, to avoid double-output problems.
	 */
	fflush(stdout);
	fflush(stderr);

	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a worker process");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			PGSQL dst = { 0 };

			/* initialize our connection to the target database */
			if (!pgsql_init(&dst, tableSpecs->target_pguri, PGSQL_CONN_TARGET))
			{
				/* errors have already been logged */
				return false;
			}

			/* finally, vacuum analyze the table and its indexes */
			char vacuum[BUFSIZE] = { 0 };

			sformat(vacuum, sizeof(vacuum),
					"VACUUM ANALYZE %s",
					tableSpecs->qname);

			log_info("%s;", vacuum);

			if (!pgsql_execute(&dst, vacuum))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			(void) pgsql_finish(&dst);

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			break;
		}
	}

	/* now we're done, and we want async behavior, do not wait */
	return true;
}


/*
 * copydb_fatal_exit sends a termination signal to all the subprocess and waits
 * until all the known subprocess are finished, then returns true.
 */
bool
copydb_fatal_exit()
{
	log_fatal("Terminating all processes in our process group");

	/* signal all sub-processes that now is the time to stop */
	if (kill(0, SIGTERM) == -1)
	{
		log_error("Failed to signal pgcopydb process group: %m");
		return false;
	}

	return copydb_wait_for_subprocesses();
}


/*
 * copydb_wait_for_subprocesses calls waitpid() until no child process is known
 * running. It also fetches the return code of all the sub-processes, and
 * returns true only when all the subprocesses have returned zero (success).
 */
bool
copydb_wait_for_subprocesses()
{
	bool allReturnCodeAreZero = true;

	log_debug("Waiting for sub-processes to finish");

	for (;;)
	{
		int status;

		/* ignore errors */
		pid_t pid = waitpid(-1, &status, WNOHANG);

		switch (pid)
		{
			case -1:
			{
				if (errno == ECHILD)
				{
					/* no more childrens */
					return allReturnCodeAreZero;
				}

				pg_usleep(100 * 1000); /* 100 ms */
				break;
			}

			case 0:
			{
				/*
				 * We're using WNOHANG, 0 means there are no stopped or exited
				 * children sleep for awhile and ask again later.
				 */
				pg_usleep(100 * 1000); /* 100 ms */
				break;
			}

			default:
			{
				int returnCode = WEXITSTATUS(status);

				if (returnCode == 0)
				{
					log_debug("Sub-processes %d exited with code %d",
							  pid, returnCode);
				}
				else
				{
					allReturnCodeAreZero = false;

					log_error("Sub-processes %d exited with code %d",
							  pid, returnCode);
				}
			}
		}
	}

	return allReturnCodeAreZero;
}


/*
 * copydb_collect_finished_subprocesses calls waitpid() to acknowledge finished
 * processes, without waiting for all of them.
 */
bool
copydb_collect_finished_subprocesses(bool *allDone)
{
	bool allReturnCodeAreZero = true;

	for (;;)
	{
		int status;

		/* ignore errors */
		pid_t pid = waitpid(-1, &status, WNOHANG);

		switch (pid)
		{
			case -1:
			{
				if (errno == ECHILD)
				{
					/* no more childrens */
					return allReturnCodeAreZero;
				}

				break;
			}

			case 0:
			{
				/*
				 * We're using WNOHANG, 0 means there are no stopped or exited
				 * children.
				 */
				*allDone = true;
				return allReturnCodeAreZero;
			}

			default:
			{
				int returnCode = WEXITSTATUS(status);

				if (returnCode == 0)
				{
					log_debug("Sub-processes %d exited with code %d",
							  pid, returnCode);
				}
				else
				{
					allReturnCodeAreZero = false;

					log_error("Sub-processes %d exited with code %d",
							  pid, returnCode);
				}

				break;
			}
		}
	}

	return allReturnCodeAreZero;
}


/*
 * copydb_copy_roles copies roles from the source instance into the target
 * instance, using pg_dumpall --roles-only and our own SQL client that reads
 * the file and applies SQL command on the target system.
 */
bool
copydb_copy_roles(CopyDataSpec *copySpecs)
{
	if (!pg_dumpall_roles(&(copySpecs->pgPaths),
						  copySpecs->source_pguri,
						  copySpecs->dumpPaths.rolesFilename))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pg_restore_roles(&(copySpecs->pgPaths),
						  copySpecs->target_pguri,
						  copySpecs->dumpPaths.rolesFilename))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_prepare_schema_json_file prepares a JSON formatted file that contains
 * the list of all the tables and indexes and sequences that are going to be
 * migrated.
 */
bool
copydb_prepare_schema_json_file(CopyDataSpec *copySpecs)
{
	JSON_Value *js = json_value_init_object();
	JSON_Object *jsobj = json_value_get_object(js);

	log_trace("copydb_prepare_schema_json_file");

	/* main options for the setup */
	JSON_Value *jsSetup = json_value_init_object();
	JSON_Object *jsSetupObj = json_value_get_object(jsSetup);

	/* snapshot */
	if (!IS_EMPTY_STRING_BUFFER(copySpecs->sourceSnapshot.snapshot))
	{
		char *snapshot = copySpecs->sourceSnapshot.snapshot;
		json_object_set_string(jsSetupObj, "snapshot", snapshot);
	}

	/* source and target URIs, without passwords */
	char scrubbedSourceURI[MAXCONNINFO] = { 0 };
	char scrubbedTargetURI[MAXCONNINFO] = { 0 };

	(void) parse_and_scrub_connection_string(copySpecs->source_pguri,
											 scrubbedSourceURI);

	(void) parse_and_scrub_connection_string(copySpecs->target_pguri,
											 scrubbedTargetURI);

	json_object_set_string(jsSetupObj, "source_pguri", scrubbedSourceURI);
	json_object_set_string(jsSetupObj, "target_pguri", scrubbedTargetURI);

	json_object_set_number(jsSetupObj,
						   "table-jobs",
						   (double) copySpecs->tableJobs);

	json_object_set_number(jsSetupObj,
						   "index-jobs",
						   (double) copySpecs->indexJobs);

	json_object_set_number(jsSetupObj,
						   "split-tables-larger-than",
						   (double) copySpecs->splitTablesLargerThan);

	json_object_set_value(jsobj, "setup", jsSetup);

	/* filtering, if any */
	if (copySpecs->filters.type != SOURCE_FILTER_TYPE_NONE)
	{
		log_trace("copydb_prepare_schema_json_file: filtering");

		SourceFilters *filters = &(copySpecs->filters);

		JSON_Value *jsFilter = json_value_init_object();
		JSON_Object *jsFilterObj = json_value_get_object(jsFilter);

		json_object_set_string(jsFilterObj,
							   "type",
							   filterTypeToString(filters->type));

		/* exclude-schema */
		if (filters->excludeSchemaList.count > 0)
		{
			JSON_Value *jsSchema = json_value_init_array();
			JSON_Array *jsSchemaArray = json_value_get_array(jsSchema);

			for (int i = 0; i < filters->excludeSchemaList.count; i++)
			{
				char *nspname = filters->excludeSchemaList.array[i].nspname;

				json_array_append_string(jsSchemaArray, nspname);
			}

			json_object_set_value(jsFilterObj, "exclude-schema", jsSchema);
		}

		/* exclude table lists */
		struct section
		{
			char name[NAMEDATALEN];
			SourceFilterTableList *list;
		};

		struct section sections[] = {
			{ "exclude-table", &(filters->excludeTableList) },
			{ "exclude-table-data", &(filters->excludeTableDataList) },
			{ "exclude-index", &(filters->excludeIndexList) },
			{ "include-only-table", &(filters->includeOnlyTableList) },
			{ "", NULL },
		};

		for (int i = 0; sections[i].list != NULL; i++)
		{
			char *sectionName = sections[i].name;
			SourceFilterTableList *list = sections[i].list;

			if (list->count > 0)
			{
				JSON_Value *jsList = json_value_init_array();
				JSON_Array *jsListArray = json_value_get_array(jsList);

				for (int i = 0; i < list->count; i++)
				{
					SourceFilterTable *table = &(list->array[i]);

					JSON_Value *jsTable = json_value_init_object();
					JSON_Object *jsTableObj = json_value_get_object(jsTable);

					json_object_set_string(jsTableObj, "schema", table->nspname);
					json_object_set_string(jsTableObj, "name", table->relname);

					json_array_append_value(jsListArray, jsTable);
				}

				json_object_set_value(jsFilterObj, sectionName, jsList);
			}
		}

		json_object_set_value(jsobj, "filters", jsFilter);
	}

	/* array of tables */
	SourceTableArray *tableArray = &(copySpecs->sourceTableArray);

	log_trace("copydb_prepare_schema_json_file: %d tables", tableArray->count);

	JSON_Value *jsTables = json_value_init_array();
	JSON_Array *jsTableArray = json_value_get_array(jsTables);

	for (int tableIndex = 0; tableIndex < tableArray->count; tableIndex++)
	{
		SourceTable *table = &(tableArray->array[tableIndex]);

		log_trace("copydb_prepare_schema_json_file: tables[%d]: %s.%s",
				  tableIndex,
				  table->nspname,
				  table->relname);

		JSON_Value *jsTable = json_value_init_object();
		JSON_Object *jsTableObj = json_value_get_object(jsTable);

		json_object_set_number(jsTableObj, "oid", (double) table->oid);
		json_object_set_string(jsTableObj, "schema", table->nspname);
		json_object_set_string(jsTableObj, "name", table->relname);

		json_object_set_number(jsTableObj, "reltuples", (double) table->reltuples);
		json_object_set_number(jsTableObj, "bytes", (double) table->bytes);
		json_object_set_string(jsTableObj, "bytes-pretty", table->bytesPretty);

		json_object_set_boolean(jsTableObj, "exclude-data", table->excludeData);

		json_object_set_string(jsTableObj,
							   "restore-list-name",
							   table->restoreListName);

		json_object_set_string(jsTableObj, "part-key", table->partKey);

		/* if we have COPY partitioning, create an array of parts */
		JSON_Value *jsParts = json_value_init_array();
		JSON_Array *jsPartArray = json_value_get_array(jsParts);

		if (table->partsArray.count > 1)
		{
			for (int i = 0; i < table->partsArray.count; i++)
			{
				SourceTableParts *part = &(table->partsArray.array[i]);

				JSON_Value *jsPart = json_value_init_object();
				JSON_Object *jsPartObj = json_value_get_object(jsPart);

				json_object_set_number(jsPartObj, "number",
									   (double) part->partNumber);

				json_object_set_number(jsPartObj, "total",
									   (double) part->partCount);

				json_object_set_number(jsPartObj, "min",
									   (double) part->min);

				json_object_set_number(jsPartObj, "max",
									   (double) part->max);

				json_object_set_number(jsPartObj, "count",
									   (double) part->count);

				json_array_append_value(jsPartArray, jsPart);
			}

			json_object_set_value(jsTableObj, "parts", jsParts);
		}

		json_array_append_value(jsTableArray, jsTable);
	}

	json_object_set_value(jsobj, "tables", jsTables);

	/* array of indexes */
	SourceIndexArray *indexArray = &(copySpecs->sourceIndexArray);

	log_trace("copydb_prepare_schema_json_file: %d indexes", indexArray->count);

	JSON_Value *jsIndexes = json_value_init_array();
	JSON_Array *jsIndexArray = json_value_get_array(jsIndexes);

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);

		JSON_Value *jsIndex = json_value_init_object();
		JSON_Object *jsIndexObj = json_value_get_object(jsIndex);

		json_object_set_number(jsIndexObj, "oid", (double) index->indexOid);
		json_object_set_string(jsIndexObj, "schema", index->indexNamespace);
		json_object_set_string(jsIndexObj, "name", index->indexRelname);

		json_object_set_boolean(jsIndexObj, "isPrimary", index->isPrimary);
		json_object_set_boolean(jsIndexObj, "isUnique", index->isUnique);

		json_object_set_string(jsIndexObj, "columns", index->indexColumns);
		json_object_set_string(jsIndexObj, "sql", index->indexDef);

		json_object_set_string(jsIndexObj,
							   "restore-list-name",
							   index->indexRestoreListName);

		/* add a table object */
		JSON_Value *jsTable = json_value_init_object();
		JSON_Object *jsTableObj = json_value_get_object(jsTable);

		json_object_set_number(jsTableObj, "oid", (double) index->tableOid);
		json_object_set_string(jsTableObj, "schema", index->tableNamespace);
		json_object_set_string(jsTableObj, "name", index->tableRelname);

		json_object_set_value(jsIndexObj, "table", jsTable);

		/* add a constraint object */
		if (index->constraintOid != 0)
		{
			JSON_Value *jsConstraint = json_value_init_object();
			JSON_Object *jsConstraintObj = json_value_get_object(jsConstraint);

			json_object_set_number(jsConstraintObj,
								   "oid",
								   (double) index->constraintOid);

			json_object_set_string(jsConstraintObj,
								   "name",
								   index->constraintName);

			json_object_set_string(jsConstraintObj,
								   "sql",
								   index->constraintDef);

			json_object_set_string(jsIndexObj,
								   "restore-list-name",
								   index->constraintRestoreListName);

			json_object_set_value(jsIndexObj, "constraint", jsConstraint);
		}

		/* append the JSON index to the index table */
		json_array_append_value(jsIndexArray, jsIndex);
	}

	json_object_set_value(jsobj, "indexes", jsIndexes);

	/* array of sequences */
	SourceSequenceArray *sequenceArray = &(copySpecs->sequenceArray);

	log_trace("copydb_prepare_schema_json_file: %d sequences",
			  sequenceArray->count);

	JSON_Value *jsSeqs = json_value_init_array();
	JSON_Array *jsSeqArray = json_value_get_array(jsSeqs);

	for (int seqIndex = 0; seqIndex < sequenceArray->count; seqIndex++)
	{
		SourceSequence *seq = &(sequenceArray->array[seqIndex]);

		JSON_Value *jsSeq = json_value_init_object();
		JSON_Object *jsSeqObj = json_value_get_object(jsSeq);

		json_object_set_number(jsSeqObj, "oid", (double) seq->oid);
		json_object_set_string(jsSeqObj, "schema", seq->nspname);
		json_object_set_string(jsSeqObj, "name", seq->relname);

		json_object_set_number(jsSeqObj, "last-value", (double) seq->lastValue);
		json_object_set_boolean(jsSeqObj, "is-called", (double) seq->isCalled);

		json_object_set_string(jsSeqObj,
							   "restore-list-name",
							   seq->restoreListName);

		json_array_append_value(jsSeqArray, jsSeq);
	}

	json_object_set_value(jsobj, "sequences", jsSeqs);

	/* now pretty-print the JSON to file */
	char *serialized_string = json_serialize_to_string_pretty(js);
	size_t len = strlen(serialized_string);

	log_debug("Storing migration schema in JSON file \"%s\"",
			  copySpecs->cfPaths.schemafile);

	if (!write_file(serialized_string, len, copySpecs->cfPaths.schemafile))
	{
		log_error("Failed to write schema JSON file, see above for details");
		return false;
	}

	json_free_serialized_string(serialized_string);
	json_value_free(js);

	return true;
}
