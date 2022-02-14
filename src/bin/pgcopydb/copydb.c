/*
 * src/bin/pgcopydb/copydb.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"

#define COMMON_GUC_SETTINGS \
	{ "client_encoding", "'UTF-8'" },

GUC srcSettings[] = {
	COMMON_GUC_SETTINGS
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
					bool resume)
{
	CopyFilePaths *cfPaths = &(copySpecs->cfPaths);
	DirectoryState *dirState = &(copySpecs->dirState);

	pid_t pid = getpid();

	if (!copydb_prepare_filepaths(cfPaths, dir))
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
		else if (dirState->allDone)
		{
			log_fatal("Please use --restart to allow for removing files "
					  "that belong to a completed previous run.");
			return false;
		}

		/* if we did nothing yet, just act as if --resume was used */
		else if (!dirState->schemaDumpIsDone)
		{
			log_debug("schema dump has not been done yet, just continue");
		}

		/* if --resume has been used, we just continue */
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
	dirState->blobsCopyIsDone = false;

	dirState->allDone =
		dirState->schemaDumpIsDone &&
		dirState->schemaPreDataHasBeenRestored &&
		dirState->schemaPostDataHasBeenRestored &&
		dirState->tableCopyIsDone &&
		dirState->indexCopyIsDone &&
		dirState->sequenceCopyIsDone;

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
copydb_prepare_filepaths(CopyFilePaths *cfPaths, const char *dir)
{
	char topdir[MAXPGPATH] = { 0 };

	if (dir != NULL && !IS_EMPTY_STRING_BUFFER(dir))
	{
		strlcpy(topdir, dir, sizeof(topdir));
	}
	else
	{
		char tmpdir[MAXPGPATH] = { 0 };

		if (!get_env_copy_with_fallback("XDG_RUNTIME_DIR",
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

	/* now that we have our topdir, prepare all the others from there */
	sformat(cfPaths->pidfile, MAXPGPATH, "%s/pgcopydb.pid", cfPaths->topdir);
	sformat(cfPaths->snfile, MAXPGPATH, "%s/snapshot", cfPaths->topdir);
	sformat(cfPaths->schemadir, MAXPGPATH, "%s/schema", cfPaths->topdir);
	sformat(cfPaths->rundir, MAXPGPATH, "%s/run", cfPaths->topdir);
	sformat(cfPaths->tbldir, MAXPGPATH, "%s/run/tables", cfPaths->topdir);
	sformat(cfPaths->idxdir, MAXPGPATH, "%s/run/indexes", cfPaths->topdir);

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

	return true;
}


/*
 * copydb_prepare_dump_paths computes the paths for the pg_dump and pg_restore
 * activities.
 */
bool
copydb_prepare_dump_paths(CopyFilePaths *cfPaths, DumpPaths *dumpPaths)
{
	sformat(dumpPaths->preFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "pre.dump");

	sformat(dumpPaths->postFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "post.dump");

	sformat(dumpPaths->listFilename, MAXPGPATH, "%s/%s",
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
				  CopyDataSection section,
				  char *snapshot,
				  RestoreOptions restoreOptions,
				  bool skipLargeObjects,
				  bool restart,
				  bool resume)
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
		.skipLargeObjects = skipLargeObjects,

		.restart = restart,
		.resume = resume,

		.tableJobs = tableJobs,
		.indexJobs = indexJobs,
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
						SourceTable *source)
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
	strlcpy(tmpTableSpecs.sourceSnapshot.pguri,
			specs->sourceSnapshot.pguri,
			sizeof(tmpTableSpecs.sourceSnapshot.pguri));

	strlcpy(tmpTableSpecs.sourceSnapshot.snapshot,
			specs->sourceSnapshot.snapshot,
			sizeof(tmpTableSpecs.sourceSnapshot.snapshot));

	/* copy the SourceTable into our memory area */
	tmpTableSpecs.sourceTable = *source;

	/* copy the structure as a whole memory area to the target place */
	*tableSpecs = tmpTableSpecs;

	/* compute the table fully qualified name */
	sformat(tableSpecs->qname, sizeof(tableSpecs->qname),
			"\"%s\".\"%s\"",
			tableSpecs->sourceTable.nspname,
			tableSpecs->sourceTable.relname);


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

	IsolationLevel level = ISOLATION_SERIALIZABLE;
	bool readOnly = true;
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

	log_info("Exported snapshot \"%s\" from the source database",
			 snapshot->snapshot);

	return true;
}


/*
 * copydb_set_snapshot opens a transaction and set it to re-use an existing
 * snapshot.
 */
bool
copydb_set_snapshot(TransactionSnapshot *snapshot)
{
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

	/*
	 * As Postgres docs for SET TRANSACTION SNAPSHOT say:
	 *
	 * Furthermore, the transaction must already be set to SERIALIZABLE or
	 * REPEATABLE READ isolation level (otherwise, the snapshot would be
	 * discarded immediately, since READ COMMITTED mode takes a new snapshot
	 * for each command).
	 */
	IsolationLevel level = ISOLATION_REPEATABLE_READ;
	bool readOnly = true;
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
copydb_close_snapshot(TransactionSnapshot *snapshot)
{
	PGSQL *pgsql = &(snapshot->pgsql);

	if (!pgsql_commit(pgsql))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  snapshot->snapshot,
				  snapshot->pguri);
		return false;
	}

	(void) pgsql_finish(pgsql);

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
		if (!copydb_set_snapshot(sourceSnapshot))
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
	log_fatal("Terminating all subprocesses");
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
					return true;
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
copydb_collect_finished_subprocesses()
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
					return true;
				}

				break;
			}

			case 0:
			{
				/*
				 * We're using WNOHANG, 0 means there are no stopped or exited
				 * children.
				 */
				return true;
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
