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
#include "cli_copy.h"
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


/*
 * copydb_init_tempdir initialises the file paths that are going to be used to
 * store temporary information while the pgcopydb process is running.
 */
bool
copydb_init_workdir(CopyFilePaths *cfPaths, char *dir, bool removeDir)
{
	pid_t pid = getpid();

	if (dir != NULL && !IS_EMPTY_STRING_BUFFER(dir))
	{
		strlcpy(cfPaths->topdir, dir, sizeof(cfPaths->topdir));
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

		sformat(cfPaths->topdir, MAXPGPATH, "%s/pgcopydb", tmpdir);
	}

	/* now that we have our topdir, prepare all the others from there */
	sformat(cfPaths->pidfile, MAXPGPATH, "%s/pgcopydb.pid", cfPaths->topdir);
	sformat(cfPaths->schemadir, MAXPGPATH, "%s/schema", cfPaths->topdir);
	sformat(cfPaths->rundir, MAXPGPATH, "%s/run", cfPaths->topdir);
	sformat(cfPaths->tbldir, MAXPGPATH, "%s/run/tables", cfPaths->topdir);
	sformat(cfPaths->idxdir, MAXPGPATH, "%s/run/indexes", cfPaths->topdir);

	sformat(cfPaths->idxfilepath, MAXPGPATH,
			"%s/run/indexes.json", cfPaths->topdir);

	/* now create the target directories that we depend on. */
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

		/* warn about trashing data from a previous run */
		if (removeDir)
		{
			log_warn("Directory \"%s\" already exists: removing it entirely",
					 cfPaths->topdir);
		}
	}

	/*
	 * dir is the --target provided in file-based commands such as `pgcopydb
	 * dump db`, and we might have to create the whole directory hierarchy that
	 * we expect when given a --target /tmp/demo or something.
	 *
	 * When using a command such as `pgcopydb copy db` then the target is a
	 * Postgres URI and we use internal directories such as /tmp/pgcopydb/ and
	 * we want to avoid deleting files from a previous run, so we bypass
	 * creating empty directories with ensure_empty_dir in that case.
	 *
	 * dir is NULL when the command uses its own internal directories
	 * dir is NOT NULL when the command uses --source dir or --target dir
	 */
	bool shouldCreateDirectories =
		dir == NULL || !directory_exists(cfPaths->topdir);

	/*
	 * And that's why we should cache our decision making: right after this
	 * point, the target directory exists, and we might still have to make the
	 * decision again to create sub-directories. We should ensure consistent
	 * decision making, so we can't call directory_exists(cfPaths->topdir)
	 * anymore.
	 */
	if (shouldCreateDirectories)
	{
		log_debug("mkdir -p \"%s\"", cfPaths->topdir);

		if (removeDir)
		{
			if (!ensure_empty_dir(cfPaths->topdir, 0700))
			{
				/* errors have already been logged. */
				return false;
			}
		}
		else
		{
			if (pg_mkdir_p((char *) cfPaths->topdir, 0700) == -1)
			{
				log_fatal("Failed to create directory \"%s\"", cfPaths->topdir);
				return false;
			}
		}
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

	if (shouldCreateDirectories)
	{
		for (int i = 0; dirs[i] != NULL; i++)
		{
			log_debug("mkdir -p \"%s\"", dirs[i]);

			if (removeDir)
			{
				if (!ensure_empty_dir(dirs[i], 0700))
				{
					return false;
				}
			}
			else
			{
				if (pg_mkdir_p((char *) dirs[i], 0700) == -1)
				{
					log_fatal("Failed to create directory \"%s\"", dir);
					return false;
				}
			}
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
				  bool dropIfExists,
				  bool noOwner,
				  bool skipLargeObjects)
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
		.dropIfExists = dropIfExists,
		.noOwner = noOwner,
		.skipLargeObjects = skipLargeObjects,

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

	/* copy the structure as a whole memory area to the target place */
	*specs = tmpCopySpecs;

	/* now compute some global paths that are needed for pgcopydb */
	sformat(specs->dumpPaths.preFilename, MAXPGPATH, "%s/%s",
			specs->cfPaths.schemadir, "pre.dump");

	sformat(specs->dumpPaths.postFilename, MAXPGPATH, "%s/%s",
			specs->cfPaths.schemadir, "post.dump");

	sformat(specs->dumpPaths.listFilename, MAXPGPATH, "%s/%s",
			specs->cfPaths.schemadir, "post.list");

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

		.sourceTable = source,
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

	/* copy the structure as a whole memory area to the target place */
	*tableSpecs = tmpTableSpecs;

	/* compute the table fully qualified name */
	sformat(tableSpecs->qname, sizeof(tableSpecs->qname),
			"\"%s\".\"%s\"",
			tableSpecs->sourceTable->nspname,
			tableSpecs->sourceTable->relname);


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
 * copydb_init_index_file_paths prepares a given index (and constraint) file
 * paths to help orchestrate the concurrent operations.
 */
bool
copydb_init_indexes_paths(CopyTableDataSpec *tableSpecs)
{
	SourceIndexArray *indexArray = tableSpecs->indexArray;
	IndexFilePathsArray *indexPathsArray = &(tableSpecs->indexPathsArray);

	indexPathsArray->count = indexArray->count;
	indexPathsArray->array =
		(IndexFilePaths *) malloc(indexArray->count * sizeof(IndexFilePaths));

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);
		IndexFilePaths *indexPaths = &(indexPathsArray->array[i]);

		sformat(indexPaths->lockFile, sizeof(indexPaths->lockFile),
				"%s/%u",
				tableSpecs->cfPaths->rundir,
				index->indexOid);

		sformat(indexPaths->doneFile, sizeof(indexPaths->doneFile),
				"%s/%u.done",
				tableSpecs->cfPaths->idxdir,
				index->indexOid);

		sformat(indexPaths->constraintDoneFile,
				sizeof(indexPaths->constraintDoneFile),
				"%s/%u.done",
				tableSpecs->cfPaths->idxdir,
				index->constraintOid);
	}

	return true;
}


/*
 * copydb_objectid_has_been_processed_already returns true when a doneFile
 * could be found on-disk for the given target object OID.
 */
bool
copydb_objectid_has_been_processed_already(CopyDataSpec *specs, uint32_t oid)
{
	char doneFile[MAXPGPATH] = { 0 };

	/* build the doneFile for the target index or constraint */
	sformat(doneFile, sizeof(doneFile), "%s/%u.done",
			specs->cfPaths.idxdir,
			oid);

	if (file_exists(doneFile))
	{
		char *sql = NULL;
		long size = 0L;

		if (!read_file(doneFile, &sql, &size))
		{
			/* no worries, just skip then */
		}

		/* we're interested in the last line of the file */
		char *lines[BUFSIZE] = { 0 };
		int lineCount = splitLines(sql, lines, BUFSIZE);

		log_debug("Skipping dumpId %d (%s)", oid, lines[lineCount - 1]);

		/* read_file allocates memory */
		free(sql);

		return true;
	}

	return false;
}


/*
 * copydb_dump_source_schema uses pg_dump -Fc --schema --section=pre-data or
 * --section=post-data to dump the source database schema to files.
 */
bool
copydb_dump_source_schema(CopyDataSpec *specs, PostgresDumpSection section)
{
	if (section == PG_DUMP_SECTION_SCHEMA ||
		section == PG_DUMP_SECTION_PRE_DATA ||
		section == PG_DUMP_SECTION_ALL)
	{
		if (!pg_dump_db(&(specs->pgPaths),
						specs->source_pguri,
						"pre-data",
						specs->dumpPaths.preFilename))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (section == PG_DUMP_SECTION_SCHEMA ||
		section == PG_DUMP_SECTION_POST_DATA ||
		section == PG_DUMP_SECTION_ALL)
	{
		if (!pg_dump_db(&(specs->pgPaths),
						specs->source_pguri,
						"post-data",
						specs->dumpPaths.postFilename))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_target_prepare_schema restores the pre.dump file into the target
 * database.
 */
bool
copydb_target_prepare_schema(CopyDataSpec *specs)
{
	if (!file_exists(specs->dumpPaths.preFilename))
	{
		log_fatal("File \"%s\" does not exists", specs->dumpPaths.preFilename);
		return false;
	}

	if (!pg_restore_db(&(specs->pgPaths),
					   specs->target_pguri,
					   specs->dumpPaths.preFilename,
					   NULL,
					   specs->dropIfExists,
					   specs->noOwner))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_target_finalize_schema finalizes the schema after all the data has
 * been copied over, and after indexes and their constraints have been created
 * too.
 */
bool
copydb_target_finalize_schema(CopyDataSpec *specs)
{
	if (!file_exists(specs->dumpPaths.postFilename))
	{
		log_fatal("File \"%s\" does not exists", specs->dumpPaths.postFilename);
		return false;
	}

	/*
	 * The post.dump archive file contains all the objects to create once the
	 * table data has been copied over. It contains in particular the
	 * constraints and indexes that we have already built concurrently in the
	 * previous step, so we want to filter those out.
	 *
	 * Here's how to filter out some objects with pg_restore:
	 *
	 *   1. pg_restore -f- --list post.dump > post.list
	 *   2. edit post.list to comment out lines
	 *   3. pg_restore --use-list post.list post.dump
	 */
	ArchiveContentArray contents = { 0 };

	if (!pg_restore_list(&(specs->pgPaths),
						 specs->dumpPaths.postFilename,
						 &contents))
	{
		/* errors have already been logged */
		return false;
	}

	/* edit our post.list file now */
	PQExpBuffer listContents = createPQExpBuffer();

	if (listContents == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(listContents);
		return false;
	}

	/* for each object in the list, comment when we already processed it */
	for (int i = 0; i < contents.count; i++)
	{
		uint32_t oid = contents.array[i].objectOid;

		/* commenting is done by prepending ";" as prefix to the line */
		char *prefix =
			copydb_objectid_has_been_processed_already(specs, oid) ? ";" : "";

		appendPQExpBuffer(listContents, "%s%d; %u %u\n",
						  prefix,
						  contents.array[i].dumpId,
						  contents.array[i].catalogOid,
						  contents.array[i].objectOid);
	}

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(listContents))
	{
		log_error("Failed to create pg_restore list file: out of memory");
		destroyPQExpBuffer(listContents);
		return false;
	}

	if (!write_file(listContents->data,
					listContents->len,
					specs->dumpPaths.listFilename))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(listContents);
		return false;
	}

	destroyPQExpBuffer(listContents);

	if (!pg_restore_db(&(specs->pgPaths),
					   specs->target_pguri,
					   specs->dumpPaths.postFilename,
					   specs->dumpPaths.listFilename,
					   specs->dropIfExists,
					   specs->noOwner))
	{
		/* errors have already been logged */
		return false;
	}

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
		/* errors have already been logged */
		return false;
	}

	(void) pgsql_finish(pgsql);

	return true;
}


/*
 * copydb_table_data fetches the list of tables from the source database and
 * then run a pg_dump --data-only --schema ... --table ... | pg_restore on each
 * of them, using up to tblJobs sub-processes for that.
 *
 * Each subprocess also fetches a list of indexes for each given table, and
 * creates those indexes in parallel using up to idxJobs sub-processes for
 * that.
 */
bool
copydb_copy_all_table_data(CopyDataSpec *specs)
{
	SourceTableArray tableArray = { 0, NULL };
	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

	/*
	 * First, we need to open a snapshot that we're going to re-use in all our
	 * connections to the source database.
	 */
	TransactionSnapshot *sourceSnapshot = &(specs->sourceSnapshot);

	if (!copydb_export_snapshot(sourceSnapshot))
	{
		log_fatal("Failed to export a snapshot on \"%s\"", sourceSnapshot->pguri);
		return false;
	}

	/*
	 * Check if we have large objects to take into account, because that's not
	 * supported at the moment.
	 */
	if (!specs->skipLargeObjects)
	{
		int64_t largeObjectCount = 0;

		if (!schema_count_large_objects(&(specs->sourceSnapshot.pgsql),
										&largeObjectCount))
		{
			/* errors have already been logged */
			return false;
		}

		if (largeObjectCount > 0)
		{
			log_fatal("pgcopydb version %s has no support for large objects, "
					  "and we found %lld rows in pg_largeobject_metadata",
					  PGCOPYDB_VERSION,
					  (long long) largeObjectCount);
			log_fatal("Consider using --skip-large-objects");
			return false;
		}
	}

	log_info("Listing ordinary tables in \"%s\"", specs->source_pguri);

	/*
	 * Now get the list of the tables we want to COPY over.
	 */
	if (!schema_list_ordinary_tables(&(specs->sourceSnapshot.pgsql),
									 &tableArray))
	{
		/* errors have already been logged */
		return false;
	}

	int count = tableArray.count;
	int errors = 0;

	/* only use as many processes as required */
	if (count < specs->tableJobs)
	{
		specs->tableJobs = count;
	}

	log_info("Fetched information for %d tables, now starting %d processes",
			 tableArray.count,
			 specs->tableJobs);

	specs->tableSpecsArray.count = count;
	specs->tableSpecsArray.array =
		(CopyTableDataSpec *) malloc(count * sizeof(CopyTableDataSpec));

	/*
	 * Prepare the copy specs for each table we have.
	 */
	for (int tableIndex = 0; tableIndex < tableArray.count; tableIndex++)
	{
		/* initialize our TableDataProcess entry now */
		SourceTable *source = &(tableArray.array[tableIndex]);

		/* okay now start the subprocess for this table */
		CopyTableDataSpec *tableSpecs = &(tableSpecsArray->array[tableIndex]);

		if (!copydb_init_table_specs(tableSpecs, specs, source))
		{
			/* errors have already been logged */
			goto terminate;
		}
	}

	/*
	 * Now we have tableArray.count tables to migrate and we want to use
	 * specs->tableJobs sub-processes to work on those migrations. Start the
	 * processes, each sub-process walks through the array and pick the first
	 * table that's not being processed already, until all has been done.
	 */
	if (!copydb_start_table_processes(specs))
	{
		log_fatal("Failed to start a sub-process to COPY the data");
		(void) copydb_fatal_exit();
	}

	/*
	 * Now is a good time to reset sequences: we're waiting for the TABLE DATA
	 * sections and the CREATE INDEX, CONSTRAINTS and VACUUM ANALYZE to be done
	 * with. Sequences can be reset to their expected values while the COPY are
	 * still running, as COPY won't drain identifiers from the sequences
	 * anyway.
	 */
	log_info("Reset the sequences values on the target database");

	if (!copydb_copy_all_sequences(specs))
	{
		/* errors have already been logged */
		++errors;
	}

terminate:

	/* close the source transaction snapshot now */
	if (!copydb_close_snapshot(sourceSnapshot))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  sourceSnapshot->snapshot,
				  sourceSnapshot->pguri);
		++errors;
	}

	/* now we have a unknown count of subprocesses still running */
	if (!copydb_wait_for_subprocesses())
	{
		/* errors have already been logged */
		++errors;
	}

	/*
	 * Now that all the sub-processes are done, we can also unlink the index
	 * concurrency semaphore.
	 */
	if (!semaphore_finish(&(specs->tableSemaphore)))
	{
		log_warn("Failed to remove table concurrency semaphore %d, "
				 "see above for details",
				 specs->tableSemaphore.semId);
	}

	if (!semaphore_finish(&(specs->indexSemaphore)))
	{
		log_warn("Failed to remove index concurrency semaphore %d, "
				 "see above for details",
				 specs->indexSemaphore.semId);
	}

	return errors == 0;
}


/*
 * copydb_copy_all_sequences fetches the list of sequences from the source
 * database and then for each of them runs a SELECT last_value, is_called FROM
 * the sequence on the source database and then calls SELECT setval(); on the
 * target database with the same values.
 */
bool
copydb_copy_all_sequences(CopyDataSpec *specs)
{
	/* re-use the main snapshot and transaction to list sequences */
	PGSQL *src = &(specs->sourceSnapshot.pgsql);
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, specs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	SourceSequenceArray sequenceArray = { 0, NULL };

	log_info("Listing sequences in \"%s\"", specs->source_pguri);

	if (!schema_list_sequences(src, &sequenceArray))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Fetched information for %d sequences", sequenceArray.count);

	if (!pgsql_begin(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	int errors = 0;

	for (int seqIndex = 0; seqIndex < sequenceArray.count; seqIndex++)
	{
		SourceSequence *seq = &(sequenceArray.array[seqIndex]);

		char qname[BUFSIZE] = { 0 };

		sformat(qname, sizeof(qname), "\"%s\".\"%s\"",
				seq->nspname,
				seq->relname);

		if (!schema_get_sequence_value(src, seq))
		{
			/* just skip this one */
			log_warn("Failed to get sequence values for %s", qname);
			++errors;
			continue;
		}

		if (!schema_set_sequence_value(&dst, seq))
		{
			/* just skip this one */
			log_warn("Failed to set sequence values for %s", qname);
			++errors;
			continue;
		}
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		++errors;
	}

	return errors == 0;
}


/*
 * copydb_start_table_processes forks() as many as specs->tableJobs processes
 * that will all concurrently process TABLE DATA and then CREATE INDEX and then
 * also VACUUM ANALYZE each table.
 */
bool
copydb_start_table_processes(CopyDataSpec *specs)
{
	for (int i = 0; i < specs->tableJobs; i++)
	{
		/*
		 * Flush stdio channels just before fork, to avoid double-output
		 * problems.
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
				if (!copydb_start_table_process(specs))
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
	}

	return copydb_wait_for_subprocesses();
}


/*
 * copydb_start_table_process stats a sub-process that walks through the array
 * of tables to COPY over from the source database to the target database.
 *
 * Each process walks through the entire array, and for each entry:
 *
 *  - acquires a semaphore to enter the critical section, alone
 *    - check if the current entry is already done, or being processed
 *    - if not, create the lock file
 *  - exit the critical section
 *  - if we created a lock file, process the selected table
 */
bool
copydb_start_table_process(CopyDataSpec *specs)
{
	int errors = 0;

	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

	/* connect once to the source database for the whole process */
	if (!copydb_set_snapshot(&(specs->sourceSnapshot)))
	{
		/* errors have already been logged */
		return false;
	}

	for (int tableIndex = 0; tableIndex < tableSpecsArray->count; tableIndex++)
	{
		/* initialize our TableDataProcess entry now */
		CopyTableDataSpec *tableSpecs = &(tableSpecsArray->array[tableIndex]);

		bool isBeingProcessed = false;

		if (!copydb_table_is_being_processed(specs,
											 tableSpecs,
											 &isBeingProcessed))
		{
			/* errors have already been logged */
			return false;
		}

		log_trace("%s isBeingProcessed: %s",
				  tableSpecs->qname,
				  isBeingProcessed ? "yes" : "no");

		/* if the table is being processed, we should skip it now */
		if (isBeingProcessed)
		{
			log_debug("Skipping table %s, already done or being processed",
					  tableSpecs->qname);
			continue;
		}

		/*
		 * 1. Now COPY the TABLE DATA from the source to the destination.
		 */
		tableSpecs->sourceSnapshot = specs->sourceSnapshot;

		if (!copydb_copy_table(tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		/* enter the critical section to communicate that we're done */
		if (!copydb_mark_table_as_done(specs, tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * 2. Fetch the list of indexes and constraints attached to this table.
		 */
		SourceIndexArray indexArray = { 0 };

		tableSpecs->indexArray = &indexArray;

		if (tableSpecs->section == DATA_SECTION_INDEXES ||
			tableSpecs->section == DATA_SECTION_CONSTRAINTS ||
			tableSpecs->section == DATA_SECTION_ALL)
		{
			if (!schema_list_table_indexes(&(tableSpecs->sourceSnapshot.pgsql),
										   tableSpecs->sourceTable->nspname,
										   tableSpecs->sourceTable->relname,
										   tableSpecs->indexArray))
			{
				/* errors have already been logged */
				++errors;
			}

			/* build the index file paths we need for the upcoming operations */
			if (!copydb_init_indexes_paths(tableSpecs))
			{
				/* errors have already been logged */
				++errors;
			}
		}

		/*
		 * 3. Now start the CREATE INDEX sub-processes for this table.
		 *
		 *    This starts a main sub-process that launches as many worker
		 *    processes as indexes to be built concurrently, and waits until
		 *    these CREATE INDEX commands are all done.
		 *
		 *    When the indexes are done being built, then the constraints are
		 *    created in the main sub-process.
		 */
		if (!copydb_copy_table_indexes(tableSpecs))
		{
			log_warn("Failed to create all the indexes for %s, "
					 "see above for details",
					 tableSpecs->qname);
			log_warn("Consider `pgcopydb copy indexes` to try again");
			++errors;
		}

		/*
		 * 4. Now start the VACUUM ANALYZE parts of the processing, in a
		 *    concurrent sub-process. The sub-process is running in parallel to
		 *    the CREATE INDEX and constraints processes.
		 */
		if (!copydb_start_vacuum_table(tableSpecs))
		{
			log_warn("Failed to VACUUM ANALYZE %s", tableSpecs->qname);
			++errors;
		}

		/*
		 * 4. Opportunistically see if some CREATE INDEX processed have
		 *    finished already.
		 */
		if (!copydb_collect_finished_subprocesses())
		{
			/* errors have already been logged */
			++errors;
		}
	}

	/* terminate our connection to the source database now */
	(void) pgsql_finish(&(specs->sourceSnapshot.pgsql));

	/*
	 * When this process has finished looping over all the tables in the table
	 * array, then it waits until all the sub-processes are done. That's the
	 * CREATE INDEX workers and the VACUUM workers.
	 */
	if (!copydb_wait_for_subprocesses())
	{
		/* errors have already been logged */
		++errors;
	}

	return errors == 0;
}


/*
 * copydb_table_is_being_processed checks lock and done files to see if a given
 * table is already being processed, or has already been processed entirely by
 * another process. In which case the table is to be skipped by the current
 * process.
 */
bool
copydb_table_is_being_processed(CopyDataSpec *specs,
								CopyTableDataSpec *tableSpecs,
								bool *isBeingProcessed)
{
	/* enter the critical section */
	(void) semaphore_lock(&(specs->tableSemaphore));

	/*
	 * If the doneFile exists, then the table has been processed already,
	 * skip it.
	 *
	 * If the lockFile exists, then the table is currently being processed
	 * by another worker process, skip it.
	 */
	if (file_exists(tableSpecs->tablePaths.doneFile) ||
		file_exists(tableSpecs->tablePaths.lockFile))
	{
		*isBeingProcessed = true;
		(void) semaphore_unlock(&(specs->tableSemaphore));
		return true;
	}

	/*
	 * Otherwise, the table is not being processed yet.
	 */
	*isBeingProcessed = false;

	/*
	 * First, write the lockFile, with a summary of what's going-on.
	 */
	CopyTableSummary emptySummary = { 0 };
	CopyTableSummary *summary =
		(CopyTableSummary *) malloc(sizeof(CopyTableSummary));

	*summary = emptySummary;

	summary->pid = getpid();
	summary->table = tableSpecs->sourceTable;

	sformat(summary->command, sizeof(summary->command),
			"COPY %s;",
			tableSpecs->qname);

	if (!open_table_summary(summary, tableSpecs->tablePaths.lockFile))
	{
		log_info("Failed to create the lock file at \"%s\"",
				 tableSpecs->tablePaths.lockFile);

		/* end of the critical section */
		(void) semaphore_unlock(&(specs->tableSemaphore));

		return false;
	}

	/* attach the new summary to the tableSpecs, where it was NULL before */
	tableSpecs->summary = summary;

	/* end of the critical section */
	(void) semaphore_unlock(&(specs->tableSemaphore));

	return true;
}


/*
 * copydb_mark_table_as_done creates the table doneFile with the expected
 * summary content. To create a doneFile we must acquire the synchronisation
 * semaphore first. The lockFile is also removed here.
 */
bool
copydb_mark_table_as_done(CopyDataSpec *specs,
						  CopyTableDataSpec *tableSpecs)
{
	/* enter the critical section to communicate that we're done */
	(void) semaphore_lock(&(specs->tableSemaphore));

	if (!unlink_file(tableSpecs->tablePaths.lockFile))
	{
		log_error("Failed to remove the lockFile \"%s\"",
				  tableSpecs->tablePaths.lockFile);
		(void) semaphore_unlock(&(specs->tableSemaphore));
		return false;
	}

	/* write the doneFile with the summary and timings now */
	if (!finish_table_summary(tableSpecs->summary,
							  tableSpecs->tablePaths.doneFile))
	{
		log_info("Failed to create the summary file at \"%s\"",
				 tableSpecs->tablePaths.doneFile);
		(void) semaphore_unlock(&(specs->tableSemaphore));
		return false;
	}

	/* end of the critical section */
	(void) semaphore_unlock(&(specs->tableSemaphore));

	return true;
}


/*
 * copydb_copy_table implements the sub-process activity to pg_dump |
 * pg_restore the table's data and then create the indexes and the constraints
 * in parallel.
 */
bool
copydb_copy_table(CopyTableDataSpec *tableSpecs)
{
	/* we want to set transaction snapshot to the main one on the source */
	PGSQL *src = &(tableSpecs->sourceSnapshot.pgsql);
	PGSQL dst = { 0 };

	CopyTableSummary *summary = tableSpecs->summary;

	/* initialize our connection to the target database */
	if (!pgsql_init(&dst, tableSpecs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* COPY the data from the source table to the target table */
	if (tableSpecs->section == DATA_SECTION_TABLE_DATA ||
		tableSpecs->section == DATA_SECTION_ALL)
	{
		/* Now copy the data from source to target */
		log_info("%s", summary->command);

		if (!pg_copy(src, &dst, tableSpecs->qname, tableSpecs->qname))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_copy_table_indexes fetches the index definitions attached to the
 * given source table, and starts as many processes as we have definitions to
 * create all indexes in parallel to each other.
 */
bool
copydb_copy_table_indexes(CopyTableDataSpec *tableSpecs)
{
	if (tableSpecs->section != DATA_SECTION_INDEXES &&
		tableSpecs->section != DATA_SECTION_ALL)
	{
		return true;
	}

	/*
	 * Indexes are created all-at-once in parallel, a sub-process is
	 * forked per index definition to send each SQL/DDL command to the
	 * Postgres server.
	 */
	if (tableSpecs->indexArray->count >= 1)
	{
		log_info("Creating %d index%s for table %s",
				 tableSpecs->indexArray->count,
				 tableSpecs->indexArray->count > 1 ? "es" : "",
				 tableSpecs->qname);
	}
	else
	{
		log_debug("Table %s has no index attached", tableSpecs->qname);
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
			if (!copydb_start_create_indexes(tableSpecs))
			{
				log_error("Failed to create indexes, see above for details");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/*
			 * When done as part of the full copy, we also create each index's
			 * constraint as soon as the parallel index built is done.
			 */
			if (tableSpecs->section == DATA_SECTION_ALL)
			{
				if (!copydb_create_constraints(tableSpecs))
				{
					log_error("Failed to create constraints, "
							  "see above for details");
					exit(EXIT_CODE_INTERNAL_ERROR);
				}
			}

			/*
			 * Create an index list file for the table, so that we can easily
			 * find relevant indexing information from the table itself.
			 */
			if (!create_table_index_file(tableSpecs->summary,
										 tableSpecs->indexArray,
										 tableSpecs->tablePaths.idxListFile))
			{
				/* this only means summary is missing some indexing information */
				log_warn("Failed to create table %s index list file \"%s\"",
						 tableSpecs->qname,
						 tableSpecs->tablePaths.idxListFile);
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
 * copydb_start_create_indexes creates all the indexes for a given table in
 * parallel, using a sub-process to send each index command.
 */
bool
copydb_start_create_indexes(CopyTableDataSpec *tableSpecs)
{
	SourceTable *sourceTable = tableSpecs->sourceTable;
	SourceIndexArray *indexArray = tableSpecs->indexArray;

	for (int i = 0; i < indexArray->count; i++)
	{
		/*
		 * Fork a sub-process for each index, so that they're created in
		 * parallel. Flush stdio channels just before fork, to avoid
		 * double-output problems.
		 */
		fflush(stdout);
		fflush(stderr);

		/* time to create the node_active sub-process */
		int fpid = fork();

		switch (fpid)
		{
			case -1:
			{
				log_error("Failed to fork a process for creating index for "
						  "table \"%s\".\"%s\"",
						  sourceTable->nspname,
						  sourceTable->relname);
				return -1;
			}

			case 0:
			{
				/* child process runs the command */
				if (!copydb_create_index(tableSpecs, i))
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
	}

	/*
	 * Here we need to be sync, so that the caller can continue with creating
	 * the constraints from the indexes right when all the indexes have been
	 * built.
	 */
	return copydb_wait_for_subprocesses();
}


/*
 * copydb_create_indexes creates all the indexes for a given table in
 * parallel, using a sub-process to send each index command.
 */
bool
copydb_create_index(CopyTableDataSpec *tableSpecs, int idx)
{
	IndexFilePaths *indexPaths = &(tableSpecs->indexPathsArray.array[idx]);
	SourceIndexArray *indexArray = tableSpecs->indexArray;
	SourceIndex *index = &(indexArray->array[idx]);

	const char *pguri = tableSpecs->target_pguri;
	PGSQL dst = { 0 };

	/* First, write the lockFile, with a summary of what's going-on */
	CopyIndexSummary summary = {
		.pid = getpid(),
		.index = index,
		.command = { 0 }
	};

	/* prepare the CREATE INDEX command, adding IF NOT EXISTS */
	if (tableSpecs->section == DATA_SECTION_INDEXES)
	{
		int ci_len = strlen("CREATE INDEX ");
		int cu_len = strlen("CREATE UNIQUE INDEX ");

		if (strncmp(index->indexDef, "CREATE INDEX ", ci_len) == 0)
		{
			sformat(summary.command, sizeof(summary.command),
					"CREATE INDEX IF NOT EXISTS %s;",
					index->indexDef + ci_len);
		}
		else if (strncmp(index->indexDef, "CREATE UNIQUE INDEX ", cu_len) == 0)
		{
			sformat(summary.command, sizeof(summary.command),
					"CREATE UNIQUE INDEX IF NOT EXISTS %s;",
					index->indexDef + cu_len);
		}
		else
		{
			log_error("Failed to parse \"%s\"", index->indexDef);
			return false;
		}
	}
	else
	{
		/*
		 * Just use the pg_get_indexdef() command, with an added semi-colon for
		 * logging clarity.
		 */
		sformat(summary.command, sizeof(summary.command),
				"%s;",
				index->indexDef);
	}

	if (!open_index_summary(&summary, indexPaths->lockFile))
	{
		log_info("Failed to create the lock file at \"%s\"",
				 indexPaths->lockFile);
		return false;
	}

	/* now grab an index semaphore lock */
	(void) semaphore_lock(tableSpecs->indexSemaphore);

	if (!pgsql_init(&dst, (char *) pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	log_info("%s", summary.command);

	if (!pgsql_execute(&dst, summary.command))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(tableSpecs->indexSemaphore);
		exit(EXIT_CODE_TARGET);
	}

	/* the CREATE INDEX command is done, release our lock */
	(void) semaphore_unlock(tableSpecs->indexSemaphore);

	/* create the doneFile for the index */
	if (!finish_index_summary(&summary, indexPaths->doneFile))
	{
		log_info("Failed to create the summary file at \"%s\"",
				 indexPaths->doneFile);
		return false;
	}

	/* also remove the lockFile, we don't need it anymore */
	if (!unlink_file(indexPaths->lockFile))
	{
		/* just continue, this is not a show-stopper */
		log_warn("Failed to remove the lockFile \"%s\"", indexPaths->lockFile);
	}

	return true;
}


/*
 * copydb_create_constraints loops over the index definitions for a given table
 * and creates all the associated constraints, one after the other.
 */
bool
copydb_create_constraints(CopyTableDataSpec *tableSpecs)
{
	int errors = 0;
	SourceIndexArray *indexArray = tableSpecs->indexArray;

	const char *pguri = tableSpecs->target_pguri;
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, (char *) pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);
		IndexFilePaths *indexPaths = &(tableSpecs->indexPathsArray.array[i]);

		if (index->constraintOid > 0 &&
			!IS_EMPTY_STRING_BUFFER(index->constraintName))
		{
			char sql[BUFSIZE] = { 0 };

			sformat(sql, sizeof(sql),
					"ALTER TABLE \"%s\".\"%s\" "
					"ADD CONSTRAINT \"%s\" %s "
					"USING INDEX \"%s\"",
					index->tableNamespace,
					index->tableRelname,
					index->constraintName,
					index->isPrimary
					? "PRIMARY KEY"
					: (index->isUnique ? "UNIQUE" : ""),
					index->indexRelname);

			log_info("%s;", sql);

			if (!pgsql_execute(&dst, sql))
			{
				/* errors have already been logged */
				return false;
			}

			/* create the doneFile for the constraint */
			char contents[BUFSIZE] = { 0 };

			sformat(contents, sizeof(contents), "%s;\n", sql);

			if (!write_file(contents,
							strlen(contents),
							indexPaths->constraintDoneFile))
			{
				log_warn("Failed to create the constraint done file");
				log_warn("Restoring the --post-data part of the schema "
						 "might fail because of already existing objects");
			}
		}
	}

	return errors == 0;
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
