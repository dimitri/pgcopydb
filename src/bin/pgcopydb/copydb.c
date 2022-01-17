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


static int waitUntilOneSubprocessIsDone(TableDataProcessArray *subProcessArray);


/*
 * copydb_init_tempdir initialises the file paths that are going to be used to
 * store temporary information while the pgcopydb process is running.
 */
bool
copydb_init_workdir(CopyFilePaths *cfPaths, char *dir)
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
		if (dir == NULL)
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
		if (!ensure_empty_dir(cfPaths->topdir, 0700))
		{
			/* errors have already been logged. */
			return false;
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
			if (!ensure_empty_dir(dirs[i], 0700))
			{
				return false;
			}
		}
	}
	else
	{
		/* with dir is not null, refrain from removing anything */
		for (int i = 0; dirs[i] != NULL; i++)
		{
			if (!directory_exists(dir))
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
				  CopyFilePaths *cfPaths,
				  PostgresPaths *pgPaths,
				  char *source_pguri,
				  char *target_pguri,
				  int tableJobs,
				  int indexJobs,
				  bool dropIfExists,
				  bool noOwner)
{
	/* fill-in a structure with the help of the C compiler */
	CopyDataSpec tmpCopySpecs = {
		.cfPaths = cfPaths,
		.pgPaths = pgPaths,

		.source_pguri = source_pguri,
		.target_pguri = target_pguri,

		.dropIfExists = dropIfExists,
		.noOwner = noOwner,

		.tableJobs = tableJobs,
		.indexJobs = indexJobs,
		.indexSemaphore = { 0 }
	};

	/* copy the structure as a whole memory area to the target place */
	*specs = tmpCopySpecs;

	/* now compute some global paths that are needed for pgcopydb */
	sformat(specs->dumpPaths.preFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "pre.dump");

	sformat(specs->dumpPaths.postFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "post.dump");

	sformat(specs->dumpPaths.listFilename, MAXPGPATH, "%s/%s",
			cfPaths->schemadir, "post.list");

	/* create the index semaphore */
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
						SourceTable *source,
						TableDataProcess *process)
{
	/* fill-in a structure with the help of the C compiler */
	CopyTableDataSpec tmpTableSpecs = {
		.cfPaths = specs->cfPaths,
		.pgPaths = specs->pgPaths,

		.source_pguri = specs->source_pguri,
		.target_pguri = specs->target_pguri,

		.sourceTable = source,
		.indexArray = NULL,
		.process = process,

		.tableJobs = specs->tableJobs,
		.indexJobs = specs->indexJobs,
		.indexSemaphore = &(specs->indexSemaphore)
	};

	/* copy the structure as a whole memory area to the target place */
	*tableSpecs = tmpTableSpecs;

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
			specs->cfPaths->idxdir,
			oid);

	if (file_exists(doneFile))
	{
		char *sql = NULL;
		long size = 0L;

		if (!read_file(doneFile, &sql, &size))
		{
			/* no worries, just skip then */
		}

		log_debug("Skipping dumpId %d (%s)", oid, sql);

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
		if (!pg_dump_db(specs->pgPaths,
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
		if (!pg_dump_db(specs->pgPaths,
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

	if (!pg_restore_db(specs->pgPaths,
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

	if (!pg_restore_list(specs->pgPaths,
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

	if (!pg_restore_db(specs->pgPaths,
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
	PGSQL pgsql = { 0 };
	SourceTableArray tableArray = { 0, NULL };
	TableDataProcessArray tableProcessArray = { specs->tableJobs, NULL };
	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

	tableProcessArray.array =
		(TableDataProcess *) malloc(specs->tableJobs * sizeof(TableDataProcess));

	if (tableProcessArray.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
	}

	/* ensure the memory area is initialized to all zeroes */
	memset((void *) tableProcessArray.array,
		   0,
		   specs->tableJobs * sizeof(TableDataProcess));

	log_info("Listing ordinary tables in \"%s\"", specs->source_pguri);

	if (!pgsql_init(&pgsql, specs->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!schema_list_ordinary_tables(&pgsql, &tableArray))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Fetched information for %d tables", tableArray.count);

	int count = tableArray.count;

	specs->tableSpecsArray.count = count;
	specs->tableSpecsArray.array =
		(CopyTableDataSpec *) malloc(count * sizeof(CopyTableDataSpec));

	for (int tableIndex = 0; tableIndex < tableArray.count; tableIndex++)
	{
		int subProcessIndex =
			waitUntilOneSubprocessIsDone(&tableProcessArray);

		/* initialize our TableDataProcess entry now */
		SourceTable *source = &(tableArray.array[tableIndex]);
		TableDataProcess *process = &(tableProcessArray.array[subProcessIndex]);

		/* okay now start the subprocess for this table */
		CopyTableDataSpec *tableSpecs = &(tableSpecsArray->array[tableIndex]);

		if (!copydb_init_table_specs(tableSpecs, specs, source, process))
		{
			/* errors have already been logged */
			return false;
		}

		if (!copydb_start_table_data(tableSpecs))
		{
			log_fatal("Failed to start a table data copy process for table "
					  "\"%s\".\"%s\", see above for details",
					  tableSpecs->sourceTable->nspname,
					  tableSpecs->sourceTable->relname);

			(void) copydb_fatal_exit(&tableProcessArray);
			return false;
		}

		log_debug("[%d] is processing table %d \"%s\".\"%s\" with oid %d",
				  tableSpecs->process->pid,
				  tableIndex,
				  tableSpecs->sourceTable->nspname,
				  tableSpecs->sourceTable->relname,
				  tableSpecs->process->oid);
	}

	/* now we have a unknown count of subprocesses still running */
	bool success = copydb_wait_for_subprocesses();

	/*
	 * Now that all the sub-processes are done, we can also unlink the index
	 * concurrency semaphore.
	 */
	if (!semaphore_finish(&(specs->indexSemaphore)))
	{
		log_warn("Failed to remove index concurrency semaphore %d, "
				 "see above for details",
				 specs->indexSemaphore.semId);
	}

	return success;
}


/*
 * copydb_fatal_exit sends a termination signal to all the subprocess and waits
 * until all the known subprocess are finished, then returns true.
 */
bool
copydb_fatal_exit(TableDataProcessArray *subProcessArray)
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
					log_debug("Sub-processes exited with code %d", returnCode);
				}
				else
				{
					allReturnCodeAreZero = false;

					log_error("Sub-processes exited with code %d", returnCode);
				}
			}
		}
	}

	return allReturnCodeAreZero;
}


/*
 * waitUntilOneSubprocessIsDone waits until one of the subprocess (any one of
 * them) has created its doneFile, and returns the array index of that process.
 *
 * The subProcessArray entry that is found done is also cleaned-up.
 */
static int
waitUntilOneSubprocessIsDone(TableDataProcessArray *subProcessArray)
{
	for (;;)
	{
		/* don't block user's interrupt (C-c and the like) */
		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			(void) copydb_fatal_exit(subProcessArray);
			return false;
		}

		/* probe each sub-process' doneFile */
		for (int procIndex = 0; procIndex < subProcessArray->count; procIndex++)
		{
			/* when the subprocess pid is zero, we have found an entry */
			if (subProcessArray->array[procIndex].pid == 0)
			{
				log_debug("waitUntilOneSubprocessIsDone: %d", procIndex);
				return procIndex;
			}

			/* when we have a doneFile, the process is done */
			if (file_exists(subProcessArray->array[procIndex].doneFile))
			{
				log_debug("waitUntilOneSubprocessIsDone: found \"%s\"",
						  subProcessArray->array[procIndex].doneFile);

				/* clean-up the array entry, stop tracking this PID */
				subProcessArray->array[procIndex].pid = 0;

				/* now we have a slot to process the current table */
				log_debug("waitUntilOneSubprocessIsDone: %d", procIndex);
				return procIndex;
			}
		}

		/*
		 * If all subprocesses are still running, wait for a little
		 * while and then check again. The check being cheap enough
		 * (it's just an access(2) system call), we sleep for 150ms
		 * only.
		 */
		pg_usleep(150 * 1000);
	}
}


/*
 * copydb_start_table_data forks a sub-process that handles the table data
 * copying, using pg_dump | pg_restore for a specific given table.
 */
bool
copydb_start_table_data(CopyTableDataSpec *tableSpecs)
{
	/* first prepare the status and summary files paths and contents */
	SourceTable *source = tableSpecs->sourceTable;
	TableDataProcess *process = tableSpecs->process;

	process->oid = source->oid;

	sformat(process->lockFile, sizeof(process->lockFile),
			"%s/%d", tableSpecs->cfPaths->rundir, process->oid);

	sformat(process->doneFile, sizeof(process->doneFile),
			"%s/%d.done", tableSpecs->cfPaths->tbldir, process->oid);

	/* Flush stdio channels just before fork, to avoid double-output problems */
	fflush(stdout);
	fflush(stderr);

	/* time to create the node_active sub-process */
	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a process for copying table data for "
					  "\"%s\".\"%s\"",
					  tableSpecs->sourceTable->nspname,
					  tableSpecs->sourceTable->relname);
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			if (!copydb_copy_table(tableSpecs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			tableSpecs->process->pid = fpid;

			return true;
		}
	}
}


/*
 * copydb_copy_table implements the sub-process activity to pg_dump |
 * pg_restore the table's data and then create the indexes and the constraints
 * in parallel.
 */
bool
copydb_copy_table(CopyTableDataSpec *tableSpecs)
{
	PGSQL src = { 0 };
	PGSQL dst = { 0 };

	char qname[BUFSIZE] = { 0 };

	sformat(qname, sizeof(qname), "\"%s\".\"%s\"",
			tableSpecs->sourceTable->nspname,
			tableSpecs->sourceTable->relname);

	/* First, write the lockFile, with a summary of what's going-on */
	CopyTableSummary summary = {
		.pid = getpid(),
		.table = tableSpecs->sourceTable,
	};

	sformat(summary.command, sizeof(summary.command), "COPY %s;", qname);

	if (!open_table_summary(&summary, tableSpecs->process->lockFile))
	{
		log_info("Failed to create the lock file at \"%s\"",
				 tableSpecs->process->lockFile);
		return false;
	}

	/* Now copy the data from source to target */
	log_info("%s", summary.command);

	if (!pgsql_init(&src, tableSpecs->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_init(&dst, tableSpecs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pg_copy(&src, &dst, qname, qname))
	{
		/* errors have already been logged */
		return false;
	}

	/* now say we're done with the table data */
	if (!finish_table_summary(&summary, tableSpecs->process->doneFile))
	{
		log_info("Failed to create the summary file at \"%s\"",
				 tableSpecs->process->doneFile);
		return false;
	}

	/* also remove the lockFile, we don't need it anymore */
	if (!unlink_file(tableSpecs->process->lockFile))
	{
		/* just continue, this is not a show-stopper */
		log_warn("Failed to remove the lockFile \"%s\"",
				 tableSpecs->process->lockFile);
	}

	/* then fetch the index list for this table */
	SourceIndexArray indexArray = { 0 };

	tableSpecs->indexArray = &indexArray;

	if (!schema_list_table_indexes(&src,
								   tableSpecs->sourceTable->nspname,
								   tableSpecs->sourceTable->relname,
								   tableSpecs->indexArray))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Indexes are created all-at-once in parallel, a sub-process is forked per
	 * index definition to send each SQL/DDL command to the Postgres server.
	 */
	if (tableSpecs->indexArray->count >= 1)
	{
		log_info("Creating %d index%s for table \"%s\".\"%s\"",
				 tableSpecs->indexArray->count,
				 tableSpecs->indexArray->count > 1 ? "es" : "",
				 tableSpecs->sourceTable->nspname,
				 tableSpecs->sourceTable->relname);
	}
	else
	{
		log_debug("Table \"%s\".\"%s\" has no index attached",
				  tableSpecs->sourceTable->nspname,
				  tableSpecs->sourceTable->relname);
	}

	/* build the index file paths we need for the upcoming operations */
	if (!copydb_init_indexes_paths(tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_start_create_indexes(tableSpecs))
	{
		log_error("Failed to create indexes, see above for details");
		return false;
	}

	/*
	 * Create an index list file for the table, so that we can easily find
	 * relevant indexing information from the table itself.
	 */
	if (!create_table_index_file(&summary,
								 tableSpecs->indexArray,
								 tableSpecs->tablePaths.idxListFile))
	{
		/* this only means summary is missing some indexing information */
		log_warn("Failed to create table %s index list file \"%s\"",
				 qname,
				 tableSpecs->tablePaths.idxListFile);
	}

	/*
	 * Once all the indexes have been created in parallel, then we can create
	 * the constraints that are associated with the indexes. The ALTER TABLE
	 * commands are taking an exclusive lock on the table, so it's better to
	 * just run the commands serially (one after the other).
	 */
	if (!copydb_create_constraints(tableSpecs))
	{
		log_error("Failed to create constraints, see above for details");
		return false;
	}

	/* finally, vacuum analyze the table and its indexes */
	char vacuum[BUFSIZE] = { 0 };

	sformat(vacuum, sizeof(vacuum), "VACUUM ANALYZE %s", qname);

	log_info("%s;", vacuum);

	if (!pgsql_execute(&dst, vacuum))
	{
		/* errors have already been logged */
		return false;
	}

	/* now we're done */
	return true;
}


/*
 * copydb_start_create_indexes creates all the indexes for a given table in
 * parallel, using a sub-process to send each index command.
 */
bool
copydb_start_create_indexes(CopyTableDataSpec *tableSpecs)
{
	/* At the moment we disregard the jobs limitation */
	int jobs = tableSpecs->indexJobs;

	(void) jobs;                /* TODO */

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
	PGSQL pgconn = { 0 };

	/* First, write the lockFile, with a summary of what's going-on */
	CopyIndexSummary summary = {
		.pid = getpid(),
		.index = index,
	};

	sformat(summary.command, sizeof(summary.command), "%s;", index->indexDef);

	if (!open_index_summary(&summary, indexPaths->lockFile))
	{
		log_info("Failed to create the lock file at \"%s\"",
				 indexPaths->lockFile);
		return false;
	}

	/* now grab an index semaphore lock */
	(void) semaphore_lock(tableSpecs->indexSemaphore);

	if (!pgsql_init(&pgconn, (char *) pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	log_info("%s;", index->indexDef);

	if (!pgsql_execute(&pgconn, index->indexDef))
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
	SourceIndexArray *indexArray = tableSpecs->indexArray;

	const char *pguri = tableSpecs->target_pguri;
	PGSQL pgconn = { 0 };

	if (!pgsql_init(&pgconn, (char *) pguri, PGSQL_CONN_TARGET))
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

			if (!pgsql_execute(&pgconn, sql))
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

	return true;
}
