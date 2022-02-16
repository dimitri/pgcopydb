/*
 * src/bin/pgcopydb/indexes.c
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


/*
 * copydb_init_index_file_paths prepares a given index (and constraint) file
 * paths to help orchestrate the concurrent operations.
 */
bool
copydb_init_indexes_paths(CopyFilePaths *cfPaths,
						  SourceIndexArray *indexArray,
						  IndexFilePathsArray *indexPathsArray)
{
	indexPathsArray->count = indexArray->count;
	indexPathsArray->array =
		(IndexFilePaths *) malloc(indexArray->count * sizeof(IndexFilePaths));

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);
		IndexFilePaths *indexPaths = &(indexPathsArray->array[i]);

		sformat(indexPaths->lockFile, sizeof(indexPaths->lockFile),
				"%s/%u",
				cfPaths->rundir,
				index->indexOid);

		sformat(indexPaths->doneFile, sizeof(indexPaths->doneFile),
				"%s/%u.done",
				cfPaths->idxdir,
				index->indexOid);

		sformat(indexPaths->constraintLockFile,
				sizeof(indexPaths->constraintLockFile),
				"%s/%u",
				cfPaths->rundir,
				index->constraintOid);

		sformat(indexPaths->constraintDoneFile,
				sizeof(indexPaths->constraintDoneFile),
				"%s/%u.done",
				cfPaths->idxdir,
				index->constraintOid);
	}

	return true;
}


/*
 * copydb_copy_all_indexes fetches the list of indexes from the source database
 * and then create all the same indexes on the target database, which is
 * expected to have the same tables created already.
 *
 * When specs->section is DATA_SECTION_INDEXES then only indexes are created,
 * when specs->section is DATA_SECTION_CONSTRAINTS then only constraints are
 * created.
 */
bool
copydb_copy_all_indexes(CopyDataSpec *specs)
{
	if (specs->dirState.indexCopyIsDone)
	{
		log_info("Skipping indexes, already done on a previous run");
		return true;
	}

	if (specs->section != DATA_SECTION_INDEXES &&
		specs->section != DATA_SECTION_CONSTRAINTS &&
		specs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping indexes in section %d", specs->section);
		return true;
	}

	PGSQL *src = &(specs->sourceSnapshot.pgsql);

	SourceIndexArray indexArray = { 0, NULL };
	IndexFilePathsArray indexPathsArray = { 0, NULL };

	log_info("Listing indexes in source database");

	SourceFilters filters = { 0 };

	if (!schema_list_all_indexes(src, &filters, &indexArray))
	{
		/* errors have already been logged */
		return false;
	}

	/* build the index file paths we need for the upcoming operations */
	if (!copydb_init_indexes_paths(&(specs->cfPaths),
								   &indexArray,
								   &indexPathsArray))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Creating %d indexes in the target database using %d processes",
			 indexArray.count,
			 specs->indexJobs);

	if (!copydb_start_index_processes(specs, &indexArray, &indexPathsArray))
	{
		/* errors have already been logged */
		return false;
	}

	/* free malloc'ed memory area */
	free(indexArray.array);
	free(indexPathsArray.array);

	return true;
}


/*
 * copydb_start_index_processes forks() as many as specs->indexJobs processes
 * that will all concurrently run the CREATE INDEX needed to copy the indexes
 * from the source database to the target database.
 */
bool
copydb_start_index_processes(CopyDataSpec *specs,
							 SourceIndexArray *indexArray,
							 IndexFilePathsArray *indexPathsArray)
{
	Semaphore lockFileSemaphore = { 0 };

	lockFileSemaphore.initValue = 1;

	if (!semaphore_create(&lockFileSemaphore))
	{
		log_error("Failed to create the same-index concurrency semaphore "
				  "to orchestrate %d CREATE INDEX jobs",
				  specs->indexJobs);
		return false;
	}

	for (int i = 0; i < specs->indexJobs; i++)
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
				if (!copydb_start_index_process(specs,
												indexArray,
												indexPathsArray,
												&lockFileSemaphore))
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

	bool success = copydb_wait_for_subprocesses();

	/* and write that we successfully finished copying all tables */
	if (!write_file("", 0, specs->cfPaths.done.indexes))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.indexes);
	}

	if (!semaphore_finish(&lockFileSemaphore))
	{
		log_warn("Failed to remove same-index concurrency semaphore %d, "
				 "see above for details",
				 lockFileSemaphore.semId);
	}

	return success;
}


/*
 * copydb_start_index_process stats a sub-process that walks through the array
 * of indexes to copy over from the source database to the target database.
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
copydb_start_index_process(CopyDataSpec *specs,
						   SourceIndexArray *indexArray,
						   IndexFilePathsArray *indexPathsArray,
						   Semaphore *lockFileSemaphore)
{
	int errors = 0;
	bool constraint = specs->section == DATA_SECTION_CONSTRAINTS;

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);
		IndexFilePaths *indexPaths = &(indexPathsArray->array[i]);

		bool ifNotExists = true;

		if (!copydb_create_index(specs->target_pguri,
								 index,
								 indexPaths,
								 lockFileSemaphore,
								 &(specs->indexSemaphore),
								 constraint,
								 ifNotExists))
		{
			/* errors have already been logged */
			++errors;
			continue;
		}
	}

	if (!copydb_wait_for_subprocesses())
	{
		/* errors have already been logged */
		++errors;
	}

	return errors == 0;
}


/*
 * copydb_create_indexes creates all the indexes for a given table in
 * parallel, using a sub-process to send each index command.
 *
 * This function uses two distinct semaphores:
 *
 * - the lockFileSemaphore allows multiple worker process to lock around the
 *   choice of the next index to process, guaranteeing that any single index is
 *   processed by only one worker: same-index concurrency.
 *
 * - the createIndexSemaphore should be initialized with indexJobs as its
 *   initValue to enable creating up to that number of indexes at the same time
 *   on the target system.
 */
bool
copydb_create_index(const char *pguri,
					SourceIndex *index,
					IndexFilePaths *indexPaths,
					Semaphore *lockFileSemaphore,
					Semaphore *createIndexSemaphore,
					bool constraint,
					bool ifNotExists)
{
	PGSQL dst = { 0 };

	/* First, write the lockFile, with a summary of what's going-on */
	CopyIndexSummary summary = {
		.pid = getpid(),
		.index = index,
		.command = { 0 }
	};

	bool isDone = false;
	bool isBeingProcessed = false;

	bool isConstraintIndex = index->constraintOid != 0;

	/*
	 * When asked to create the constraint and there is no constraint attached
	 * to this index, skip the operation entirely.
	 */
	if (constraint &&
		(index->constraintOid <= 0 ||
		 IS_EMPTY_STRING_BUFFER(index->constraintName)))
	{
		return true;
	}

	/*
	 * When asked to create an index for a constraint and the index is neither
	 * a UNIQUE nor a PRIMARY KEY index, then we can't use the ALTER TABLE ...
	 * ADD CONSTRAINT ... USING INDEX ... command, because this only works with
	 * UNIQUE and PRIMARY KEY indexes.
	 *
	 * This means that we have to skip creating the index first, and will only
	 * then create it during the constraint phase, as part of the "plain" ALTER
	 * TABLE ... ADD CONSTRAINT ... command.
	 */
	else if (isConstraintIndex && !index->isPrimary && !index->isUnique)
	{
		log_warn("Skipping concurrent build of index "
				 "\"%s\" for constraint %s on \"%s\".\"%s\", "
				 "it is not a UNIQUE or a PRIMARY constraint",
				 index->indexRelname,
				 index->constraintDef,
				 index->tableNamespace,
				 index->tableRelname);
		return true;
	}

	/* deal with same-index concurrency if we have to */
	if (!copydb_index_is_being_processed(index,
										 indexPaths,
										 constraint,
										 lockFileSemaphore,
										 &summary,
										 &isDone,
										 &isBeingProcessed))
	{
		/* errors have already been logged */
		return false;
	}

	if (isDone || isBeingProcessed)
	{
		log_debug("Skipping index %s which is being created by another process",
				  index->indexRelname);
		return true;
	}

	/* now grab an index semaphore lock if we have one */
	(void) semaphore_lock(createIndexSemaphore);

	/* prepare the create index command, maybe adding IF NOT EXISTS */
	if (constraint)
	{
		if (!copydb_prepare_create_constraint_command(index,
													  summary.command,
													  sizeof(summary.command)))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		if (!copydb_prepare_create_index_command(index,
												 ifNotExists,
												 summary.command,
												 sizeof(summary.command)))
		{
			/* errors have already been logged */
			return false;
		}
	}

	log_info("%s", summary.command);

	if (!pgsql_init(&dst, (char *) pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(createIndexSemaphore);
		return false;
	}

	/* also set our GUC values for the target connection */
	if (!pgsql_set_gucs(&dst, dstSettings))
	{
		log_fatal("Failed to set our GUC settings on the target connection, "
				  "see above for details");
		return false;
	}

	if (!pgsql_execute(&dst, summary.command))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(createIndexSemaphore);
		return false;
	}

	(void) pgsql_finish(&dst);

	/* the CREATE INDEX command is done, release our lock */
	(void) semaphore_unlock(createIndexSemaphore);

	if (!copydb_mark_index_as_done(index,
								   indexPaths,
								   constraint,
								   lockFileSemaphore,
								   &summary))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_index_is_being_processed checks lock and done files to see if a given
 * index is already being processed, or has been processed entirely by another
 * process. In which case the index is to be skipped by the current process.
 */
bool
copydb_index_is_being_processed(SourceIndex *index,
								IndexFilePaths *indexPaths,
								bool constraint,
								Semaphore *lockFileSemaphore,
								CopyIndexSummary *summary,
								bool *isDone,
								bool *isBeingProcessed)
{
	char *lockFile =
		constraint ? indexPaths->constraintLockFile : indexPaths->lockFile;

	char *doneFile =
		constraint ? indexPaths->constraintDoneFile : indexPaths->doneFile;

	/* some callers have no same-index concurrency, just create the lockFile */
	if (lockFileSemaphore == NULL)
	{
		if (!open_index_summary(summary, lockFile, constraint))
		{
			log_info("Failed to create the lock file at \"%s\"", lockFile);
			(void) semaphore_unlock(lockFileSemaphore);
			return false;
		}

		*isDone = false;
		*isBeingProcessed = false;

		return true;
	}

	/* enter the critical section */
	(void) semaphore_lock(lockFileSemaphore);

	if (file_exists(doneFile))
	{
		*isDone = true;
		*isBeingProcessed = false;
		(void) semaphore_unlock(lockFileSemaphore);

		return true;
	}

	/* okay so it's not done yet */
	*isDone = false;

	/* check if the lockFile has already been claimed for this index */
	if (file_exists(lockFile))
	{
		CopyIndexSummary indexSummary = { .index = index };

		if (!read_index_summary(&indexSummary, lockFile))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(lockFileSemaphore);
			return false;
		}

		/* if we can signal the pid, it is still running */
		if (kill(indexSummary.pid, 0) == 0)
		{
			*isBeingProcessed = true;
			(void) semaphore_unlock(lockFileSemaphore);

			log_debug("Skipping index %s processed by concurrent worker %d",
					  index->indexRelname, indexSummary.pid);

			return true;
		}
		else
		{
			log_warn("Found stale pid %d in file \"%s\", removing it "
					 "and creating index %s",
					 indexSummary.pid,
					 lockFile,
					 index->indexRelname);

			/* stale pid, remove the old lockFile now, then process index */
			if (!unlink_file(lockFile))
			{
				log_error("Failed to remove the lockFile \"%s\"", lockFile);
				(void) semaphore_unlock(lockFileSemaphore);
				return false;
			}
		}
	}

	/*
	 * Otherwise, the index is not being processed yet.
	 */
	*isBeingProcessed = false;

	if (!open_index_summary(summary, lockFile, constraint))
	{
		log_info("Failed to create the lock file at \"%s\"", lockFile);
		(void) semaphore_unlock(lockFileSemaphore);
		return false;
	}

	/* end of the critical section */
	(void) semaphore_unlock(lockFileSemaphore);

	return true;
}


/*
 * copydb_mark_index_as_done creates the table doneFile with the expected
 * summary content. To create a doneFile we must acquire the synchronisation
 * semaphore first. The lockFile is also removed here.
 */
bool
copydb_mark_index_as_done(SourceIndex *index,
						  IndexFilePaths *indexPaths,
						  bool constraint,
						  Semaphore *lockFileSemaphore,
						  CopyIndexSummary *summary)
{
	char *lockFile =
		constraint ? indexPaths->constraintLockFile : indexPaths->lockFile;

	char *doneFile =
		constraint ? indexPaths->constraintDoneFile : indexPaths->doneFile;

	if (lockFileSemaphore != NULL)
	{
		(void) semaphore_lock(lockFileSemaphore);
	}

	/* create the doneFile for the index */
	if (!finish_index_summary(summary, doneFile, constraint))
	{
		log_info("Failed to create the summary file at \"%s\"", doneFile);

		if (lockFileSemaphore != NULL)
		{
			(void) semaphore_unlock(lockFileSemaphore);
		}
		return false;
	}

	/* also remove the lockFile, we don't need it anymore */
	if (!unlink_file(lockFile))
	{
		log_error("Failed to remove the lockFile \"%s\"", lockFile);
		if (lockFileSemaphore != NULL)
		{
			(void) semaphore_unlock(lockFileSemaphore);
		}
		return false;
	}

	if (lockFileSemaphore != NULL)
	{
		(void) semaphore_unlock(lockFileSemaphore);
	}

	return true;
}


/*
 * copydb_prepare_create_index_command prepares the SQL command to use to
 * create a given index. When ifNotExists is true the IF NOT EXISTS keywords
 * are added to the command, necessary to resume operations in some cases.
 */
bool
copydb_prepare_create_index_command(SourceIndex *index,
									bool ifNotExists,
									char *command,
									size_t size)
{
	/* prepare the create index command, maybe adding IF NOT EXISTS */
	if (ifNotExists)
	{
		int ci_len = strlen("CREATE INDEX ");
		int cu_len = strlen("CREATE UNIQUE INDEX ");

		if (strncmp(index->indexDef, "CREATE INDEX ", ci_len) == 0)
		{
			sformat(command, size, "CREATE INDEX IF NOT EXISTS %s;",
					index->indexDef + ci_len);
		}
		else if (strncmp(index->indexDef, "CREATE UNIQUE INDEX ", cu_len) == 0)
		{
			sformat(command, size, "CREATE UNIQUE INDEX IF NOT EXISTS %s;",
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
		sformat(command, size, "%s;", index->indexDef);
	}

	return true;
}


/*
 * copydb_prepare_create_constraint_command prepares the SQL command to use to
 * create the given constraint on-top of an already existing Index.
 */
bool
copydb_prepare_create_constraint_command(SourceIndex *index,
										 char *command,
										 size_t size)
{
	if (index->isPrimary || index->isUnique)
	{
		char *constraintType = index->isPrimary ? "PRIMARY KEY" : "UNIQUE";

		sformat(command, size,
				"ALTER TABLE \"%s\".\"%s\" "
				"ADD CONSTRAINT \"%s\" %s "
				"USING INDEX \"%s\";",
				index->tableNamespace,
				index->tableRelname,
				index->constraintName,
				constraintType,
				index->indexRelname);
	}
	else
	{
		sformat(command, size,
				"ALTER TABLE \"%s\".\"%s\" "
				"ADD CONSTRAINT \"%s\" %s ",
				index->tableNamespace,
				index->tableRelname,
				index->constraintName,
				index->constraintDef);
	}

	return true;
}
