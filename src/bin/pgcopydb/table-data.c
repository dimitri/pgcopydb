/*
 * src/bin/pgcopydb/table-data.c
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
	int errors = 0;

	if (specs->dirState.tableCopyIsDone &&
		specs->dirState.indexCopyIsDone &&
		specs->dirState.sequenceCopyIsDone &&
		specs->section != DATA_SECTION_CONSTRAINTS)
	{
		log_info("Skipping tables, indexes, and sequences, "
				 "already done on a previous run");
		return true;
	}

	/* now fetch the list of tables from the source database */
	if (!copydb_prepare_table_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now we have tableArray.count tables to migrate and we want to use
	 * specs->tableJobs sub-processes to work on those migrations. Start the
	 * processes, each sub-process walks through the array and pick the first
	 * table that's not being processed already, until all has been done.
	 */
	if (!copydb_process_table_data(specs))
	{
		log_fatal("Failed to COPY the data, see above for details");
		return false;
	}

	if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
	{
		int signal = get_current_signal(SIGTERM);
		const char *signalStr = signal_to_string(signal);

		log_warn("Received signal %s, terminating", signalStr);

		/* ensure we return false, signaling something unexpected happened */
		++errors;
		goto terminate;
	}

	/* Now write that we successfully finished copying all indexes */
	if (!write_file("", 0, specs->cfPaths.done.indexes))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.indexes);
	}

terminate:

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
 * copydb_prepare_table_data fetches the list of tables to COPY data from the
 * source and into the target, and initialises our internal
 * CopyTableDataSpecsArray to drive the operations.
 */
bool
copydb_prepare_table_specs(CopyDataSpec *specs)
{
	SourceTableArray tableArray = { 0, NULL };
	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

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
		CopyTableDataSpec *tableSpecs = &(tableSpecsArray->array[tableIndex]);

		/*
		 * The CopyTableDataSpec structure has its own memory area for the
		 * SourceTable entry, which is copied by the following function. This
		 * means that 'SourceTableArray tableArray' is actually local memory.
		 */
		if (!copydb_init_table_specs(tableSpecs, specs, source))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* free our temporary memory that's been malloc'ed */
	free(tableArray.array);

	return true;
}


/*
 * copydb_process_table_data forks() as many as specs->tableJobs processes that
 * will all concurrently process TABLE DATA and then CREATE INDEX and then also
 * VACUUM ANALYZE each table.
 */
bool
copydb_process_table_data(CopyDataSpec *specs)
{
	int errors = 0;

	/*
	 * Are blobs table data? well pg_dump --section sayth yes.
	 */
	if (!copydb_start_blob_process(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now create as many sub-process as needed, per --table-jobs.
	 */
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
				if (!copydb_process_table_data_worker(specs))
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
	 * Now is a good time to reset sequences: we're waiting for the TABLE DATA
	 * sections and the CREATE INDEX, CONSTRAINTS and VACUUM ANALYZE to be done
	 * with. Sequences can be reset to their expected values while the COPY are
	 * still running, as COPY won't drain identifiers from the sequences
	 * anyway.
	 */
	if (!copydb_copy_all_sequences(specs))
	{
		/* errors have already been logged */
		++errors;
	}

	bool allDone = false;

	while (!allDone)
	{
		if (!copydb_collect_finished_subprocesses(&allDone))
		{
			/* errors have already been logged */
			(void) copydb_fatal_exit();

			return false;
		}

		pg_usleep(100 * 1000); /* 100 ms */
	}

	/* and write that we successfully finished copying all tables */
	if (!write_file("", 0, specs->cfPaths.done.tables))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.tables);
	}

	return errors == 0;
}


/*
 * copydb_process_table_data_worker stats a sub-process that walks through the
 * array of tables to COPY over from the source database to the target
 * database.
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
copydb_process_table_data_worker(CopyDataSpec *specs)
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

		/* reuse the same connection to the source database */
		tableSpecs->sourceSnapshot = specs->sourceSnapshot;

		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			int signal = get_current_signal(SIGTERM);
			const char *signalStr = signal_to_string(signal);

			log_debug("Received signal %s, terminating", signalStr);
			break;
		}

		bool isDone = false;
		bool isBeingProcessed = false;

		if (!copydb_table_is_being_processed(specs,
											 tableSpecs,
											 &isDone,
											 &isBeingProcessed))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * 1. Now COPY the TABLE DATA from the source to the destination.
		 */
		if (!isDone && !isBeingProcessed)
		{
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
		}

		/*
		 * 2. Fetch the list of indexes and constraints attached to this table
		 *    and create them in a background process.
		 */
		if (specs->dirState.indexCopyIsDone &&
			specs->section != DATA_SECTION_CONSTRAINTS)
		{
			log_info("Skipping indexes, already done on a previous run");
		}
		else if (!isDone && !isBeingProcessed)
		{
			if (!copydb_copy_table_indexes(tableSpecs))
			{
				log_warn("Failed to create all the indexes for %s, "
						 "see above for details",
						 tableSpecs->qname);
				log_warn("Consider `pgcopydb copy indexes` to try again");
				++errors;
			}
		}

		/*
		 * 3. Now start the VACUUM ANALYZE parts of the processing, in a
		 *    concurrent sub-process. The sub-process is running in parallel to
		 *    the CREATE INDEX and constraints processes.
		 */
		if (!isDone && !isBeingProcessed)
		{
			if (!copydb_start_vacuum_table(tableSpecs))
			{
				log_warn("Failed to VACUUM ANALYZE %s", tableSpecs->qname);
				++errors;
			}
		}

		/*
		 * 4. Opportunistically see if some CREATE INDEX processed have
		 *    finished already.
		 */
		bool allDone = false;

		if (!copydb_collect_finished_subprocesses(&allDone))
		{
			/* errors have already been logged */
			++errors;
		}
	}

	/* terminate our connection to the source database now */
	(void) copydb_close_snapshot(&(specs->sourceSnapshot));

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
								bool *isDone,
								bool *isBeingProcessed)
{
	if (specs->dirState.tableCopyIsDone)
	{
		log_info("Skipping table %s, already done on a previous run",
				 tableSpecs->qname);

		*isDone = true;
		*isBeingProcessed = false;
		return true;
	}

	/* enter the critical section */
	(void) semaphore_lock(&(specs->tableSemaphore));

	/*
	 * If the doneFile exists, then the table has been processed already,
	 * skip it.
	 *
	 * If the lockFile exists, then the table is currently being processed
	 * by another worker process, skip it.
	 */
	if (file_exists(tableSpecs->tablePaths.doneFile))
	{
		*isDone = true;
		*isBeingProcessed = false;
		(void) semaphore_unlock(&(specs->tableSemaphore));

		return true;
	}

	/* okay so it's not done yet */
	*isDone = false;

	if (file_exists(tableSpecs->tablePaths.lockFile))
	{
		/*
		 * Now it could be that the lockFile still exists and has been created
		 * on a previous run, in which case the pid in there would be a stale
		 * pid.
		 *
		 * So check for that situation before returning with the happy path.
		 */
		CopyTableSummary tableSummary = { .table = &(tableSpecs->sourceTable) };

		if (!read_table_summary(&tableSummary, tableSpecs->tablePaths.lockFile))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(specs->tableSemaphore));

			return false;
		}

		/* if we can signal the pid, it is still running */
		if (kill(tableSummary.pid, 0) == 0)
		{
			*isBeingProcessed = true;
			(void) semaphore_unlock(&(specs->tableSemaphore));

			log_debug("Skipping table %s processed by concurrent worker %d",
					  tableSpecs->qname, tableSummary.pid);

			return true;
		}
		else
		{
			log_warn("Found stale pid %d in file \"%s\", removing it "
					 "and processing table %s",
					 tableSummary.pid,
					 tableSpecs->tablePaths.lockFile,
					 tableSpecs->qname);

			/* stale pid, remove the old lockFile now, then process the table */
			if (!unlink_file(tableSpecs->tablePaths.lockFile))
			{
				log_error("Failed to remove the lockFile \"%s\"",
						  tableSpecs->tablePaths.lockFile);
				(void) semaphore_unlock(&(specs->tableSemaphore));
				return false;
			}

			/* pass through to the rest of this function */
		}
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
	summary->table = &(tableSpecs->sourceTable);

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
	/* COPY the data from the source table to the target table */
	if (tableSpecs->section != DATA_SECTION_TABLE_DATA &&
		tableSpecs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping table data in section %d", tableSpecs->section);
		return true;
	}

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

	/* Now copy the data from source to target */
	log_info("%s", summary->command);

	/* when using `pgcopydb copy table-data`, we don't truncate */
	bool truncate = tableSpecs->section != DATA_SECTION_TABLE_DATA;

	if (!pg_copy(src, &dst, tableSpecs->qname, tableSpecs->qname, truncate))
	{
		/* errors have already been logged */
		return false;
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
		tableSpecs->section != DATA_SECTION_CONSTRAINTS &&
		tableSpecs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping index creation in section %d", tableSpecs->section);
		return true;
	}

	SourceIndexArray indexArray = { 0 };

	tableSpecs->indexArray = &indexArray;

	if (!schema_list_table_indexes(&(tableSpecs->sourceSnapshot.pgsql),
								   tableSpecs->sourceTable.nspname,
								   tableSpecs->sourceTable.relname,
								   tableSpecs->indexArray))
	{
		/* errors have already been logged */
		return false;
	}

	/* build the index file paths we need for the upcoming operations */
	if (!copydb_init_indexes_paths(tableSpecs->cfPaths,
								   tableSpecs->indexArray,
								   &(tableSpecs->indexPathsArray)))
	{
		/* errors have already been logged */
		return false;
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
			if (!copydb_create_table_indexes(tableSpecs))
			{
				log_error("Failed to create indexes, see above for details");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/*
			 * When done as part of the full copy, we also create each index's
			 * constraint as soon as the parallel index built is done.
			 */
			if (tableSpecs->section == DATA_SECTION_ALL ||
				tableSpecs->section == DATA_SECTION_CONSTRAINTS)
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
 * copydb_create_indexes creates all the indexes for a given table in parallel,
 * using a sub-process to send each index command.
 */
bool
copydb_create_table_indexes(CopyTableDataSpec *tableSpecs)
{
	SourceTable *sourceTable = &(tableSpecs->sourceTable);
	SourceIndexArray *indexArray = tableSpecs->indexArray;
	IndexFilePathsArray *indexPathsArray = &(tableSpecs->indexPathsArray);

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
				SourceIndex *index = &(indexArray->array[i]);
				IndexFilePaths *indexPaths = &(indexPathsArray->array[i]);

				/*
				 * Add IF NOT EXISTS clause when the --resume option has been
				 * used, or when the command is `pgcopydb copy indexes`, in
				 * which cases we don't know what to expect on the target
				 * database.
				 */
				bool ifNotExists =
					tableSpecs->resume ||
					tableSpecs->section == DATA_SECTION_INDEXES;

				/* by design, we don't have same-index concurrency */
				Semaphore *lockFileSemaphore = NULL;

				/* child process runs the command */
				if (!copydb_create_index(tableSpecs->target_pguri,
										 index,
										 indexPaths,
										 lockFileSemaphore,
										 tableSpecs->indexSemaphore,
										 false, /* constraint */
										 ifNotExists))
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

	/* also set our GUC values for the target connection */
	if (!pgsql_set_gucs(&dst, dstSettings))
	{
		log_fatal("Failed to set our GUC settings on the target connection, "
				  "see above for details");
		return false;
	}

	/*
	 * Postgres doesn't implement ALTER TABLE ... ADD CONSTRAINT ... IF NOT
	 * EXISTS, which we would be using here in some cases otherwise.
	 *
	 * When --resume is used, for instance, the previous run could have been
	 * interrupted after a constraint creation on the target database, but
	 * before the creation of its constraintDoneFile.
	 */
	SourceIndexArray dstIndexArray = { 0, NULL };

	if (!schema_list_table_indexes(&dst,
								   tableSpecs->sourceTable.nspname,
								   tableSpecs->sourceTable.relname,
								   &dstIndexArray))
	{
		/* errors have already been logged */
		return false;
	}

	if (dstIndexArray.count > 0)
	{
		log_info("Found %d/%d indexes on target database for table %s",
				 dstIndexArray.count,
				 indexArray->count,
				 tableSpecs->qname);
	}

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);
		IndexFilePaths *indexPaths = &(tableSpecs->indexPathsArray.array[i]);

		/* some indexes are not attached to a constraint at all */
		if (index->constraintOid <= 0 ||
			IS_EMPTY_STRING_BUFFER(index->constraintName))
		{
			continue;
		}

		/* First, write the lockFile, with a summary of what's going-on */
		CopyIndexSummary summary = {
			.pid = getpid(),
			.index = index,
			.command = { 0 }
		};

		/* we only install constraints in this part of the code */
		bool constraint = true;
		char *lockFile = indexPaths->constraintLockFile;

		if (!open_index_summary(&summary, lockFile, constraint))
		{
			log_info("Failed to create the lock file at \"%s\"", lockFile);
			continue;
		}

		/* skip constraints that already exist on the target database */
		bool foundConstraintOnTarget = false;

		for (int dstI = 0; dstI < dstIndexArray.count; dstI++)
		{
			SourceIndex *dstIndex = &(dstIndexArray.array[dstI]);

			if (strcmp(index->constraintName, dstIndex->constraintName) == 0)
			{
				foundConstraintOnTarget = true;
				log_info("Found constraint \"%s\" on target, skipping",
						 index->constraintName);
				break;
			}
		}

		if (!copydb_prepare_create_constraint_command(index,
													  summary.command,
													  sizeof(summary.command)))
		{
			log_warn("Failed to prepare SQL command to create constraint \"%s\"",
					 index->constraintName);
			continue;
		}

		if (!foundConstraintOnTarget)
		{
			log_info("%s", summary.command);

			/*
			 * Unique and Primary Key indexes have been built already, in the
			 * other cases the index is built within the ALTER TABLE ... ADD
			 * CONSTRAINT command.
			 */
			bool buildingIndex = !(index->isPrimary || index->isUnique);

			if (!buildingIndex)
			{
				if (!pgsql_execute(&dst, summary.command))
				{
					/* errors have already been logged */
					return false;
				}
			}
			else
			{
				/*
				 * If we're building the index, then we want to acquire the
				 * index semaphore first.
				 */
				Semaphore *createIndexSemaphore = tableSpecs->indexSemaphore;

				(void) semaphore_lock(createIndexSemaphore);

				if (!pgsql_execute(&dst, summary.command))
				{
					/* errors have already been logged */
					(void) semaphore_unlock(createIndexSemaphore);
					return false;
				}

				(void) semaphore_unlock(createIndexSemaphore);
			}
		}

		/*
		 * Create the doneFile for the constraint when we know it exists on the
		 * target database, the main use of this doneFile is to filter out
		 * already existing objects from the pg_restore --section post-data
		 * later.
		 */
		char *doneFile = indexPaths->constraintDoneFile;

		if (!finish_index_summary(&summary, doneFile, constraint))
		{
			log_warn("Failed to create the constraint done file at \"%s\"",
					 doneFile);
			log_warn("Restoring the --post-data part of the schema "
					 "might fail because of already existing objects");
			continue;
		}

		if (!unlink_file(lockFile))
		{
			log_error("Failed to remove the lockFile \"%s\"", lockFile);
			continue;
		}
	}

	/* close connection to the target database now */
	(void) pgsql_finish(&dst);

	/* free malloc'ed memory area */
	free(dstIndexArray.array);

	return errors == 0;
}


/*
 * copydb_start_blob_process starts an auxilliary process that copies the large
 * objects (blobs) from the source database into the target database.
 */
bool
copydb_start_blob_process(CopyDataSpec *specs)
{
	if (specs->skipLargeObjects)
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
			if (!copydb_copy_blobs(specs))
			{
				log_error("Failed to copy large objects, "
						  "see above for details");
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
 * copydb_copy_blobs copies the large objects.
 */
bool
copydb_copy_blobs(CopyDataSpec *specs)
{
	instr_time startTime;

	INSTR_TIME_SET_CURRENT(startTime);

	TransactionSnapshot snapshot = { 0 };

	PGSQL *src = NULL;
	PGSQL dst = { 0 };

	/*
	 * In the context of the `pgcopydb copy blobs` command, we want to re-use
	 * the already prepared snapshot.
	 */
	if (specs->section == DATA_SECTION_BLOBS)
	{
		src = &(specs->sourceSnapshot.pgsql);
	}
	else
	{
		/*
		 * In the context of a full copy command, we want to re-use the already
		 * exported snapshot and make sure to use a private PGSQL client
		 * connection instance.
		 */
		if (!copydb_copy_snapshot(specs, &snapshot))
		{
			/* errors have already been logged */
			return false;
		}

		if (!copydb_set_snapshot(&snapshot))
		{
			/* errors have already been logged */
			return false;
		}

		src = &(snapshot.pgsql);
	}

	if (!pgsql_init(&dst, (char *) specs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* also set our GUC values for the target connection */
	if (!pgsql_set_gucs(&dst, dstSettings))
	{
		log_fatal("Failed to set our GUC settings on the target connection, "
				  "see above for details");
		return false;
	}

	uint32_t count = 0;

	if (!pg_copy_large_objects(src,
							   &dst,
							   specs->restoreOptions.dropIfExists,
							   &count))
	{
		log_error("Failed to copy large objects");
		return false;
	}

	/* if we opened a snapshot, now is the time to close it */
	if (!IS_EMPTY_STRING_BUFFER(snapshot.snapshot))
	{
		if (!copydb_close_snapshot(&snapshot))
		{
			log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
					  snapshot.snapshot,
					  snapshot.pguri);
			return false;
		}
	}

	/* close connection to the target database now */
	(void) pgsql_finish(&dst);

	instr_time duration;

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, startTime);

	/* and write that we successfully finished copying all blobs */
	CopyBlobsSummary summary = {
		.pid = getpid(),
		.count = count,
		.durationMs = INSTR_TIME_GET_MILLISEC(duration)
	};

	/* ignore errors on the blob file summary */
	(void) write_blobs_summary(&summary, specs->cfPaths.done.blobs);

	return true;
}
