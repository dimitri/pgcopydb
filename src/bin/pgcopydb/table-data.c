/*
 * src/bin/pgcopydb/table-data.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

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

	/*
	 * Now that all the sub-processes are done, we can also unlink the table
	 * and index concurrency semaphore, and the vacuum and create index queues.
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

	if (!queue_unlink(&(specs->vacuumQueue)))
	{
		log_warn("Failed to remove VACUUM process queue %d, "
				 "see above for details",
				 specs->vacuumQueue.qId);
	}

	if (!queue_unlink(&(specs->indexQueue)))
	{
		log_warn("Failed to remove CREATE INDEX process queue %d, "
				 "see above for details",
				 specs->indexQueue.qId);
	}

	return errors == 0;
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
	 * Now create as many VACUUM ANALYZE sub-processes as needed, per
	 * --table-jobs. Could be exposed separately as --vacuumJobs too, but
	 * that's not been done at this time.
	 */
	log_trace("copydb_process_table_data: \"%s\"", specs->cfPaths.tbldir);

	/*
	 * Are blobs table data? well pg_dump --section sayth yes.
	 */
	if (!copydb_start_blob_process(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_start_index_workers(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!vacuum_start_workers(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_start_seq_process(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now create as many sub-process as needed, per --table-jobs.
	 */
	if (copydb_process_table_data_with_workers(specs))
	{
		/* write that we successfully finished copying all tables */
		if (!write_file("", 0, specs->cfPaths.done.tables))
		{
			log_warn("Failed to write the tracking file \%s\"",
					 specs->cfPaths.done.tables);
		}
	}
	else
	{
		/* errors have been logged, make sure to send stop messages */
		++errors;
	}

	log_info("COPY phase is done, "
			 "now waiting for vacuum, index, blob, and sequences processes");

	/*
	 * Now that the COPY processes are done, signal this is the end to the
	 * vacuum and CREATE INDEX sub-processes by adding the STOP message to
	 * their queues.
	 */
	if (!vacuum_send_stop(specs))
	{
		/* errors have already been logged */
		++errors;
	}

	if (!copydb_index_workers_send_stop(specs))
	{
		/* errors have already been logged */
		++errors;
	}

	if (!copydb_wait_for_subprocesses())
	{
		log_error("Some sub-processes have exited with error status, "
				  "see above for details");
		++errors;
	}

	if (errors > 0)
	{
		log_error("Errors detected, see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_start_table_data_workers create a supervisor COPY process, and then
 * as sub-process of that supervisor process creates as many sub-processes as
 * needed, per --table-jobs.
 *
 * The supervisor is needed to make this function sync: we can then just wait
 * until all the known sub-processes are done, without having to take into
 * consideration other processes not in the sub-tree.
 */
bool
copydb_process_table_data_with_workers(CopyDataSpec *specs)
{
	log_notice("Now starting %d COPY processes", specs->tableJobs);

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
			log_error("Failed to fork the COPY supervisor process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			log_notice("Started COPY supervisor %d [%d]", getpid(), getppid());

			for (int i = 0; i < specs->tableJobs; i++)
			{
				/*
				 * Flush stdio channels just before fork, to avoid
				 * double-output problems.
				 */
				fflush(stdout);
				fflush(stderr);

				int fpid = fork();

				switch (fpid)
				{
					case -1:
					{
						log_error("Failed to fork a COPY worker process: %m");
						exit(EXIT_CODE_INTERNAL_ERROR);
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

			/* now COPY the extension configuration tables, while waiting */
			int errors = 0;

			if (!specs->skipExtensions)
			{
				bool createExtensions = false;

				if (!copydb_copy_extensions(specs, createExtensions))
				{
					/* errors have already been logged */
					++errors;
				}
			}

			/* the COPY supervisor waits for the COPY workers */
			if (!copydb_wait_for_subprocesses())
			{
				log_error("Some COPY worker process(es) have exited with error, "
						  "see above for details");
				++errors;
			}

			if (errors > 0)
			{
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

	/* wait until the supervisor process exits */
	int status;

	if (waitpid(fpid, &status, 0) != fpid)
	{
		log_error("Failed to wait for COPY supervisor process %d: %m", fpid);
		return false;
	}

	int returnCode = WEXITSTATUS(status);

	if (returnCode != 0)
	{
		log_error("COPY supervisor process exited with return code %d",
				  returnCode);
		return false;
	}

	return true;
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
	int copies = 0;

	log_notice("Started COPY worker %d [%d]", getpid(), getppid());

	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

	/* connect once to the source database for the whole process */
	if (!copydb_set_snapshot(specs))
	{
		/* errors have already been logged */
		return false;
	}

	for (int tableIndex = 0; tableIndex < tableSpecsArray->count; tableIndex++)
	{
		/* initialize our TableDataProcess entry now */
		CopyTableDataSpec *tableSpecs = &(tableSpecsArray->array[tableIndex]);

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
		 * Skip tables that have been entirely done already either on a
		 * previous run, or by a concurrent process while we were busy with our
		 * own work.
		 *
		 * Also skip tables that have been claimed by another of the COPY
		 * worker processes.
		 */
		if (isDone || isBeingProcessed)
		{
			continue;
		}

		/*
		 * 1. Now COPY the TABLE DATA from the source to the destination.
		 */
		bool copySucceeded = true;

		/* check for exclude-table-data filtering */
		if (!tableSpecs->sourceTable->excludeData)
		{
			++copies;

			/*
			 * If we fail to copy a given table, continue looping. Otherwise
			 * pgcopydb just continues processing all tables anyways (we wait
			 * until all the sub-processes are finished, but we don't go and
			 * signal them to stop immediately). We'd better continue with as
			 * many processes as --table-jobs was given.
			 */
			if (!copydb_copy_table(specs, tableSpecs))
			{
				/* errors have already been logged */
				copySucceeded = false;
				++errors;
			}
		}

		/* enter the critical section to communicate that we're done */
		if (copySucceeded)
		{
			if (!copydb_mark_table_as_done(specs, tableSpecs))
			{
				/* errors have already been logged */
				return false;
			}
		}

		/*
		 * 2. Send the indexes and constraints attached to this table to the
		 *    index job queue.
		 *
		 * 3. Send the table to the VACUUM ANALYZE job queue.
		 *
		 * If a partial COPY is happening, check that all the other parts are
		 * done. This check should be done in the critical section too. Only
		 * one process can see all parts as done already, and that's the one
		 * finishing last.
		 */
		bool allPartsDone = false;
		bool indexesAreBeingProcessed = false;

		if (!copydb_table_parts_are_all_done(specs,
											 tableSpecs,
											 &allPartsDone,
											 &indexesAreBeingProcessed))
		{
			/* errors have already been logged */
			++errors;
		}

		if (specs->dirState.indexCopyIsDone &&
			specs->section != DATA_SECTION_CONSTRAINTS)
		{
			log_info("Skipping indexes, already done on a previous run");
		}
		else if (allPartsDone && !indexesAreBeingProcessed)
		{
			if (!copydb_add_table_indexes(specs, tableSpecs))
			{
				log_warn("Failed to add the indexes for %s, "
						 "see above for details",
						 tableSpecs->qname);
				log_warn("Consider `pgcopydb copy indexes` to try again");
				++errors;
			}

			if (!vacuum_add_table(specs, tableSpecs))
			{
				log_warn("Failed to queue VACUUM ANALYZE %s [%u]",
						 tableSpecs->qname,
						 tableSpecs->sourceTable->oid);
				++errors;
			}
		}
	}

	/* terminate our connection to the source database now */
	(void) copydb_close_snapshot(specs);

	log_debug("copydb_process_table_data_worker: done %d copies, %d errors",
			  copies,
			  errors);

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
		log_notice("Skipping table %s, already done on a previous run",
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
		CopyTableSummary tableSummary = { .table = tableSpecs->sourceTable };

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

			log_trace("Skipping table %s processed by concurrent worker %d",
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
		(CopyTableSummary *) calloc(1, sizeof(CopyTableSummary));

	*summary = emptySummary;

	summary->pid = getpid();
	summary->table = tableSpecs->sourceTable;

	if (IS_EMPTY_STRING_BUFFER(tableSpecs->part.copyQuery))
	{
		sformat(summary->command, sizeof(summary->command),
				"COPY %s",
				tableSpecs->qname);
	}
	else
	{
		sformat(summary->command, sizeof(summary->command),
				"COPY %s",
				tableSpecs->part.copyQuery);
	}

	if (!open_table_summary(summary, tableSpecs->tablePaths.lockFile))
	{
		log_info("Failed to create the lock file for table %s at \"%s\"",
				 tableSpecs->qname,
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
		log_error("Failed to create the summary file at \"%s\"",
				  tableSpecs->tablePaths.doneFile);
		(void) semaphore_unlock(&(specs->tableSemaphore));
		return false;
	}

	log_debug("Wrote summary for table %s at \"%s\"",
			  tableSpecs->qname,
			  tableSpecs->tablePaths.doneFile);

	/* end of the critical section */
	(void) semaphore_unlock(&(specs->tableSemaphore));

	return true;
}


/*
 * copydb_table_parts_are_all_done return true when a table COPY is done in a
 * single process, or when a table COPY has been partitionned in several
 * concurrent process and all of them are known to be done.
 */
bool
copydb_table_parts_are_all_done(CopyDataSpec *specs,
								CopyTableDataSpec *tableSpecs,
								bool *allPartsDone,
								bool *isBeingProcessed)
{
	if (tableSpecs->part.partCount <= 1)
	{
		*allPartsDone = true;
		*isBeingProcessed = false;
		return true;
	}

	*allPartsDone = false;

	/* enter the critical section */
	(void) semaphore_lock(&(specs->tableSemaphore));

	/* make sure only one process created the indexes/constraints */
	if (file_exists(tableSpecs->tablePaths.idxListFile))
	{
		*allPartsDone = true;
		*isBeingProcessed = true;

		(void) semaphore_unlock(&(specs->tableSemaphore));
		return true;
	}

	bool allDone = true;

	CopyFilePaths *cfPaths = &(specs->cfPaths);
	uint32_t oid = tableSpecs->sourceTable->oid;

	for (int i = 0; i < tableSpecs->part.partCount; i++)
	{
		TableFilePaths partPaths = { 0 };

		(void) copydb_init_tablepaths_for_part(cfPaths, &partPaths, oid, i);

		if (!file_exists(partPaths.doneFile))
		{
			allDone = false;
			break;
		}
	}

	/* create an empty index list file now, when allDone is still true */
	if (allDone)
	{
		if (!write_file("", 0, tableSpecs->tablePaths.idxListFile))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(specs->tableSemaphore));
			return false;
		}

		*allPartsDone = true;
		*isBeingProcessed = false; /* allow processing of the indexes */

		/* end of the critical section */
		(void) semaphore_unlock(&(specs->tableSemaphore));

		return true;
	}
	else
	{
		/* end of the critical section */
		(void) semaphore_unlock(&(specs->tableSemaphore));

		*allPartsDone = false;
		*isBeingProcessed = false;

		return true;
	}

	/* keep compiler happy, we should never end-up here */
	return true;
}


/*
 * copydb_copy_table implements the sub-process activity to pg_dump |
 * pg_restore the table's data and then create the indexes and the constraints
 * in parallel.
 */
bool
copydb_copy_table(CopyDataSpec *specs, CopyTableDataSpec *tableSpecs)
{
	/* COPY the data from the source table to the target table */
	if (tableSpecs->section != DATA_SECTION_TABLE_DATA &&
		tableSpecs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping table data in section %d", tableSpecs->section);
		return true;
	}

	/* we want to set transaction snapshot to the main one on the source */
	PGSQL *src = &(specs->sourceSnapshot.pgsql);
	PGSQL dst = { 0 };

	CopyTableSummary *summary = tableSpecs->summary;

	/* initialize our connection to the target database */
	if (!pgsql_init(&dst, tableSpecs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* when using `pgcopydb copy table-data`, we don't truncate */
	bool truncate = tableSpecs->section != DATA_SECTION_TABLE_DATA;

	/*
	 * When COPYing a partition, TRUNCATE only when it's the first one. Both
	 * checking of the partition is the first one being processed and the
	 * TRUNCATE operation itself must be protected in a critical section.
	 */
	if (truncate && tableSpecs->part.partCount > 1)
	{
		/*
		 * When partitioning for COPY we can only TRUNCATE once per table, we
		 * avoid doing a TRUNCATE per part. So only the process that reaches
		 * this area first is allowed to TRUNCATE, and it must do so within a
		 * critical section.
		 *
		 * As processes for the other parts of the same source table are
		 * waiting for the TRUNCATE to be done with, we can't do it in the same
		 * transaction as the COPY, and we won't be able to COPY with FREEZE
		 * either.
		 */

		/* enter the critical section */
		(void) semaphore_lock(&(specs->tableSemaphore));

		/* if the truncate done file already exists, it's been done already */
		if (!file_exists(tableSpecs->tablePaths.truncateDoneFile))
		{
			if (!pgsql_truncate(&dst, tableSpecs->qname))
			{
				/* errors have already been logged */
				(void) semaphore_unlock(&(specs->tableSemaphore));
				return false;
			}

			if (!write_file("", 0, tableSpecs->tablePaths.truncateDoneFile))
			{
				/* errors have already been logged */
				(void) semaphore_unlock(&(specs->tableSemaphore));
				return false;
			}
		}

		/* end of the critical section */
		(void) semaphore_unlock(&(specs->tableSemaphore));

		/* now TRUNCATE has been done, refrain from an extra one in pg_copy */
		truncate = false;
	}

	/* Now copy the data from source to target */
	log_notice("%s", summary->command);

	/* COPY FROM tablename, or maybe COPY FROM (SELECT ... WHERE ...) */
	char *copySource = tableSpecs->qname;

	if (tableSpecs->part.partCount > 1)
	{
		copySource = tableSpecs->part.copyQuery;
	}

	int attempts = 0;
	int maxAttempts = 5;        /* allow 5 attempts total, 4 retries */

	bool retry = true;
	bool success = false;

	while (!success && retry)
	{
		++attempts;

		/* ignore previous attempts, we need only one success here */
		success = pg_copy(src, &dst, copySource, tableSpecs->qname, truncate);

		if (success)
		{
			/* success, get out of the retry loop */
			if (attempts > 1)
			{
				log_info("Table %s COPY succeeded after %d attempts",
						 tableSpecs->qname,
						 attempts);
			}
			break;
		}

		/* errors have already been logged */
		retry =
			attempts < maxAttempts &&

			/* retry only on Connection Exception errors */
			(pgsql_state_is_connection_error(src) ||
			 pgsql_state_is_connection_error(&dst));

		if (maxAttempts <= attempts)
		{
			log_error("Failed to copy table %s even after %d attempts, "
					  "see above for details",
					  tableSpecs->qname,
					  attempts);
		}
		else if (retry)
		{
			log_info("Failed to copy table %s (connection exception), "
					 "retrying in %dms (attempt %d)",
					 tableSpecs->qname,
					 POSTGRES_PING_RETRY_CAP_SLEEP_TIME,
					 attempts);
		}

		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			break;
		}

		if (retry)
		{
			/* sleep a couple seconds then retry */
			pg_usleep(POSTGRES_PING_RETRY_CAP_SLEEP_TIME * 1000);
		}
	}

	return success;
}
