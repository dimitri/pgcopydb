/*
 * src/bin/pgcopydb/table-data.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "postgres_fe.h"
#include "libpq-fe.h"
#include "pqexpbuffer.h"

#include "catalog.h"
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


static bool copydb_copy_supervisor_add_table_hook(void *context,
												  SourceTable *table);

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
		return false;
	}

	/* Now write that we successfully finished copying all indexes */
	if (!write_file("", 0, specs->cfPaths.done.indexes))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.indexes);
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

	log_trace("copydb_process_table_data: \"%s\"", specs->cfPaths.tbldir);

	/* close SQLite databases before fork() */
	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Take care of extensions configuration table in an auxilliary process.
	 */
	if (!copydb_start_extension_data_process(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * When we have fetch information for zero table then specs->tableJobs is
	 * zero too and we won't send any STOP message in the index and vacuum
	 * queues.
	 *
	 * That
	 */
	if (specs->tableJobs > 0)
	{
		/*
		 * First start the COPY data workers with their supervisor and IPC
		 * infrastructure (queues).
		 */
		if (!copydb_start_copy_supervisor(specs))
		{
			/* errors have already been logged */
			++errors;
		}

		/*
		 * Start as many index worker process as --index-jobs
		 */
		if (errors == 0 && !copydb_start_index_workers(specs))
		{
			/* errors have already been logged */
			++errors;
		}

		/*
		 * Now create as many VACUUM ANALYZE sub-processes as needed, per
		 * --table-jobs. Could be exposed separately as --vacuumJobs too, but
		 * that's not been done at this time.
		 */
		if (errors == 0 && !vacuum_start_workers(specs))
		{
			/* errors have already been logged */
			++errors;
		}
	}
	else
	{
		log_info("STEP 4: skipping COPY, no table selected");
		log_info("STEP 6: skipping CREATE INDEX, no table selected");
		log_info("STEP 7: skipping contraints, no table selected");
		log_info("STEP 8: skipping VACUUM, no table selected");
	}

	/*
	 * Are blobs table data? well pg_dump --section sayth yes.
	 */
	if (errors == 0 && !copydb_start_blob_process(specs))
	{
		/* errors have already been logged */
		++errors;
	}

	/*
	 * Start an auxilliary process to reset sequences on the target database.
	 */
	if (errors == 0 && !copydb_start_seq_process(specs))
	{
		/* errors have already been logged */
		++errors;
	}

	/* when errors happened, signal all processes to terminate now */
	if (errors > 0)
	{
		log_error("Failed to start some worker processes, "
				  "see above for details");

		/* send TERM signal to all the process in our process group */
		if (!kill(0, SIGTERM))
		{
			log_error("Failed to send TERM signal our process group");

			/* refrain from early return here, we want to waitpid() */
			++errors;
		}
	}

	if (!copydb_wait_for_subprocesses(specs->failFast))
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
 * copydb_start_copy_supervisor starts a COPY supervisor process, which job is
 * to create the copy data workers and then loop through the table partitions
 * queue (when needed) to drive adding the table indexes to the index queue
 * when all the partitions are done.
 */
bool
copydb_start_copy_supervisor(CopyDataSpec *specs)
{
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
			log_error("Failed to fork copy supervisor process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			(void) set_ps_title("pgcopydb: copy supervisor");

			if (!copydb_copy_supervisor(specs))
			{
				log_error("Failed to copy table data, see above for details");
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
 * copydb_copy_supervisor creates the copyQueue and if needed the
 * copyDoneQueue too, then starts --table-jobs COPY table data workers to
 * process table oids from the queue.
 */
bool
copydb_copy_supervisor(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started COPY supervisor %d [%d]", pid, getppid());

	if (!queue_create(&(specs->copyQueue), "copy table-data"))
	{
		log_error("Failed to create the COPY process queue");
		return false;
	}

	/*
	 * Start COPY table-data workers, as many as --table-jobs.
	 */
	if (!copydb_start_table_data_workers(specs))
	{
		log_fatal("Failed to start table data COPY workers, "
				  "see above for details");

		(void) copydb_fatal_exit();

		return false;
	}

	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to open internal catalogs in COPY supervisor, "
				  "see above for details");
		return false;
	}

	/*
	 * Now fill-in the COPY data queue with the table OIDs / part number.
	 */
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!catalog_iter_s_table(sourceDB,
							  specs,
							  copydb_copy_supervisor_add_table_hook))
	{
		log_fatal("Failed to add tables to the COPY worker queue, terminating");
		(void) copydb_fatal_exit();
		return false;
	}

	if (!catalog_close_from_specs(specs))
	{
		log_error("Failed to cloes internal catalogs in COPY supervisor, "
				  "see above for details");
		return false;
	}

	/*
	 * Add the STOP messages to the queue now, one STOP message per worker.
	 */
	if (!copydb_copy_supervisor_send_stop(specs))
	{
		log_fatal("Failed to send STOP messages to the COPY queue");

		/* we still need to make sure the COPY processes terminate */
		(void) copydb_fatal_exit();

		return false;
	}

	/*
	 * Now just wait for the table-data COPY processes to be done.
	 */
	if (!copydb_wait_for_subprocesses(specs->failFast))
	{
		log_error("Some COPY worker process(es) have exited with error, "
				  "see above for details");

		/* make sure vacuum and create index processes see a STOP message */
		if (!vacuum_send_stop(specs) ||
			!copydb_index_workers_send_stop(specs))
		{
			(void) copydb_fatal_exit();
		}

		return false;
	}

	/* write that we successfully finished copying all tables */
	if (!write_file("", 0, specs->cfPaths.done.tables))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.tables);
	}

	bool success = true;

	/*
	 * Now that the COPY processes are done, signal this is the end to the
	 * vacuum and CREATE INDEX sub-processes by adding the STOP message to
	 * their queues.
	 */
	success = success && vacuum_send_stop(specs);
	success = success && copydb_index_workers_send_stop(specs);

	if (!success)
	{
		/*
		 * The other subprocesses need to see a STOP message to stop their
		 * processing. Failing to send the STOP messages means that the main
		 * pgcopydb never finishes, and we want to ensure the command
		 * terminates.
		 */
		(void) copydb_fatal_exit();

		return false;
	}

	return true;
}


/*
 * copydb_copy_supervisor_add_table_hook is an iterator callback function.
 */
static bool
copydb_copy_supervisor_add_table_hook(void *context, SourceTable *table)
{
	CopyDataSpec *specs = (CopyDataSpec *) context;

	if (table->partition.partCount == 0)
	{
		if (!copydb_add_copy(specs, table->oid, 0))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		/*
		 * Add as many times the table OID as we have partitions, each with
		 * their own partition number that starts at 1 (not zero).
		 */
		for (int i = 0; i < table->partition.partCount; i++)
		{
			int partNumber = i + 1;

			if (!copydb_add_copy(specs, table->oid, partNumber))
			{
				/* errors have already been logged */
				return false;
			}
		}
	}

	return true;
}


/*
 * copydb_copy_supervisor_send_stop sends the STOP messages to the copy queue,
 * one STOP message per worker.
 */
bool
copydb_copy_supervisor_send_stop(CopyDataSpec *specs)
{
	for (int i = 0; i < specs->tableJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP };

		if (!queue_send(&(specs->copyQueue), &stop))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_add_copy sends a message to the COPY queue to process a given table,
 * or a given table partition.
 */
bool
copydb_add_copy(CopyDataSpec *specs, uint32_t oid, uint32_t part)
{
	QMessage mesg = {
		.type = QMSG_TYPE_TABLEPOID,
		.data.tp = { .oid = oid, .part = part }
	};

	if (!queue_send(&(specs->copyQueue), &mesg))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_start_table_data_workers create as many sub-process as needed, per
 * --table-jobs.
 */
bool
copydb_start_table_data_workers(CopyDataSpec *specs)
{
	log_info("STEP 4: starting %d table data COPY processes", specs->tableJobs);

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
				return false;
			}

			case 0:
			{
				/* child process runs the command */
				(void) set_ps_title("pgcopydb: copy worker");

				if (!copydb_table_data_worker(specs))
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

	return true;
}


/*
 * copydb_table_data_worker is a worker process that loops over messages
 * received from a queue, each message being the Oid of an index to create on
 * the target database.
 */
bool
copydb_table_data_worker(CopyDataSpec *specs)
{
	uint64_t errors = 0;
	pid_t pid = getpid();

	log_notice("Started table data COPY worker %d [%d]", pid, getppid());

	/* connect once to the source database for the whole process */
	if (!copydb_set_snapshot(specs))
	{
		/* errors have already been logged */
		return false;
	}

	PGSQL *src = &(specs->sourceSnapshot.pgsql);
	PGSQL dst = { 0 };

	/* initialize our connection to the target database */
	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* open connection to target and set GUC values */
	if (!pgsql_set_gucs(&dst, dstSettings))
	{
		log_fatal("Failed to set our GUC settings on the target connection, "
				  "see above for details");
		return false;
	}

	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to open internal catalogs in COPY worker process, "
				  "see above for details");
		return false;
	}

	while (true)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(&(specs->copyQueue), &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("COPY worker has been interrupted");
			break;
		}

		if (!recv_ok)
		{
			log_error("COPY worker failed to receive a message from queue, "
					  "see above for details");
			break;
		}

		switch (mesg.type)
		{
			case QMSG_TYPE_STOP:
			{
				log_debug("Stop message received by COPY worker");
				(void) copydb_close_snapshot(specs);
				pgsql_finish(&dst);
				return true;
			}

			case QMSG_TYPE_TABLEPOID:
			{
				if (!copydb_copy_data_by_oid(specs,
											 src,
											 &dst,
											 mesg.data.tp.oid,
											 mesg.data.tp.part))
				{
					log_error("Failed to copy data for table with oid %u "
							  "and part number %u, see above for details",
							  mesg.data.tp.oid,
							  mesg.data.tp.part);

					++errors;

					if (specs->failFast)
					{
						pgsql_finish(&dst);
						return false;
					}

					/* clean-up our target connection state for next table */
					(void) copydb_close_snapshot(specs);

					if (!copydb_set_snapshot(specs))
					{
						/* errors have already been logged */
						return false;
					}
				}
				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on table queue %d",
						  mesg.type,
						  specs->copyQueue.qId);
				break;
			}
		}
	}

	/* terminate our connection to the source database now */
	(void) copydb_close_snapshot(specs);

	pgsql_finish(&dst);

	if (!catalog_delete_process(&(specs->catalogs.source), pid))
	{
		log_warn("Failed to delete catalog process entry for pid %d", pid);
	}

	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	return errors == 0;
}


/*
 * copydb_copy_data_by_oid finds the SourceTable entry by its OID and then
 * COPY the table data to the target database.
 */
bool
copydb_copy_data_by_oid(CopyDataSpec *specs, PGSQL *src, PGSQL *dst,
						uint32_t oid, uint32_t part)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	SourceTable *table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_table(sourceDB, oid, part, table) ||
		table->oid == 0)
	{
		log_error("Failed to lookup table oid %u in internal catalogs, "
				  "see above for details",
				  oid);

		free(table);
		return false;
	}

	log_trace("copydb_copy_data_by_oid: %u %s %lld %lld..%lld",
			  table->oid, table->qname,
			  (long long) table->partition.partNumber,
			  (long long) table->partition.min,
			  (long long) table->partition.max);

	CopyTableDataSpec *tableSpecs =
		(CopyTableDataSpec *) calloc(1, sizeof(CopyTableDataSpec));

	if (!copydb_init_table_specs(tableSpecs, specs, table, part))
	{
		/* errors have already been logged */
		free(table);
		free(tableSpecs);
		return false;
	}

	log_debug("copydb_copy_data_by_oid: %u %s, part %d",
			  oid,
			  table->qname,
			  part);

	char psTitle[BUFSIZE] = { 0 };

	if (table->partsArray.count > 0)
	{
		sformat(psTitle, sizeof(psTitle), "pgcopydb: copy %s [%d/%d]",
				table->qname,
				part + 1,
				table->partsArray.count);
	}
	else
	{
		sformat(psTitle, sizeof(psTitle), "pgcopydb: copy %s", table->qname);
	}

	(void) set_ps_title(psTitle);

	/*
	 * Skip tables that have been entirely done already on a previous run.
	 */
	bool isDone = false;

	if (!copydb_table_create_lockfile(specs, tableSpecs, &isDone))
	{
		/* errors have already been logged */
		free(table);
		free(tableSpecs);
		return false;
	}

	if (isDone)
	{
		log_info("Skipping table %s (%u), already done on a previous run",
				 tableSpecs->sourceTable->qname,
				 tableSpecs->sourceTable->oid);
		free(table);
		free(tableSpecs);
		return true;
	}

	/*
	 * 1. Now COPY the TABLE DATA from the source to the destination.
	 */
	if (!table->excludeData)
	{
		if (!copydb_copy_table(specs, src, dst, tableSpecs))
		{
			/* errors have already been logged */
			free(table);
			free(tableSpecs);
			return false;
		}
	}

	if (!copydb_mark_table_as_done(specs, tableSpecs))
	{
		/* errors have already been logged */
		free(table);
		free(tableSpecs);
		return false;
	}

	if (specs->section == DATA_SECTION_TABLE_DATA)
	{
		log_debug("Skip indexes, constraints, vacuum (section: table-data)");
		return true;
	}

	/*
	 * 2. Send the indexes and constraints attached to this table to the
	 *    index job queue.
	 *
	 * If a partial COPY is happening, check that all the other parts are done.
	 * This check should be done in the critical section too. Only one process
	 * can see all parts as done already, and that's the one finishing last.
	 */
	if (specs->dirState.indexCopyIsDone &&
		specs->section != DATA_SECTION_CONSTRAINTS)
	{
		log_info("Skipping indexes, already done on a previous run");
	}
	else
	{
		bool allPartsDone = false;
		bool indexesAreBeingProcessed = false;

		if (!copydb_table_parts_are_all_done(specs,
											 tableSpecs,
											 &allPartsDone,
											 &indexesAreBeingProcessed))
		{
			/* errors have already been logged */
			free(table);
			free(tableSpecs);
			return false;
		}
		else if (allPartsDone && !indexesAreBeingProcessed)
		{
			/*
			 * The VACUUM command takes a conflicting lock with the CREATE
			 * INDEX and ALTER TABLE commands used for indexes and constraints,
			 * and as a result we send a table to the vacuum queue only after
			 * its indexes have all been built.
			 *
			 * When a table has no indexes though, we never reach the code that
			 * checks if all the indexes have been built already. In that case,
			 * just add the table to the vacuum queue already.
			 */
			if (!catalog_s_table_count_indexes(sourceDB,
											   tableSpecs->sourceTable))
			{
				log_error("Failed to count indexes attached to table %s",
						  tableSpecs->sourceTable->qname);
				return false;
			}

			if (tableSpecs->sourceTable->indexCount == 0)
			{
				if (!specs->skipVacuum)
				{
					SourceTable *sourceTable = tableSpecs->sourceTable;

					if (!vacuum_add_table(specs, sourceTable->oid))
					{
						log_error("Failed to queue VACUUM ANALYZE %s [%u]",
								  sourceTable->qname,
								  sourceTable->oid);
						free(table);
						free(tableSpecs);
						return false;
					}
				}
			}
			else if (!copydb_add_table_indexes(specs, tableSpecs))
			{
				log_error("Failed to add the indexes for %s, "
						  "see above for details",
						  tableSpecs->sourceTable->qname);
				free(table);
				free(tableSpecs);
				return false;
			}
		}
	}

	free(table);
	free(tableSpecs);

	return true;
}


/*
 * copydb_table_create_lockfile checks done file to see if a given table has
 * already been processed in a previous run, and creates the lockfile to
 * register progress for command: pgcopydb list progress.
 */
bool
copydb_table_create_lockfile(CopyDataSpec *specs,
							 CopyTableDataSpec *tableSpecs,
							 bool *isDone)
{
	if (specs->dirState.tableCopyIsDone)
	{
		log_notice("Skipping table %s, already done on a previous run",
				   tableSpecs->sourceTable->qname);

		*isDone = true;
		return true;
	}

	/* enter the critical section */
	(void) semaphore_lock(&(specs->tableSemaphore));

	/*
	 * If the doneFile exists, then the table has been processed already,
	 * skip it.
	 */
	if (file_exists(tableSpecs->tablePaths.doneFile))
	{
		*isDone = true;
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
			(void) semaphore_unlock(&(specs->tableSemaphore));

			log_error("Failed to start table-data COPY worker for table %s (%u), "
					  "lock file \"%s\" is owned by running process %d",
					  tableSpecs->sourceTable->qname,
					  tableSpecs->sourceTable->oid,
					  tableSpecs->tablePaths.lockFile,
					  tableSummary.pid);

			return false;
		}
		else
		{
			log_notice("Found stale pid %d in file \"%s\", removing it "
					   "and processing table %s",
					   tableSummary.pid,
					   tableSpecs->tablePaths.lockFile,
					   tableSpecs->sourceTable->qname);

			/* stale pid, remove the old lockFile now, then process the table */
			if (!unlink_file(tableSpecs->tablePaths.lockFile))
			{
				log_error("Failed to remove the stale lockFile \"%s\"",
						  tableSpecs->tablePaths.lockFile);
				(void) semaphore_unlock(&(specs->tableSemaphore));
				return false;
			}

			/* pass through to the rest of this function */
		}
	}

	/*
	 * Now, write the lockFile, with a summary of what's going-on.
	 */
	CopyTableSummary emptySummary = { 0 };
	CopyTableSummary *summary =
		(CopyTableSummary *) calloc(1, sizeof(CopyTableSummary));

	*summary = emptySummary;

	summary->pid = getpid();
	summary->table = tableSpecs->sourceTable;

	/* "COPY " is 5 bytes, then 1 for \0 */
	int len = strlen(tableSpecs->sourceTable->qname) + 5 + 1;
	summary->command = (char *) calloc(len, sizeof(char));

	if (summary->command == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(summary->command, len, "COPY %s", tableSpecs->sourceTable->qname);

	if (!open_table_summary(summary, tableSpecs->tablePaths.lockFile))
	{
		log_info("Failed to create the lock file for table %s at \"%s\"",
				 tableSpecs->sourceTable->qname,
				 tableSpecs->tablePaths.lockFile);

		/* end of the critical section */
		(void) semaphore_unlock(&(specs->tableSemaphore));

		return false;
	}

	/* attach the new summary to the tableSpecs, where it was NULL before */
	tableSpecs->summary = summary;

	/* also track the process information in our catalogs */
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	ProcessInfo ps = {
		.pid = getpid(),
		.psType = "COPY",
		.psTitle = ps_buffer,
		.tableOid = tableSpecs->sourceTable->oid,
		.partNumber = tableSpecs->part.partNumber
	};

	if (!catalog_upsert_process_info(sourceDB, &ps))
	{
		log_error("Failed to track progress in our catalogs, "
				  "see above for details");
		return false;
	}

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
			  tableSpecs->sourceTable->qname,
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
copydb_copy_table(CopyDataSpec *specs, PGSQL *src, PGSQL *dst,
				  CopyTableDataSpec *tableSpecs)
{
	/* COPY the data from the source table to the target table */
	if (tableSpecs->section != DATA_SECTION_TABLE_DATA &&
		tableSpecs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping table data in section %d", tableSpecs->section);
		return true;
	}

	/* first, fetch attributes from our source database */
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!catalog_s_table_fetch_attrs(sourceDB, tableSpecs->sourceTable))
	{
		log_error("Failed to fetch table %s attribute list, "
				  "see above for details",
				  tableSpecs->sourceTable->qname);
		return false;
	}

	/* we want to set transaction snapshot to the main one on the source */
	CopyTableSummary *summary = tableSpecs->summary;

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
			if (!pgsql_truncate(dst, tableSpecs->sourceTable->qname))
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

	truncate = false;

	/* Now copy the data from source to target */
	log_notice("%s", summary->command);

	/* COPY FROM tablename, or maybe COPY FROM (SELECT ... WHERE ...) */
	PQExpBuffer copySrc = createPQExpBuffer();
	PQExpBuffer copyDst = createPQExpBuffer();

	if (copySrc == NULL || copyDst == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!copydb_prepare_copy_query(tableSpecs, copySrc, true))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(copySrc);
		return false;
	}

	if (!copydb_prepare_copy_query(tableSpecs, copyDst, false))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(copySrc);
		destroyPQExpBuffer(copyDst);
		return false;
	}

	int attempts = 0;
	int maxAttempts = 5;        /* allow 5 attempts total, 4 retries */

	bool retry = true;
	bool success = false;

	while (!success && retry)
	{
		++attempts;

		/* ignore previous attempts, we need only one success here */
		success = pg_copy(src, dst, copySrc->data, copyDst->data, truncate,
						  &(summary->bytesTransmitted));

		if (success)
		{
			/* success, get out of the retry loop */
			if (attempts > 1)
			{
				log_info("Table %s COPY succeeded after %d attempts",
						 tableSpecs->sourceTable->qname,
						 attempts);
			}
			break;
		}

		/* errors have already been logged */
		retry =
			attempts < maxAttempts &&

			/* retry only on Connection Exception errors */
			(pgsql_state_is_connection_error(src) ||
			 pgsql_state_is_connection_error(dst));

		if (maxAttempts <= attempts)
		{
			log_error("Failed to copy table %s even after %d attempts, "
					  "see above for details",
					  tableSpecs->sourceTable->qname,
					  attempts);
		}
		else if (retry)
		{
			log_info("Failed to copy table %s (connection exception), "
					 "retrying in %dms (attempt %d)",
					 tableSpecs->sourceTable->qname,
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

	destroyPQExpBuffer(copySrc);
	destroyPQExpBuffer(copyDst);

	return success;
}


/*
 * copydb_prepare_copy_query prepares a COPY query using the list of attribute
 * names from the SourceTable instance.
 */
bool
copydb_prepare_copy_query(CopyTableDataSpec *tableSpecs,
						  PQExpBuffer query,
						  bool source)
{
	SourceTable *table = tableSpecs->sourceTable;
	bool isFirst = true;

	if (source)
	{
		/*
		 * Always use a sub-query as the copy source, that's easier to hack
		 * around if comes a time when sophistication is required.
		 */
		appendPQExpBufferStr(query, "(SELECT ");

		for (int i = 0; i < table->attributes.count; i++)
		{
			SourceTableAttribute *attribute = &(table->attributes.array[i]);
			char *attname = attribute->attname;

			/* Generated columns cannot be used in COPY */
			if (attribute->attisgenerated)
			{
				log_debug("Skipping %s in COPY as it is a generated column",
						  attname);
				continue;
			}

			if (!isFirst)
			{
				appendPQExpBufferStr(query, ", ");
			}
			else
			{
				isFirst = false;
			}

			appendPQExpBuffer(query, "%s", attname);
		}

		appendPQExpBuffer(query, " FROM ONLY %s ", tableSpecs->sourceTable->qname);

		/*
		 * On a source COPY query we might want to add filtering.
		 */
		if (tableSpecs->part.partCount > 1)
		{
			/*
			 * The way schema_list_partitions prepares the boundaries is non
			 * overlapping, so we can use the BETWEEN operator to select our source
			 * rows in the COPY sub-query.
			 */
			if (streq(tableSpecs->part.partKey, "ctid"))
			{
				if (tableSpecs->part.max == -1)
				{
					/* the last part for ctid splits covers "extra" relpages */
					appendPQExpBuffer(query,
									  " WHERE ctid >= '(%lld,0)'::tid",
									  (long long) tableSpecs->part.min + 1);
				}
				else
				{
					appendPQExpBuffer(query,
									  " WHERE ctid >= '(%lld,0)'::tid"
									  " and ctid < '(%lld,0)'::tid",
									  (long long) tableSpecs->part.min,
									  (long long) tableSpecs->part.max + 1);
				}
			}
			else
			{
				appendPQExpBuffer(query,
								  " WHERE %s BETWEEN %lld AND %lld ",
								  tableSpecs->part.partKey,
								  (long long) tableSpecs->part.min,
								  (long long) tableSpecs->part.max);
			}
		}

		appendPQExpBufferStr(query, ")");
	}
	else
	{
		/*
		 * For the destination query, use the table(...) syntax.
		 */
		appendPQExpBuffer(query, "%s", tableSpecs->sourceTable->qname);

		for (int i = 0; i < table->attributes.count; i++)
		{
			SourceTableAttribute *attribute = &(table->attributes.array[i]);
			char *attname = attribute->attname;

			/* Generated columns cannot be used in COPY */
			if (attribute->attisgenerated)
			{
				log_debug("Skipping %s in COPY as it is a generated column",
						  attname);
				continue;
			}

			if (isFirst)
			{
				appendPQExpBufferStr(query, "(");
				isFirst = false;
			}
			else
			{
				appendPQExpBufferStr(query, ", ");
			}

			appendPQExpBuffer(query, "%s", attname);
		}

		if (table->attributes.count > 0)
		{
			appendPQExpBufferStr(query, ")");
		}
	}

	if (PQExpBufferBroken(query))
	{
		log_error("Failed to create COPY query for %s: out of memory",
				  tableSpecs->sourceTable->qname);
		return false;
	}

	return true;
}
