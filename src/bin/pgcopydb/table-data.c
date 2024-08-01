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


static bool copydb_copy_supervisor_add_table_hook(void *ctx, SourceTable *table);

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
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (specs->runState.tableCopyIsDone &&
		specs->runState.indexCopyIsDone &&
		specs->runState.sequenceCopyIsDone &&
		specs->section != DATA_SECTION_CONSTRAINTS)
	{
		log_info("Skipping tables, indexes, and sequences, "
				 "already done on a previous run");
		return true;
	}

	if (!summary_start_timing(sourceDB, TIMING_SECTION_TOTAL_DATA))
	{
		/* errors have already been logged */
		return false;
	}

	/* close SQLite databases before fork() */
	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

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

	/*
	 * Catalogs have been closed before forking sub-processes, re-open again.
	 */
	if (!catalog_open_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_TOTAL_DATA))
	{
		/* errors have already been logged */
		return false;
	}

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
	 * Take care of extensions configuration table in an auxilliary process.
	 */
	bool createExtension = false;

	if (!copydb_start_extension_data_process(specs, createExtension))
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
		if (errors == 0 && !copydb_start_index_supervisor(specs))
		{
			/* errors have already been logged */
			++errors;
		}

		/*
		 * Now create as many VACUUM ANALYZE sub-processes as needed, per
		 * --table-jobs. Could be exposed separately as --vacuumJobs too, but
		 * that's not been done at this time.
		 */
		if (errors == 0 && !vacuum_start_supervisor(specs))
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
	 * Are blobs table data? well pg_dump --section says yes.
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


typedef struct CopySupervisorContext
{
	CopyDataSpec *specs;
	PGSQL *dst;
} CopySupervisorContext;


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

	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!catalog_open(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_start_timing(sourceDB, TIMING_SECTION_COPY_DATA))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now start the worker that adds tables to the queue.
	 */
	if (!copydb_copy_start_worker_queue_tables(specs))
	{
		log_fatal("Failed to start table data COPY queue worker, "
				  "see above for details");

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

		if (specs->failFast)
		{
			(void) copydb_fatal_exit();
		}
		else
		{
			/* send create index workers a STOP message */
			if (!copydb_index_workers_send_stop(specs))
			{
				(void) copydb_fatal_exit();
			}
		}

		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_COPY_DATA))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_close(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now that the COPY processes are done, signal this is the end to the
	 * CREATE INDEX sub-processes by adding the STOP message to
	 * their queues.
	 */
	if (!copydb_index_workers_send_stop(specs))
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
 * copydb_copy_start_worker_queue_tables starts the COPY worker process that
 * iterate over the list of tables and add them to the tables-data process
 * queue.
 */
bool
copydb_copy_start_worker_queue_tables(CopyDataSpec *specs)
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
			(void) set_ps_title("pgcopydb: copy queue tables");

			if (!copydb_copy_worker_queue_tables(specs))
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
 * copydb_copy_worker_queue_tables iterates over the list of tables and sends
 * the to the table-data copy queue.
 */
bool
copydb_copy_worker_queue_tables(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started queue tables COPY worker %d [%d]", pid, getppid());

	CopySupervisorContext context = {
		.specs = specs,
		.dst = NULL
	};

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
	CatalogTableStats stats = { 0 };

	if (!catalog_s_table_stats(sourceDB, &stats))
	{
		log_error("Failed to compute source table statistics, "
				  "see above for details");
		return false;
	}

	/*
	 * If some of our tables are going to be partitioned, then we need to
	 * TRUNCATE them on the target server before adding them to the process
	 * queue. That means we need to open a connection now.
	 *
	 * We don't bother with setting our GUCs in that connection as all we're
	 * going to do here is a series of TRUNCATE ONLY commands.
	 */
	PGSQL dst = { 0 };

	if (stats.countSplits > 0)
	{
		char *pguri = specs->connStrings.target_pguri;

		if (!pgsql_init(&dst, pguri, PGSQL_CONN_TARGET))
		{
			/* errors have already been logged */
			return false;
		}

		context.dst = &dst;
	}

	if (!catalog_iter_s_table(sourceDB,
							  &context,
							  copydb_copy_supervisor_add_table_hook))
	{
		log_fatal("Failed to add tables to the COPY worker queue, terminating");
		(void) pgsql_finish(&dst);
		(void) copydb_fatal_exit();
		return false;
	}

	if (stats.countSplits > 0)
	{
		(void) pgsql_finish(&dst);
	}

	if (!catalog_close_from_specs(specs))
	{
		log_error("Failed to close internal catalogs in COPY supervisor, "
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

	return true;
}


/*
 * copydb_copy_supervisor_add_table_hook is an iterator callback function.
 */
static bool
copydb_copy_supervisor_add_table_hook(void *ctx, SourceTable *table)
{
	CopySupervisorContext *context = (CopySupervisorContext *) ctx;

	CopyDataSpec *specs = context->specs;
	PGSQL *dst = context->dst;

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
		 *
		 * Before adding the table to be processed by workers, truncate it on
		 * the target database now, avoiding concurrency issues.
		 */
		bool granted = false;

		if (!pgsql_has_table_privilege(dst, table->qname, "TRUNCATE", &granted))
		{
			/* errors have already been logged */
			return false;
		}

		if (granted)
		{
			if (!pgsql_truncate(dst, table->qname))
			{
				/* errors have already been logged */
				return false;
			}
		}

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
	log_info("STEP 4: starting %d table-data COPY processes", specs->tableJobs);

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

	log_notice("Started table-data COPY worker %d [%d]", pid, getppid());

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

	bool stop = false;

	while (!stop)
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
				stop = true;
				log_debug("Stop message received by COPY worker");
				break;
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

	return stop == true && errors == 0;
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
		return false;
	}

	log_debug("copydb_copy_data_by_oid: %u %s, part %d",
			  oid,
			  table->qname,
			  part);

	/*
	 * Now check that the table still exists on the source server.
	 */
	bool tableStillExists = true;

	if (!copydb_check_table_exists(src, table, &tableStillExists))
	{
		/* errors have already been logged */
		return false;
	}

	if (!tableStillExists)
	{
		log_warn("Skipping table %s (oid %u) which does not exists anymore "
				 "on the source database",
				 table->qname, oid);
		return true;
	}

	char psTitle[BUFSIZE] = { 0 };

	if (table->partition.partCount > 0)
	{
		sformat(psTitle, sizeof(psTitle), "pgcopydb: copy %s [%d/%d]",
				table->qname,
				table->partition.partNumber,
				table->partition.partCount);
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

	if (!copydb_table_create_lockfile(specs, tableSpecs, dst, &isDone))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Skip only table-data copy when it it has been done already on a previous
	 * run. We still need to process the indexes, constraints, and vacuum.
	 * So, signal the index and vacuum workers as usual.
	 */
	if (isDone)
	{
		log_info("Skipping table-data %s (%u), already done on a previous run",
				 tableSpecs->sourceTable->qname,
				 tableSpecs->sourceTable->oid);
	}
	else
	{
		/*
		 * 1. Now COPY the TABLE DATA from the source to the destination.
		 */
		if (!table->excludeData)
		{
			if (!copydb_copy_table(specs, src, dst, tableSpecs))
			{
				/* errors have already been logged */
				return false;
			}
		}

		if (!copydb_mark_table_as_done(specs, tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}
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
	if (specs->runState.indexCopyIsDone &&
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
						return false;
					}
				}
			}
			else if (!copydb_add_table_indexes(specs, tableSpecs))
			{
				log_error("Failed to add the indexes for %s, "
						  "see above for details",
						  tableSpecs->sourceTable->qname);
				return false;
			}
		}
	}

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
							 PGSQL *dst,
							 bool *isDone)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (specs->runState.tableCopyIsDone)
	{
		log_notice("Skipping table %s, already done on a previous run",
				   tableSpecs->sourceTable->qname);

		*isDone = true;
		return true;
	}

	if (!summary_lookup_table(sourceDB, tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	CopyTableSummary *tableSummary = &(tableSpecs->summary);

	/* if the catalog summary information is complete, we're done */
	if (tableSummary->doneTime > 0)
	{
		*isDone = true;
		return true;
	}

	if (tableSummary->pid != 0)
	{
		/* if we can signal the pid, it is still running */
		if (kill(tableSummary->pid, 0) == 0)
		{
			log_error("Failed to start table-data COPY worker for table %s (%u), "
					  "already being processed by pid %d",
					  tableSpecs->sourceTable->qname,
					  tableSpecs->sourceTable->oid,
					  tableSummary->pid);

			return false;
		}
		else
		{
			log_notice("Found stale pid %d, removing it to process table %s",
					   tableSummary->pid,
					   tableSpecs->sourceTable->qname);

			/* stale pid, remove the summary entry and process the table */
			if (!summary_delete_table(sourceDB, tableSpecs))
			{
				/* errors have already been logged */
				return false;
			}

			/* pass through to the rest of this function */
		}
	}

	/* build the table attributes' list */
	if (!catalog_s_table_attrlist(sourceDB, tableSpecs->sourceTable))
	{
		log_error("Failed to fetch table %s attribute list, "
				  "see above for details",
				  tableSpecs->sourceTable->qname);
		return false;
	}

	/* COPY FROM tablename, or maybe COPY FROM (SELECT ... WHERE ...) */
	CopyArgs *args = &(tableSpecs->copyArgs);

	args->srcQname = tableSpecs->sourceTable->qname;
	args->srcAttrList = tableSpecs->sourceTable->attrList;
	args->srcWhereClause = NULL;
	args->dstQname = tableSpecs->sourceTable->qname;
	args->dstAttrList = tableSpecs->sourceTable->attrList;
	args->truncate = false;     /* default value, see below */
	args->freeze = tableSpecs->sourceTable->partition.partCount <= 1;
	args->bytesTransmitted = 0;
	args->useCopyBinary = specs->useCopyBinary;

	/*
	 * Check to see if we want to TRUNCATE the table and benefit from the COPY
	 * FREEZE optimisation.
	 *
	 * First, if the table COPY is partitionned then we truncate at the
	 * top-level rather than for each partition, disabling the COPY FREEZE
	 * optimisation.
	 *
	 * Second, we need the permission to run the TRUNCATE command on the target
	 * table on the target database.
	 */
	if (tableSpecs->sourceTable->partition.partCount <= 1)
	{
		bool granted = false;

		if (!pgsql_has_table_privilege(dst,
									   tableSpecs->sourceTable->qname,
									   "TRUNCATE",
									   &granted))
		{
			/* errors have already been logged */
			return false;
		}

		args->truncate = granted;
	}

	if (!copydb_prepare_copy_query(tableSpecs, args))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_prepare_summary_command(tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_add_table(sourceDB, tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* also track the process information in our catalogs */
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
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!summary_finish_table(sourceDB, tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_increment_timing(sourceDB,
								  TIMING_SECTION_COPY_DATA,
								  1, /* count */
								  tableSpecs->sourceTable->bytes,
								  tableSpecs->summary.durationMs))
	{
		/* errors have already been logged */
		return false;
	}

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
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (tableSpecs->part.partCount <= 1)
	{
		*allPartsDone = true;
		*isBeingProcessed = false;
		return true;
	}

	*allPartsDone = false;

	if (!summary_table_count_parts_done(sourceDB, tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * If all partitions are done, try and register this worker's PID as the
	 * first worker that saw the situation. Only that one is allowed to queue
	 * the CREATE INDEX (or VACUUM) commands.
	 */
	int partCount = tableSpecs->sourceTable->partition.partCount;

	if (tableSpecs->countPartsDone == partCount)
	{
		*allPartsDone = true;

		/* insert or ignore our pid as the partsDonePid */
		if (!summary_add_table_parts_done(sourceDB, tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		if (!summary_lookup_table_parts_done(sourceDB, tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		/* set isBeingProcessed to false to allow processing indexes */
		*isBeingProcessed = (tableSpecs->partsDonePid != getpid());
	}

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

	/* Now copy the data from source to target */
	CopyTableSummary *summary = &(tableSpecs->summary);

	int attempts = 0;
	int maxAttempts = 5;        /* allow 5 attempts total, 4 retries */

	bool retry = true;
	bool success = false;

	while (!success && retry)
	{
		++attempts;

		/* ignore previous attempts, we need only one success here */
		success = pg_copy(src, dst, &(tableSpecs->copyArgs));

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

	/* publish bytesTransmitted accumulated value to the summary */
	summary->bytesTransmitted = tableSpecs->copyArgs.bytesTransmitted;

	return success;
}


/*
 * copydb_prepare_copy_query prepares a COPY query using the list of attribute
 * names from the SourceTable instance.
 */
bool
copydb_prepare_copy_query(CopyTableDataSpec *tableSpecs, CopyArgs *args)
{
	/*
	 * On a source COPY query we might want to add filtering.
	 */
	if (tableSpecs->sourceTable->partition.partCount > 1)
	{
		PQExpBuffer srcWhereClause = createPQExpBuffer();

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
				appendPQExpBuffer(srcWhereClause,
								  "WHERE ctid >= '(%lld,0)'::tid",
								  (long long) tableSpecs->part.min);
			}
			else
			{
				appendPQExpBuffer(srcWhereClause,
								  "WHERE ctid >= '(%lld,0)'::tid"
								  " and ctid < '(%lld,0)'::tid",
								  (long long) tableSpecs->part.min,
								  (long long) tableSpecs->part.max + 1);
			}
		}
		else
		{
			/* partition to take care of NULL values */
			if (tableSpecs->part.min == -1 &&
				tableSpecs->part.max == -1)
			{
				appendPQExpBuffer(srcWhereClause,
								  "WHERE %s IS NULL",
								  tableSpecs->part.partKey);
			}

			/* the last partition has no upper bound */
			else if (tableSpecs->part.max == -1)
			{
				appendPQExpBuffer(srcWhereClause,
								  "WHERE %s >= %lld",
								  tableSpecs->part.partKey,
								  (long long) tableSpecs->part.min);
			}
			else
			{
				appendPQExpBuffer(srcWhereClause,
								  "WHERE %s BETWEEN %lld AND %lld",
								  tableSpecs->part.partKey,
								  (long long) tableSpecs->part.min,
								  (long long) tableSpecs->part.max);
			}
		}

		if (PQExpBufferBroken(srcWhereClause))
		{
			log_error("Failed to create where clause for %s: out of memory",
					  args->srcQname);
			return false;
		}

		args->srcWhereClause = strdup(srcWhereClause->data);
		destroyPQExpBuffer(srcWhereClause);
	}

	return true;
}


/*
 * copydb_prepare_summary_command prepares the table summary command:
 *
 *  COPY qname WHERE ...
 */
bool
copydb_prepare_summary_command(CopyTableDataSpec *tableSpecs)
{
	CopyTableSummary *tableSummary = &(tableSpecs->summary);

	PQExpBuffer command = createPQExpBuffer();

	appendPQExpBuffer(command, "COPY %s", tableSpecs->sourceTable->qname);

	if (tableSpecs->copyArgs.srcWhereClause != NULL)
	{
		appendPQExpBuffer(command, " %s", tableSpecs->copyArgs.srcWhereClause);
	}

	tableSummary->command =
		command->data != NULL ? strdup(command->data) : NULL;

	/* also keep a pointer around in the copyArgs structure */
	tableSpecs->copyArgs.logCommand = tableSummary->command;

	if (PQExpBufferBroken(command) || tableSummary->command == NULL)
	{
		log_error("Failed to create summary command for %s: out of memory",
				  tableSpecs->sourceTable->qname);
		return false;
	}

	return true;
}


/*
 * copydb_check_table_exists checks that a table still exists. In order to
 * avoid race conditions when checking for existence, grab a explicit ACCESS
 * SHARE LOCK on the table.
 */
bool
copydb_check_table_exists(PGSQL *pgsql, SourceTable *table, bool *exists)
{
	if (!pgsql_table_exists(pgsql,
							table->oid,
							table->nspname,
							table->relname,
							exists))
	{
		/* errors have already been logged */
		return false;
	}

	/* if the table does not exists, we stop here */
	if (!exists)
	{
		return true;
	}

	/* if the table was reported to exists, try and lock it */
	bool locked = pgsql_lock_table(pgsql, table->qname, "ACCESS SHARE");

	if (!locked)
	{
		log_error("Failed to LOCK table %s in ACCESS SHARE mode", table->qname);
	}

	/*
	 * If we failed to obtain the lock, maybe the table doesn't exists anymore,
	 * in which case we do not want to report an error condition.
	 */
	if (!pgsql_table_exists(pgsql,
							table->oid,
							table->nspname,
							table->relname,
							exists))
	{
		/* errors have already been logged */
		return false;
	}

	return locked || !(*exists);
}
