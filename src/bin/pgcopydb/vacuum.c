/*
 * src/bin/pgcopydb/vacuum.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "catalog.h"
#include "cli_root.h"
#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "signals.h"
#include "summary.h"

/*
 * vacuum_start_supervisor starts a VACUUM supervisor process.
 */
bool
vacuum_start_supervisor(CopyDataSpec *specs)
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
			log_error("Failed to fork vacuum supervisor process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			(void) set_ps_title("pgcopydb: vacuum supervisor");

			if (!vacuum_supervisor(specs))
			{
				log_error(
					"Failed to vacuum analyze tables on target, see above for details");
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
 * vacuum_supervisor starts the vacuum workers and does the waitpid() dance for
 * them.
 */
bool
vacuum_supervisor(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started VACUUM supervisor %d [%d]", pid, getppid());

	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!catalog_open(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Start cumulative sections timings for vacuum
	 */
	if (!summary_start_timing(sourceDB, TIMING_SECTION_VACUUM))
	{
		/* errors have already been logged */
		return false;
	}

	if (!vacuum_start_workers(specs))
	{
		log_error("Failed to start vacuum workers, see above for details");
		return false;
	}

	/*
	 * Now just wait for the vacuum processes to be done.
	 */
	if (!copydb_wait_for_subprocesses(specs->failFast))
	{
		log_error("Some VACUUM worker process(es) have exited with error, "
				  "see above for details");

		if (specs->failFast)
		{
			(void) copydb_fatal_exit();
		}

		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_VACUUM))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * vacuum_start_workers create as many sub-process as needed, per --table-jobs.
 * Could be exposed separately as --vacuumJobs too, but that's not been done at
 * this time.
 */
bool
vacuum_start_workers(CopyDataSpec *specs)
{
	if (specs->skipVacuum)
	{
		log_info("STEP 8: skipping VACUUM jobs per --skip-vacuum");
		return true;
	}

	log_info("STEP 8: starting %d VACUUM processes", specs->vacuumJobs);

	for (int i = 0; i < specs->vacuumJobs; i++)
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
				log_error("Failed to fork a vacuum worker process: %m");
				return false;
			}

			case 0:
			{
				/* child process runs the command */
				(void) set_ps_title("pgcopydb: vacuum worker");

				if (!vacuum_worker(specs))
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
 * vacuum_worker is a worker process that loops over messages received from a
 * queue, each message being the Oid of a table to vacuum on the target
 * database.
 */
bool
vacuum_worker(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started VACUUM worker %d [%d]", pid, getppid());

	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to open internal catalogs in VACUUM worker process, "
				  "see above for details");
		return false;
	}

	int errors = 0;
	bool stop = false;

	while (!stop)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(&(specs->vacuumQueue), &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("VACUUM worker has been interrupted");
			return false;
		}

		if (!recv_ok)
		{
			/* errors have already been logged */
			return false;
		}

		switch (mesg.type)
		{
			case QMSG_TYPE_STOP:
			{
				stop = true;
				log_debug("Stop message received by vacuum worker");
				break;
			}

			case QMSG_TYPE_TABLEOID:
			{
				if (!vacuum_analyze_table_by_oid(specs, mesg.data.oid))
				{
					++errors;

					log_error("Failed to vacuum table with oid %u, "
							  "see above for details",
							  mesg.data.oid);

					if (specs->failFast)
					{
						return false;
					}
				}
				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on vacuum queue %d",
						  mesg.type,
						  specs->vacuumQueue.qId);
				break;
			}
		}
	}

	if (!catalog_delete_process(&(specs->catalogs.source), pid))
	{
		log_warn("Failed to delete catalog process entry for pid %d", pid);
	}

	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	bool success = (stop == true && errors == 0);

	if (errors > 0)
	{
		log_error("VACUUM worker %d encountered %d errors, "
				  "see above for details",
				  pid,
				  errors);
	}

	return success;
}


/*
 * vacuum_analyze_table_by_oid reads the done file for the given table OID,
 * fetches the schemaname and relname from there, and then connects to the
 * target database to issue a VACUUM ANALYZE command.
 */
bool
vacuum_analyze_table_by_oid(CopyDataSpec *specs, uint32_t oid)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	SourceTable table = { 0 };

	if (!catalog_lookup_s_table(sourceDB, oid, 0, &table))
	{
		log_error("Failed to lookup table oid %u in internal catalogs, "
				  "see above for details",
				  oid);
		return false;
	}

	log_trace("vacuum_analyze_table_by_oid: %u %s", table.oid, table.qname);

	CopyTableDataSpec tableSpecs = { 0 };

	/* vacuum is done per table, irrespective of the COPY partitioning */
	if (!copydb_init_table_specs(&tableSpecs, specs, &table, 0))
	{
		/* errors have already been logged */
		return false;
	}

	PGSQL dst = { 0 };

	/* initialize our connection to the target database */
	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* finally, vacuum analyze the table and its indexes */
	char vacuum[BUFSIZE] = { 0 };

	sformat(vacuum, sizeof(vacuum),
			"VACUUM ANALYZE %s.%s",
			table.nspname,
			table.relname);

	/* also set the process title for this specific table */
	char psTitle[BUFSIZE] = { 0 };
	sformat(psTitle, sizeof(psTitle), "pgcopydb: %s", vacuum);
	(void) set_ps_title(psTitle);

	log_notice("%s;", vacuum);

	/* also track the process information in our catalogs */
	ProcessInfo ps = {
		.pid = getpid(),
		.psType = "VACUUM",
		.psTitle = ps_buffer,
		.tableOid = table.oid
	};

	if (!catalog_upsert_process_info(sourceDB, &ps))
	{
		log_error("Failed to track progress in our catalogs, "
				  "see above for details");
		return false;
	}

	if (!summary_add_vacuum(sourceDB, &tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_execute(&dst, vacuum))
	{
		log_error("Failed to run command, see above for details: %s", vacuum);
		return false;
	}

	(void) pgsql_finish(&dst);

	if (!summary_finish_vacuum(sourceDB, &tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_increment_timing(sourceDB,
								  TIMING_SECTION_VACUUM,
								  1, /* count */
								  0, /* bytes */
								  tableSpecs.vSummary.durationMs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * vacuum_add_table sends a message to the VACUUM process queue to process
 * given table.
 */
bool
vacuum_add_table(CopyDataSpec *specs, uint32_t oid)
{
	QMessage mesg = {
		.type = QMSG_TYPE_TABLEOID,
		.data.oid = oid
	};

	log_debug("vacuum_add_table: %u", oid);

	if (!queue_send(&(specs->vacuumQueue), &mesg))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * vacuum_send_stop sends the STOP message to the VACUUM workers.
 *
 * Each worker will consume one STOP message before stopping, so we need to
 * send as many STOP messages as we have started worker processes.
 */
bool
vacuum_send_stop(CopyDataSpec *specs)
{
	if (specs->skipVacuum)
	{
		return true;
	}

	for (int i = 0; i < specs->vacuumJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP, .data.oid = 0 };

		log_debug("Send STOP message to VACUUM queue %d",
				  specs->vacuumQueue.qId);

		if (!queue_send(&(specs->vacuumQueue), &stop))
		{
			/* errors have already been logged */
			continue;
		}
	}

	return true;
}
