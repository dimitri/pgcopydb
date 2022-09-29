/*
 * src/bin/pgcopydb/vacuum.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "signals.h"
#include "summary.h"

/*
 * vacuum_start_workers create as many sub-process as needed, per --table-jobs.
 * Could be exposed separately as --vacuumJobs too, but that's not been done at
 * this time.
 */
bool
vacuum_start_workers(CopyDataSpec *specs)
{
	log_info("STEP 8: starting %d VACUUM processes", specs->vacuumJobs);
	log_trace("vacuum_start_workers: \"%s\"", specs->cfPaths.tbldir);

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
				log_error("Failed to fork a worker process: %m");
				return false;
			}

			case 0:
			{
				/* child process runs the command */
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
	int errors = 0;
	bool stop = false;

	log_notice("Started VACUUM worker %d [%d]", getpid(), getppid());
	log_trace("vacuum_worker: \"%s\"", specs->cfPaths.tbldir);

	while (!stop)
	{
		QMessage mesg = { 0 };

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			return false;
		}

		if (!queue_receive(&(specs->vacuumQueue), &mesg))
		{
			/* errors have already been logged */
			break;
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
				/* ignore errors */
				if (!vacuum_analyze_table_by_oid(specs, mesg.data.oid))
				{
					++errors;
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

	return stop == true && errors == 0;
}


/*
 * vacuum_analyze_table_by_oid reads the done file for the given table OID,
 * fetches the schemaname and relname from there, and then connects to the
 * target database to issue a VACUUM ANALYZE command.
 */
bool
vacuum_analyze_table_by_oid(CopyDataSpec *specs, uint32_t oid)
{
	CopyFilePaths *cfPaths = &(specs->cfPaths);
	TableFilePaths tablePaths = { 0 };

	log_trace("vacuum_analyze_table_by_oid: \"%s\"", specs->cfPaths.tbldir);

	if (!copydb_init_tablepaths(cfPaths, &tablePaths, oid))
	{
		log_error("Failed to prepare pathnames for table %u", oid);
		return false;
	}

	/* the source table COPY might have been partionned */
	if (!file_exists(tablePaths.doneFile))
	{
		int part = 0;

		if (!copydb_init_tablepaths_for_part(cfPaths, &tablePaths, oid, part))
		{
			log_error("Failed to prepare pathnames for table %u", oid);
			return false;
		}
	}

	log_trace("vacuum_analyze_table_by_oid: %s", tablePaths.doneFile);

	SourceTable table = { .oid = oid };
	CopyTableSummary tableSummary = { .table = &table };

	if (!read_table_summary(&tableSummary, tablePaths.doneFile))
	{
		/* errors have already been logged */
		return false;
	}

	PGSQL dst = { 0 };

	/* initialize our connection to the target database */
	if (!pgsql_init(&dst, specs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* finally, vacuum analyze the table and its indexes */
	char vacuum[BUFSIZE] = { 0 };

	sformat(vacuum, sizeof(vacuum),
			"VACUUM ANALYZE \"%s\".\"%s\"",
			table.nspname,
			table.relname);

	log_info("%s;", vacuum);

	if (!pgsql_execute(&dst, vacuum))
	{
		/* errors have already been logged */
		return false;
	}

	(void) pgsql_finish(&dst);

	return true;
}


/*
 * vacuum_add_table sends a message to the VACUUM process queue to process
 * given table.
 */
bool
vacuum_add_table(CopyDataSpec *specs, CopyTableDataSpec *tableSpecs)
{
	QMessage mesg = {
		.type = QMSG_TYPE_TABLEOID,
		.data.oid = tableSpecs->sourceTable->oid
	};

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
