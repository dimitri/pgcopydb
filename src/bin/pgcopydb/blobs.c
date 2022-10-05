/*
 * src/bin/pgcopydb/blobs.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "copydb.h"
#include "log.h"
#include "schema.h"
#include "signals.h"

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

	log_info("STEP 5: copy Large Objects (BLOBs) in 1 sub-process");

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
			log_error("Failed to fork a worker process: %m");
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

	log_notice("Started BLOB worker %d [%d]", getpid(), getppid());

	PGSQL *src = NULL;
	PGSQL pgsql = { 0 };
	PGSQL dst = { 0 };

	TransactionSnapshot snapshot = { 0 };

	if (specs->consistent)
	{
		/*
		 * In the context of the `pgcopydb copy blobs` command, we want to
		 * re-use the already prepared snapshot.
		 */
		if (specs->section == DATA_SECTION_BLOBS)
		{
			src = &(specs->sourceSnapshot.pgsql);
		}
		else
		{
			/*
			 * In the context of a full copy command, we want to re-use the
			 * already exported snapshot and make sure to use a private PGSQL
			 * client connection instance.
			 */
			if (!copydb_copy_snapshot(specs, &snapshot))
			{
				/* errors have already been logged */
				return false;
			}

			/* swap the new instance in place of the previous one */
			specs->sourceSnapshot = snapshot;

			src = &(specs->sourceSnapshot.pgsql);

			if (!copydb_set_snapshot(specs))
			{
				/* errors have already been logged */
				return false;
			}
		}
	}
	else
	{
		/*
		 * In the context of --not-consistent we don't have an already
		 * established snapshot to set nor a connection to piggyback onto, so
		 * we have to initialize our client connection now.
		 */
		if (!pgsql_init(&pgsql, specs->source_pguri, PGSQL_CONN_SOURCE))
		{
			/* errors have already been logged */
			return false;
		}

		src = &pgsql;

		if (!pgsql_begin(src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pgsql_init(&dst, specs->target_pguri, PGSQL_CONN_TARGET))
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
	if (specs->consistent)
	{
		if (specs->section != DATA_SECTION_BLOBS)
		{
			if (!copydb_close_snapshot(specs))
			{
				/* errors have already been logged */
				return false;
			}
		}
	}
	else
	{
		if (!pgsql_commit(src))
		{
			/* errors have already been logged */
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
