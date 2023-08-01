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


#define MAX_BLOB_PER_FETCH 1000

typedef struct BlobMetadataArray
{
	int count;
	Oid oids[MAX_BLOB_PER_FETCH];
} BlobMetadataArray;

typedef struct BlobMetadataArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	BlobMetadataArray array;
	bool parsedOk;
} BlobMetadataArrayContext;

void parseBlobMetadataArray(void *ctx, PGresult *result);


/*
 * copydb_start_blob_process starts a process that fetches the large object
 * metadata and fills-in a queue, and starts a number of processes that consume
 * large object OIDs from the queue and copy the contents over.
 */
bool
copydb_start_blob_process(CopyDataSpec *specs)
{
	if (specs->skipLargeObjects)
	{
		log_info("Skipping large objects, per --skip-blobs");
		return true;
	}

	if (!queue_create(&(specs->loQueue), "blob"))
	{
		log_error("Failed to create the Large Objects process queue");
		return false;
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
			log_error("Failed to fork large objects process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			CopyBlobsSummary summary = {
				.pid = getpid(),
				.count = 0,
				.startTime = time(NULL)
			};

			instr_time startTime;
			INSTR_TIME_SET_CURRENT(startTime);

			if (!copydb_start_blob_workers(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/* now append BLOB OIDs to the queue */
			uint64_t count = 0;

			if (!copydb_queue_largeobject_metadata(specs, &count))
			{
				log_error("Failed to add large object metadata to the queue");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			if (!copydb_send_lo_stop(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			if (!copydb_wait_for_subprocesses(specs->failFast))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			instr_time duration;

			INSTR_TIME_SET_CURRENT(duration);
			INSTR_TIME_SUBTRACT(duration, startTime);

			/* and write that we successfully finished copying all blobs */
			summary.doneTime = time(NULL);
			summary.durationMs = INSTR_TIME_GET_MILLISEC(duration);

			/* ignore errors on the blob file summary */
			(void) write_blobs_summary(&summary, specs->cfPaths.done.blobs);

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
 * copydb_start_blob_workers starts an auxilliary process that copies the large
 * objects (blobs) from the source database into the target database.
 */
bool
copydb_start_blob_workers(CopyDataSpec *specs)
{
	if (specs->skipLargeObjects)
	{
		return true;
	}

	log_info("STEP 5: starting %d Large Objects workers", specs->lObjectJobs);

	for (int i = 0; i < specs->lObjectJobs; i++)
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
				log_error("Failed to fork large objects worker process: %m");
				return false;
			}

			case 0:
			{
				/* child process runs the command */
				if (!copydb_blob_worker(specs))
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

	/* now we're done, and we want async behavior, do not wait */
	return true;
}


/*
 * copydb_blob_worker is a worker process that loops over messages received
 * from a queue, each message being the Oid of a large object to copy over to
 * the target database.
 */
bool
copydb_blob_worker(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started Large Objects worker %d [%d]", pid, getppid());

	/* connect once to the source database for the whole process */
	if (!copydb_set_snapshot(specs))
	{
		/* errors have already been logged */
		return false;
	}

	int errors = 0;
	bool stop = false;

	while (!stop)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(&(specs->loQueue), &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("Large Objects worker has been interrupted");
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
				log_debug("Stop message received by Large Objects worker");
				break;
			}

			case QMSG_TYPE_BLOBOID:
			{
				if (!copydb_copy_blob_by_oid(specs, mesg.data.oid))
				{
					if (specs->failFast)
					{
						log_error("Failed to copy Large Object with oid %u, "
								  "see above for details",
								  mesg.data.oid);
						return false;
					}

					++errors;
				}
				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on "
						  "Large Objects queue %d",
						  mesg.type,
						  specs->loQueue.qId);
				break;
			}
		}
	}

	/* terminate our connection to the source database now */
	(void) copydb_close_snapshot(specs);

	bool success = (stop == true && errors == 0);

	if (errors > 0)
	{
		log_error("Large Objects worker %d encountered %d errors, "
				  "see above for details",
				  pid,
				  errors);
	}

	return success;
}


/*
 * copydb_copy_blob_by_oid copies the data for given Large Object.
 */
bool
copydb_copy_blob_by_oid(CopyDataSpec *specs, uint32_t oid)
{
	PGSQL *src = &(specs->sourceSnapshot.pgsql);
	PGSQL dst = { 0 };

	bool dropIfExists = specs->restoreOptions.dropIfExists;

	/* initialize our connection to the target database */
	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pg_copy_large_object(src, &dst, dropIfExists, oid))
	{
		log_error("Failed to copy large object %u", oid);
		return false;
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_add_blob sends a message to the Large Object process queue to process
 * given blob.
 */
bool
copydb_add_blob(CopyDataSpec *specs, uint32_t oid)
{
	QMessage mesg = {
		.type = QMSG_TYPE_BLOBOID,
		.data.oid = oid
	};

	log_debug("copydb_add_blob(%d): %u", specs->loQueue.qId, oid);

	if (!queue_send(&(specs->loQueue), &mesg))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_send_lo_stop sends the STOP message to the Large Objects workers.
 *
 * Each worker will consume one STOP message before stopping, so we need to
 * send as many STOP messages as we have started worker processes.
 */
bool
copydb_send_lo_stop(CopyDataSpec *specs)
{
	if (specs->skipLargeObjects)
	{
		return true;
	}

	for (int i = 0; i < specs->lObjectJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP, .data.oid = 0 };

		log_debug("Send STOP message to Large Object queue %d",
				  specs->loQueue.qId);

		if (!queue_send(&(specs->loQueue), &stop))
		{
			/* errors have already been logged */
			continue;
		}
	}

	return true;
}


/*
 * copydb_fetch_largeobject_metadata fetches large object metadata.
 */
bool
copydb_queue_largeobject_metadata(CopyDataSpec *specs, uint64_t *count)
{
	PGSQL *src = &(specs->sourceSnapshot.pgsql);

	/* initialize our connection to the source database */
	if (!pgsql_init(src, specs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(src))
	{
		/* errors have already been logged */
		return false;
	}

	BlobMetadataArrayContext context = { 0 };
	char *sql =
		"DECLARE bloboid CURSOR FOR "
		"SELECT oid FROM pg_largeobject_metadata ORDER BY 1";

	if (!pgsql_execute(src, sql))
	{
		/* errors have already been logged */
		return false;
	}

	*count = 0;

	/* break out of the loop when FETCH returns 0 rows */
	for (;;)
	{
		/* Do a fetch */
		char fetchSQL[BUFSIZE] = { 0 };

		sformat(fetchSQL, sizeof(fetchSQL),
				"FETCH %d IN bloboid",
				MAX_BLOB_PER_FETCH);

		if (!pgsql_execute_with_params(src, fetchSQL, 0, NULL, NULL,
									   &context, &parseBlobMetadataArray))
		{
			/* errors have already been logged */
			return false;
		}

		if (context.array.count == 0)
		{
			break;
		}

		log_debug("Queuing %d large objects", context.array.count);

		*count += context.array.count;

		for (int i = 0; i < context.array.count; i++)
		{
			Oid blobOid = context.array.oids[i];

			if (!copydb_add_blob(specs, blobOid))
			{
				log_error("Failed to queue Large Object %u, "
						  "see above for details",
						  blobOid);
				(void) pgsql_finish(src);
				return false;
			}
		}
	}

	if (!pgsql_commit(src))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Added %lld large objects to the queue", (long long) *count);

	return true;
}


/*
 * parseBlobMetadataArray parses the resultset from a FETCH on the cursor for
 * the large object metadata.
 */
void
parseBlobMetadataArray(void *ctx, PGresult *result)
{
	BlobMetadataArrayContext *context = (BlobMetadataArrayContext *) ctx;

	if (PQnfields(result) != 1)
	{
		log_error("Query returned %d columns, expected 1", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	context->array.count = PQntuples(result);

	for (int i = 0; i < context->array.count; i++)
	{
		char *value = PQgetvalue(result, i, 0);

		if (!stringToUInt32(value, &(context->array.oids[i])))
		{
			log_error("Invalid OID \"%s\"", value);

			context->parsedOk = false;
			return;
		}
	}
}
