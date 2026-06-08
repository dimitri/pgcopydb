/*
 * src/bin/pgcopydb/indexes.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

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


static bool copydb_add_table_indexes_hook(void *context, SourceIndex *index);
static bool copydb_collect_constraint_indexes_hook(void *ctx, SourceIndex *index);
static bool copydb_copy_all_indexes_hook(void *ctx, SourceIndex *index);
static bool copydb_queue_deferred_index_hook(void *ctx, SourceIndex *index);


/*
 * copydb_start_index_supervisor starts a CREATE INDEX supervisor process.
 * When supervisorPID is not NULL, the forked PID is stored there so the
 * caller can wait for it specifically (avoiding waitpid(-1) which would
 * reap unrelated children such as the follow process).
 */
bool
copydb_start_index_supervisor(CopyDataSpec *specs, pid_t *supervisorPID)
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
			(void) set_ps_title("pgcopydb: index supervisor");

			if (!copydb_index_supervisor(specs))
			{
				log_error("Failed to create indexes, see above for details");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			if (supervisorPID != NULL)
			{
				*supervisorPID = fpid;
			}
			break;
		}
	}

	/* now we're done, and we want async behavior, do not wait */
	return true;
}


/*
 * copydb_index_supervisor starts the create index workers and does the
 * waitpid() dance for them.
 */
bool
copydb_index_supervisor(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started INDEX supervisor %d [%d]", pid, getppid());

	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!catalog_open(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Start cumulative sections timings for indexes and constraints
	 */
	if (!summary_start_timing(sourceDB, TIMING_SECTION_CREATE_INDEX))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_start_timing(sourceDB, TIMING_SECTION_ALTER_TABLE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_start_index_workers(specs))
	{
		log_error("Failed to start index workers, see above for details");
		return false;
	}

	/*
	 * Now just wait for the create index processes to be done.
	 */
	if (!copydb_wait_for_subprocesses(specs->failFast))
	{
		log_error("Some INDEX worker process(es) have exited with error, "
				  "see above for details");

		if (specs->failFast)
		{
			(void) copydb_fatal_exit();
		}
		else
		{
			/* send vacuum workers a STOP message */
			if (!vacuum_send_stop(specs))
			{
				(void) copydb_fatal_exit();
			}
		}

		return false;
	}

	/*
	 * Send the STOP message to the VACUUM ANALYZE workers, so they can stop
	 * processing the tables.
	 */
	if (!vacuum_send_stop(specs))
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

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_CREATE_INDEX))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_ALTER_TABLE))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_start_index_workers create as many sub-process as needed, per
 * --index-jobs.
 */
bool
copydb_start_index_workers(CopyDataSpec *specs)
{
	log_info("STEP 6: starting %d CREATE INDEX processes", specs->indexJobs);
	log_info("STEP 7: constraints are built by the CREATE INDEX processes");

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
				log_error("Failed to fork a create index worker process: %m");
				return false;
			}

			case 0:
			{
				/* child process runs the command */
				(void) set_ps_title("pgcopydb: create index worker");

				if (!copydb_index_worker(specs))
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
 * copydb_index_worker is a worker process that loops over messages received
 * from a queue, each message being the Oid of an index to create on the target
 * database.
 */
bool
copydb_index_worker(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started CREATE INDEX worker %d [%d]", pid, getppid());

	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to open internal catalogs in CREATE INDEX worker, "
				  "see above for details");
		return false;
	}

	PGSQL dst = { 0 };
	char *pguri = specs->connStrings.target_pguri;

	if (!pgsql_init(&dst, (char *) pguri, PGSQL_CONN_TARGET))
	{
		return false;
	}

	/* also set our GUC values for the target connection */
	if (!pgsql_set_gucs(&dst, dstSettings))
	{
		log_fatal("Failed to set our GUC settings on the target connection, "
				  "see above for details");
		return false;
	}

	int errors = 0;
	bool stop = false;

	while (!stop)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(&(specs->indexQueue), &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("CREATE INDEX worker has been interrupted");
			(void) pgsql_finish(&dst);
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
				log_debug("Stop message received by create index worker");
				break;
			}

			case QMSG_TYPE_INDEXOID:
			{
				if (!copydb_create_index_by_oid(specs, &dst, mesg.data.oid))
				{
					++errors;

					log_error("Failed to create index with oid %u, "
							  "see above for details",
							  mesg.data.oid);

					if (specs->failFast)
					{
						(void) pgsql_finish(&dst);
						return false;
					}
				}
				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on index queue %d",
						  mesg.type,
						  specs->indexQueue.qId);
				break;
			}
		}
	}

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

	bool success = (stop == true && errors == 0);

	if (errors > 0)
	{
		log_error("CREATE INDEX worker %d encountered %d errors, "
				  "see above for details",
				  pid,
				  errors);
	}

	return success;
}


/*
 * copydb_create_index_by_oid finds the SourceIndex entry by its OID and then
 * creates the index on the target database.
 */
bool
copydb_create_index_by_oid(CopyDataSpec *specs, PGSQL *dst, uint32_t indexOid)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	SourceTable *table = (SourceTable *) calloc(1, sizeof(SourceTable));
	SourceIndex *index = (SourceIndex *) calloc(1, sizeof(SourceIndex));

	if (!catalog_lookup_s_index(sourceDB, indexOid, index))
	{
		log_error("Failed to lookup index %u in our catalogs", indexOid);
		return false;
	}

	if (!catalog_lookup_s_table(sourceDB, index->tableOid, 0, table))
	{
		log_error("Failed to lookup table %u in our catalogs", index->tableOid);
		return false;
	}

	log_trace("copydb_create_index_by_oid: %u %s on %s",
			  indexOid,
			  index->indexQname,
			  table->qname);

	char psTitle[BUFSIZE] = { 0 };

	sformat(psTitle, sizeof(psTitle), "pgcopydb: create index %s",
			index->indexQname);

	(void) set_ps_title(psTitle);

	/* also track the process information in our catalogs */
	ProcessInfo ps = {
		.pid = getpid(),
		.psType = "CREATE INDEX",
		.psTitle = ps_buffer,
		.indexOid = index->indexOid
	};

	if (!catalog_upsert_process_info(sourceDB, &ps))
	{
		log_error("Failed to track progress in our catalogs, "
				  "see above for details");
		return false;
	}

	/*
	 * Add IF NOT EXISTS clause when the --resume option has been used, or when
	 * the command is `pgcopydb copy indexes`, in which cases we don't know
	 * what to expect on the target database.
	 */
	bool ifNotExists =
		specs->resume || specs->section == DATA_SECTION_INDEXES;

	if (!copydb_create_index(specs, dst, index, ifNotExists))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now if that was the last index built for a given table, it's time to
	 * also create the constraints associated with the indexes. We wait until
	 * all the indexes are done because constraints are built with ALTER TABLE,
	 * which takes an exclusive lock on the table.
	 */
	bool builtAllIndexes = false;
	bool constraintsAreBeingBuilt = false;

	if (!copydb_table_indexes_are_done(specs,
									   table,
									   &builtAllIndexes,
									   &constraintsAreBeingBuilt))
	{
		/* errors have already been logged */
		return false;
	}

	if (builtAllIndexes && !constraintsAreBeingBuilt)
	{
		/*
		 * Once the indexes are built, it's time to:
		 *
		 *  1. build the constraints, some of them on-top of the indexes
		 *  2. send the table to the VACUUM ANALYZE job queue.
		 */

		if (!copydb_create_constraints(specs, dst, table))
		{
			log_error("Failed to create constraints for table %s",
					  table->qname);
			return false;
		}

		if (!specs->skipVacuum && !specs->deferAnalyze)
		{
			if (!vacuum_add_table(specs, table->oid))
			{
				log_error("Failed to queue VACUUM ANALYZE %s [%u]",
						  table->qname,
						  table->oid);
				return false;
			}
		}
	}


	return true;
}


/*
 * copydb_table_indexes_are_done checks that all indexes for a given table have
 * been built already.
 */
bool
copydb_table_indexes_are_done(CopyDataSpec *specs,
							  SourceTable *table,
							  bool *indexesAreDone,
							  bool *constraintsAreBeingBuilt)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	/* until proven otherwise... */
	*indexesAreDone = false;

	CopyTableDataSpec tableSpecs = { 0 };

	if (!copydb_init_table_specs(&tableSpecs, specs, table, 0))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_table_count_indexes_left(sourceDB, &tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * If all indexes are done, try and register this worker's PID as the first
	 * worker that saw the situation. Only that one is allowed process the
	 * constraints.
	 */
	if (tableSpecs.countIndexesLeft == 0)
	{
		*indexesAreDone = true;

		/* insert or ignore our pid as the partsDonePid */
		if (!summary_add_table_indexes_done(sourceDB, &tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		if (!summary_lookup_table_indexes_done(sourceDB, &tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * Set constraintsAreBeingBuilt to false to allow the current worker to
		 * process constraints.
		 */
		*constraintsAreBeingBuilt = (tableSpecs.indexesDonePid != getpid());
	}

	return true;
}


typedef struct IndexOIDArray
{
	int count;
	uint32_t *array;            /* malloc'ed area */
} IndexOIDArray;

typedef struct QueueTableIndexesContext
{
	CopyDataSpec *specs;
	CopyTableDataSpec *tableSpecs;
	IndexOIDArray *indexArray;
} QueueTableIndexesContext;

/*
 * copydb_add_table_indexes sends a message to the CREATE INDEX process queue
 * to process indexes attached to the given table.
 */
bool
copydb_add_table_indexes(CopyDataSpec *specs, CopyTableDataSpec *tableSpecs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	IndexOIDArray indexArray = { 0, NULL };

	if (!catalog_s_table_count_indexes(sourceDB, tableSpecs->sourceTable))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * While this COPY process is holding a catalog lock to iterate over the
	 * indexes of the given table, CREATE INDEX processes are attempting to
	 * grab the same catalog lock in order to fetch the metadata for the OID
	 * received from the queue.
	 *
	 * To avoid grabbing the SQLite semaphore while doing the queue_send()
	 * operation, we allocate an array of indexes OIDs in memory and fill the
	 * information from the iterator callback function.
	 *
	 * That's important because the queue_send() operation contains a retry
	 * loop in case of errors, one possible error is EGAIN (queue is full). We
	 * need to make sure that the CREATE INDEX processes are able to empty the
	 * queue (and thus grab the catalog lock) during the queue_send() retry
	 * loop.
	 */
	int indexCount = tableSpecs->sourceTable->indexCount;
	indexArray.count = 0;
	indexArray.array = (uint32_t *) calloc(indexCount, sizeof(uint32_t));

	if (indexArray.array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	QueueTableIndexesContext context = {
		.specs = specs,
		.tableSpecs = tableSpecs,
		.indexArray = &indexArray
	};

	/* iterate over SQLite results and fill-in the indexArray */
	if (!catalog_iter_s_index_table(sourceDB,
									tableSpecs->sourceTable->nspname,
									tableSpecs->sourceTable->relname,
									&context,
									&copydb_add_table_indexes_hook))
	{
		log_error("Failed to send table %s indexes to create index queue, "
				  "see above for details",
				  tableSpecs->sourceTable->qname);
		return false;
	}

	/*
	 * Now that we are no longer holding the catalog lock, walk through the
	 * indexArray and send index OIDs to the create index queue, retrying the
	 * operation if the queue is full.
	 */
	for (int i = 0; i < indexArray.count; i++)
	{
		QMessage mesg = {
			.type = QMSG_TYPE_INDEXOID,
			.data.oid = indexArray.array[i]
		};

		log_trace("Queueing index [%u] for table %s [%u]",
				  mesg.data.oid,
				  tableSpecs->sourceTable->qname,
				  tableSpecs->sourceTable->oid);

		if (!queue_send(&(specs->indexQueue), &mesg))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_add_table_indexes_hook is an iterator callback function.
 */
static bool
copydb_add_table_indexes_hook(void *ctx, SourceIndex *index)
{
	QueueTableIndexesContext *context = (QueueTableIndexesContext *) ctx;
	IndexOIDArray *indexArray = context->indexArray;

	indexArray->array[(indexArray->count)++] = index->indexOid;

	return true;
}


/*
 * copydb_index_workers_send_stop sends the STOP message to the CREATE INDEX
 * workers.
 *
 * Each worker will consume one STOP message before stopping, so we need to
 * send as many STOP messages as we have started worker processes.
 */
bool
copydb_index_workers_send_stop(CopyDataSpec *specs)
{
	for (int i = 0; i < specs->indexJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP, .data.oid = 0 };

		log_debug("Send STOP message to CREATE INDEX queue %d",
				  specs->indexQueue.qId);

		if (!queue_send(&(specs->indexQueue), &stop))
		{
			/* errors have already been logged */
			continue;
		}
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
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (specs->runState.indexCopyIsDone)
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

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count indexes and constraints in our catalogs");
		return false;
	}

	log_info("Creating %lld indexes in the target database using %d processes",
			 (long long) count.indexes,
			 specs->indexJobs);

	/*
	 * Start the index supervisor. When --defer-indexes --follow, track the
	 * supervisor PID so we can wait specifically for it without accidentally
	 * reaping the follow process via waitpid(-1).
	 */
	pid_t supervisorPID = 0;
	bool trackPID = specs->deferIndexes && specs->follow;

	if (!copydb_start_index_supervisor(specs, trackPID ? &supervisorPID : NULL))
	{
		/* errors have already been logged */
		return false;
	}

	/* queue all our indexes for processing by the index workers */
	if (!catalog_iter_s_index(sourceDB, specs, &copydb_copy_all_indexes_hook))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_index_workers_send_stop(specs))
	{
		log_fatal("Failed to send the STOP message in the index queue");
		(void) copydb_fatal_exit();
		return false;
	}

	/*
	 * When --defer-indexes --follow, use targeted waitpid for the supervisor
	 * only. The follow process (PID C) is also a child and must not be reaped
	 * here — it needs to keep running until CDC apply completes.
	 */
	if (trackPID)
	{
		if (!copydb_wait_for_pid(supervisorPID))
		{
			log_error("Index supervisor exited with error status, "
					  "see above for details");
			return false;
		}
	}
	else if (!copydb_wait_for_subprocesses(specs->failFast))
	{
		log_error("Some sub-processes have exited with error status, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_copy_all_indexes_hook is an iterator callback function.
 */
static bool
copydb_copy_all_indexes_hook(void *ctx, SourceIndex *index)
{
	CopyDataSpec *specs = (CopyDataSpec *) ctx;

	QMessage mesg = {
		.type = QMSG_TYPE_INDEXOID,
		.data.oid = index->indexOid
	};

	log_trace("Queueing index %s [%u]", index->indexQname, index->indexOid);

	if (!queue_send(&(specs->indexQueue), &mesg))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_queue_deferred_index_hook is an iterator callback function used
 * when --defer-indexes is set to queue all indexes after COPY completes.
 */
static bool
copydb_queue_deferred_index_hook(void *ctx, SourceIndex *index)
{
	CopyDataSpec *specs = (CopyDataSpec *) ctx;

	QMessage mesg = {
		.type = QMSG_TYPE_INDEXOID,
		.data.oid = index->indexOid
	};

	log_trace("Queueing deferred index %s [%u]",
			  index->indexQname, index->indexOid);

	if (!queue_send(&(specs->indexQueue), &mesg))
	{
		return false;
	}

	return true;
}


/*
 * copydb_queue_all_deferred_indexes queues all indexes for building after
 * the COPY phase has completed. Used when --defer-indexes is set.
 */
bool
copydb_queue_all_deferred_indexes(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	CatalogCounts count = { 0 };

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count indexes in our catalogs");
		return false;
	}

	log_info("Queueing %lld deferred indexes",
			 (long long) count.indexes);

	if (!catalog_iter_s_index(sourceDB,
							  specs,
							  &copydb_queue_deferred_index_hook))
	{
		log_error("Failed to queue deferred indexes");
		return false;
	}

	return true;
}


/*
 * copydb_create_index creates given index.
 */
bool
copydb_create_index(CopyDataSpec *specs,
					PGSQL *dst,
					SourceIndex *index,
					bool ifNotExists)
{
	CopyIndexSpec indexSpecs = { .sourceIndex = index };
	CopyIndexSummary *indexSummary = &(indexSpecs.summary);

	bool isConstraintIndex = index->constraintOid != 0;
	bool skipCreateIndex = false;

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
	if (isConstraintIndex && !index->isPrimary && !index->isUnique)
	{
		skipCreateIndex = true;
		log_notice("Skipping concurrent build of index "
				   "%s for constraint %s on %s, "
				   "it is not a UNIQUE or a PRIMARY constraint",
				   index->indexQname,
				   index->constraintDef,
				   index->tableQname);
	}

	bool isDone = false;

	if (!copydb_index_is_being_processed(specs, &indexSpecs, &isDone))
	{
		/* errors have already been logged */
		return false;
	}

	if (isDone)
	{
		log_debug("Skipping index %s which is being created by another process",
				  index->indexQname);
		return true;
	}

	if (!skipCreateIndex)
	{
		/*
		 * Prepare the CREATE INDEX command based on the index definition and
		 * ifNotExists flag.
		 */
		if (!copydb_prepare_create_index_command(&indexSpecs, ifNotExists))
		{
			/* errors have already been logged */
			return false;
		}

		log_notice("%s", indexSummary->command);

		if (!pgsql_execute(dst, indexSummary->command))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!copydb_mark_index_as_done(specs, &indexSpecs))
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
copydb_index_is_being_processed(CopyDataSpec *specs,
								CopyIndexSpec *indexSpecs,
								bool *isDone)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!summary_lookup_index(sourceDB, indexSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	CopyIndexSummary *indexSummary = &(indexSpecs->summary);

	if (indexSummary->doneTime > 0)
	{
		*isDone = true;
		return true;
	}

	if (indexSummary->pid != 0)
	{
		/* if we can signal the pid, it is still running */
		if (kill(indexSummary->pid, 0) == 0)
		{
			log_error("Failed to start CREATE INDEX worker for index %s (%u), "
					  "already being processed by pid %d",
					  indexSpecs->sourceIndex->indexQname,
					  indexSpecs->sourceIndex->indexOid,
					  indexSummary->pid);

			return false;
		}
		else
		{
			log_notice("Found stale pid %d removing it to process index %s",
					   indexSummary->pid,
					   indexSpecs->sourceIndex->indexQname);

			/* stale pid, remove the summary entry and process the index */
			if (!summary_delete_index(sourceDB, indexSpecs))
			{
				/* errors have already been logged */
				return false;
			}

			/* pass through to the rest of this function */
		}
	}

	if (!summary_add_index(sourceDB, indexSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_mark_index_as_done creates the table doneFile with the expected
 * summary content. To create a doneFile we must acquire the synchronisation
 * semaphore first. The lockFile is also removed here.
 */
bool
copydb_mark_index_as_done(CopyDataSpec *specs, CopyIndexSpec *indexSpecs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!summary_finish_index(sourceDB, indexSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_increment_timing(sourceDB,
								  TIMING_SECTION_CREATE_INDEX,
								  1, /* count */
								  0, /* bytes */
								  indexSpecs->summary.durationMs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_prepare_create_index_command prepares the SQL command to use to
 * create a given index. When ifNotExists is true the IF NOT EXISTS keywords
 * are added to the command, necessary to resume operations in some cases.
 */
bool
copydb_prepare_create_index_command(CopyIndexSpec *indexSpecs, bool ifNotExists)
{
	PQExpBuffer cmd = createPQExpBuffer();

	SourceIndex *index = indexSpecs->sourceIndex;

	/* prepare the create index command, maybe adding IF NOT EXISTS */
	if (ifNotExists)
	{
		int ci_len = strlen("CREATE INDEX ");
		int cu_len = strlen("CREATE UNIQUE INDEX ");

		if (strncmp(index->indexDef, "CREATE INDEX ", ci_len) == 0)
		{
			appendPQExpBuffer(cmd,
							  "CREATE INDEX IF NOT EXISTS %s;",
							  index->indexDef + ci_len);
		}
		else if (strncmp(index->indexDef, "CREATE UNIQUE INDEX ", cu_len) == 0)
		{
			appendPQExpBuffer(cmd,
							  "CREATE UNIQUE INDEX IF NOT EXISTS %s;",
							  index->indexDef + cu_len);
		}
		else
		{
			log_error("Failed to parse \"%s\"", index->indexDef);
			destroyPQExpBuffer(cmd);
			return false;
		}
	}
	else
	{
		/*
		 * Just use the pg_get_indexdef() command, with an added semi-colon for
		 * logging clarity.
		 */
		appendPQExpBuffer(cmd, "%s;", index->indexDef);
	}

	if (PQExpBufferBroken(cmd))
	{
		log_error("Failed to create query for CREATE INDEX \"%s\": out of memory",
				  index->indexRelname);
		destroyPQExpBuffer(cmd);
		return false;
	}

	indexSpecs->summary.command = strdup(cmd->data);

	destroyPQExpBuffer(cmd);

	return true;
}


/*
 * copydb_prepare_create_constraint_command prepares the SQL command to use to
 * create the given constraint on-top of an already existing Index.
 */
bool
copydb_prepare_create_constraint_command(CopyIndexSpec *indexSpecs)
{
	PQExpBuffer cmd = createPQExpBuffer();

	SourceIndex *index = indexSpecs->sourceIndex;

	if (index->isPrimary || index->isUnique)
	{
		char *constraintType = index->isPrimary ? "PRIMARY KEY" : "UNIQUE";

		appendPQExpBuffer(cmd,
						  "ALTER TABLE %s "
						  "ADD CONSTRAINT %s %s "
						  "USING INDEX %s",
						  index->tableQname,
						  index->constraintName,
						  constraintType,
						  index->indexRelname);
	}
	else
	{
		appendPQExpBuffer(cmd,
						  "ALTER TABLE %s "
						  "ADD CONSTRAINT %s %s ",
						  index->tableQname,
						  index->constraintName,
						  index->constraintDef);
	}

	if (index->condeferrable)
	{
		appendPQExpBufferStr(cmd, " DEFERRABLE");

		if (index->condeferred)
		{
			appendPQExpBufferStr(cmd, " INITIALLY DEFERRED");
		}
	}

	if (PQExpBufferBroken(cmd))
	{
		log_error("Failed to create query for CONSTRAINT \"%s\": out of memory",
				  index->constraintName);
		destroyPQExpBuffer(cmd);
		return false;
	}

	indexSpecs->summary.command = strdup(cmd->data);

	destroyPQExpBuffer(cmd);

	return true;
}


/*
 * copydb_create_constraints loops over the index definitions for a given table
 * and creates all the associated constraints, one after the other.
 *
 * We use a collect-then-execute pattern here: first we iterate over the source
 * catalog (which holds the catalog semaphore) and collect constraint indexes
 * into an in-memory array. Then, after the iterator returns and the semaphore
 * is released, we create each constraint. This avoids holding the catalog
 * semaphore during potentially long-running ALTER TABLE ... ADD CONSTRAINT
 * commands on the target database, which would block all other workers.
 *
 * This is the same pattern used in copydb_add_table_indexes().
 */
bool
copydb_create_constraints(CopyDataSpec *specs, PGSQL *dst, SourceTable *table)
{
	bool success = true;

	/*
	 * Postgres doesn't implement ALTER TABLE ... ADD CONSTRAINT ... IF NOT
	 * EXISTS, which we would be using here in some cases otherwise.
	 *
	 * When --resume is used, for instance, the previous run could have been
	 * interrupted after a constraint creation on the target database, but
	 * before the creation of its constraintDoneFile.
	 */
	DatabaseCatalog *targetDB = &(specs->catalogs.target);

	/* have a copy of the source table to edit indexCount etc */
	SourceTable *targetTable = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (targetTable == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* copy the structure contents over */
	*targetTable = *table;

	if (!catalog_s_table_count_indexes(targetDB, targetTable))
	{
		log_error("Failed to count indexes for table %s in our target catalog",
				  targetTable->qname);
		return false;
	}

	if (targetTable->indexCount > 0)
	{
		/*
		 * It's expected that we find indexes on the target database when
		 * running the pgcopydb clone command: we just created them before
		 * reaching to the constraint code.
		 *
		 * When running pgcopydb create constraints, that information is more
		 * relevant.
		 */
		int logLevel =
			specs->section == DATA_SECTION_ALL ? LOG_NOTICE : LOG_INFO;

		log_level(logLevel,
				  "Found %lld indexes on target database for table %s",
				  (long long) targetTable->indexCount,
				  table->qname);
	}


	/*
	 * Phase 1: Collect constraint indexes while holding the catalog semaphore.
	 *
	 * The catalog_iter_s_index_table() function acquires the source catalog
	 * semaphore for the duration of the iteration. We only do fast in-memory
	 * copies here so the semaphore is held briefly.
	 */
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	SourceIndexArray indexArray = { 0, 0, NULL };

	if (!catalog_iter_s_index_table(sourceDB,
									table->nspname,
									table->relname,
									&indexArray,
									&copydb_collect_constraint_indexes_hook))
	{
		/* errors have already been logged */
		free(indexArray.array);
		return false;
	}

	/*
	 * Phase 2: Create constraints without holding the catalog semaphore.
	 *
	 * Now iterate over the collected array and run ALTER TABLE ... ADD
	 * CONSTRAINT on the target database. These commands may be slow (e.g.
	 * EXCLUDE USING gist on a large table), but other workers can freely
	 * access the source catalog while we wait.
	 */
	for (int i = 0; i < indexArray.count; i++)
	{
		SourceIndex *index = &(indexArray.array[i]);

		CopyIndexSpec indexSpecs = { .sourceIndex = index };
		CopyIndexSummary *indexSummary = &(indexSpecs.summary);

		if (!copydb_prepare_create_constraint_command(&indexSpecs))
		{
			log_warn("Failed to prepare SQL command to create "
					 "constraint \"%s\"",
					 index->constraintName);
			success = false;
			break;
		}

		if (!summary_add_constraint(sourceDB, &indexSpecs))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		/* skip constraints that already exist on the target database */
		SourceIndex *targetIndex =
			(SourceIndex *) calloc(1, sizeof(SourceIndex));

		if (targetIndex == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			success = false;
			break;
		}

		if (!catalog_lookup_s_index_by_name(targetDB,
											index->indexNamespace,
											index->indexRelname,
											targetIndex))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		bool foundConstraintOnTarget =
			streq(index->constraintName, targetIndex->constraintName);

		if (!foundConstraintOnTarget)
		{
			log_notice("%s", indexSummary->command);

			/*
			 * Constraints are built by the CREATE INDEX worker process that
			 * is the last one to finish an index for a given table. We do
			 * not have to care about concurrency here: no semaphore locking.
			 */
			if (!pgsql_execute(dst, indexSummary->command))
			{
				/* errors have already been logged */
				success = false;
				break;
			}
		}

		if (!summary_finish_constraint(sourceDB, &indexSpecs))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		if (!summary_increment_timing(sourceDB,
									  TIMING_SECTION_ALTER_TABLE,
									  1, /* count */
									  0, /* bytes */
									  indexSpecs.summary.durationMs))
		{
			/* errors have already been logged */
			success = false;
			break;
		}
	}

	free(indexArray.array);

	return success;
}


/*
 * copydb_collect_constraint_indexes_hook is an iterator callback function that
 * collects indexes with constraints into a SourceIndexArray. This callback runs
 * while the catalog semaphore is held, so it only does fast in-memory copies.
 */
static bool
copydb_collect_constraint_indexes_hook(void *ctx, SourceIndex *index)
{
	SourceIndexArray *indexArray = (SourceIndexArray *) ctx;

	/* skip indexes that are not attached to a constraint */
	if (index->constraintOid == 0 ||
		IS_EMPTY_STRING_BUFFER(index->constraintName))
	{
		return true;
	}

	/* grow the array if needed */
	if (indexArray->count >= indexArray->capacity)
	{
		int newCap = indexArray->capacity == 0 ? 8 : indexArray->capacity * 2;
		SourceIndex *newArray =
			(SourceIndex *) realloc(indexArray->array,
									newCap * sizeof(SourceIndex));

		if (newArray == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		indexArray->array = newArray;
		indexArray->capacity = newCap;
	}

	/* deep copy the SourceIndex struct (all fields are fixed-size buffers) */
	indexArray->array[indexArray->count] = *index;
	indexArray->count++;

	return true;
}


/*
 * copydb_collect_fk_constraints_hook is an iterator callback that collects
 * FK constraints into a SourceFKConstraintArray for later processing.
 */
static bool
copydb_collect_fk_constraints_hook(void *ctx, SourceFKConstraint *fk)
{
	SourceFKConstraintArray *fkArray = (SourceFKConstraintArray *) ctx;

	/* grow the array if needed */
	if (fkArray->count >= fkArray->capacity)
	{
		int newCap = fkArray->capacity == 0 ? 8 : fkArray->capacity * 2;
		SourceFKConstraint *newArray =
			(SourceFKConstraint *) realloc(fkArray->array,
										   newCap * sizeof(SourceFKConstraint));

		if (newArray == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		fkArray->array = newArray;
		fkArray->capacity = newCap;
	}

	/* copy the struct (constraintDef pointer needs to be strdup'd) */
	fkArray->array[fkArray->count] = *fk;

	if (fk->constraintDef != NULL)
	{
		fkArray->array[fkArray->count].constraintDef = strdup(fk->constraintDef);

		if (fkArray->array[fkArray->count].constraintDef == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}
	}

	fkArray->count++;

	return true;
}


/*
 * copydb_create_fk_constraints creates all FK constraints that were fetched
 * from the source catalog. For each constraint:
 *
 *  1. Try: ALTER TABLE <table> ADD CONSTRAINT <name> <def>
 *  2. On SQLSTATE 23503: retry with NOT VALID appended.
 *  3. Record result in summary catalog.
 *
 * FK constraints are handled separately from pg_restore to allow per-constraint
 * error handling and automatic NOT VALID retry.
 */
bool
copydb_create_fk_constraints(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	/*
	 * Phase 1: Collect FK constraints while holding the catalog semaphore.
	 */
	SourceFKConstraintArray fkArray = { 0, 0, NULL };

	if (!catalog_iter_s_fk_constraint(sourceDB,
									  &fkArray,
									  &copydb_collect_fk_constraints_hook))
	{
		/* errors have already been logged */
		free(fkArray.array);
		return false;
	}

	if (fkArray.count == 0)
	{
		log_info("No FK constraints to create");
		return true;
	}

	log_info("Creating %d FK constraints", fkArray.count);

	/*
	 * Phase 2: Create FK constraints on the target.
	 */
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		log_error("Failed to initialize connection to target database");
		free(fkArray.array);
		return false;
	}

	bool success = true;
	int notValidCount = 0;
	int sourceNotValidCount = 0;
	int deferredNotValidCount = 0;
	int skippedCount = 0;
	int partitionedSkipCount = 0;

	for (int i = 0; i < fkArray.count; i++)
	{
		SourceFKConstraint *fk = &(fkArray.array[i]);

		/*
		 * Resume support: check if this FK constraint has already been
		 * processed in a previous run.
		 */
		bool alreadyDone = false;

		if (!summary_lookup_fk_constraint(sourceDB, fk->oid, &alreadyDone))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		if (alreadyDone)
		{
			log_notice("Skipping already processed FK constraint \"%s\" on %s",
					   fk->conname, fk->tableQname);
			continue;
		}

		/*
		 * Decide whether this constraint should be created as NOT VALID.
		 *
		 * There are two distinct reasons to skip the validation scan:
		 *
		 *  1. The constraint is already NOT VALID on the source
		 *     (convalidated = false). pg_get_constraintdef() already includes
		 *     NOT VALID in its output for such constraints, so the
		 *     constraintDef carries it; we only log here for clarity.
		 *
		 *  2. The caller asked for --defer-validate-fks, in which case a
		 *     source-validated constraint is created as NOT VALID so the
		 *     (potentially hours-long) validation scan does not block the
		 *     transition into CDC. The operator validates later at their
		 *     convenience with ALTER TABLE ... VALIDATE CONSTRAINT.
		 *
		 * In both cases the constraint still enforces referential integrity
		 * on all new writes (NOT VALID only skips the scan of pre-existing
		 * rows), so CDC replay remains safe: any change replayed from the
		 * source already satisfies the constraint there.
		 */
		bool notValid = false;
		bool appendNotValid = false;

		if (!fk->convalidated)
		{
			log_notice("FK constraint \"%s\" on %s is NOT VALID on source, "
					   "creating as NOT VALID on target",
					   fk->conname, fk->tableQname);
			notValid = true;
			sourceNotValidCount++;
		}
		else if (specs->deferValidateFKs)
		{
			log_notice("Deferring FK validation (--defer-validate-fks): "
					   "creating FK constraint \"%s\" on %s as NOT VALID "
					   "on target",
					   fk->conname, fk->tableQname);
			notValid = true;
			appendNotValid = true;
			deferredNotValidCount++;
		}

		/*
		 * Build the ALTER TABLE ADD CONSTRAINT command.
		 */
		PQExpBuffer cmd = createPQExpBuffer();

		appendPQExpBuffer(cmd,
						  "ALTER TABLE %s ADD CONSTRAINT %s %s",
						  fk->tableQname,
						  fk->conname,
						  fk->constraintDef);

		if (fk->condeferrable)
		{
			appendPQExpBufferStr(cmd, " DEFERRABLE");

			if (fk->condeferred)
			{
				appendPQExpBufferStr(cmd, " INITIALLY DEFERRED");
			}
		}

		/*
		 * Source-NOT-VALID constraints already carry NOT VALID inside
		 * constraintDef (from pg_get_constraintdef), so only append it here
		 * when the source was validated and we are deferring validation.
		 */
		if (appendNotValid)
		{
			appendPQExpBufferStr(cmd, " NOT VALID");
		}

		if (PQExpBufferBroken(cmd))
		{
			log_error("Failed to create query for FK constraint \"%s\": "
					  "out of memory", fk->conname);
			destroyPQExpBuffer(cmd);
			success = false;
			break;
		}

		/* record the start of this constraint creation */
		if (!summary_add_fk_constraint(sourceDB, fk, cmd->data))
		{
			/* errors have already been logged */
			destroyPQExpBuffer(cmd);
			success = false;
			break;
		}

		instr_time startTime;
		INSTR_TIME_SET_CURRENT(startTime);

		log_notice("Creating FK constraint: %s", cmd->data);

		if (!pgsql_execute(&dst, cmd->data))
		{
			/*
			 * Check if the failure is due to a foreign key violation
			 * (SQLSTATE 23503). If so, retry with NOT VALID — but only if
			 * the command did not already include NOT VALID, since in that
			 * case a 23503 cannot come from the validation scan and a retry
			 * would not help.
			 */
			if (strcmp(dst.sqlstate,
					   STR_ERRCODE_FOREIGN_KEY_VIOLATION) == 0 && !notValid)
			{
				log_warn("FK constraint \"%s\" on %s has pre-existing data "
						 "violations, retrying with NOT VALID",
						 fk->conname, fk->tableQname);

				/* rebuild the command with NOT VALID */
				resetPQExpBuffer(cmd);

				appendPQExpBuffer(cmd,
								  "ALTER TABLE %s ADD CONSTRAINT %s %s",
								  fk->tableQname,
								  fk->conname,
								  fk->constraintDef);

				if (fk->condeferrable)
				{
					appendPQExpBufferStr(cmd, " DEFERRABLE");

					if (fk->condeferred)
					{
						appendPQExpBufferStr(cmd, " INITIALLY DEFERRED");
					}
				}

				appendPQExpBufferStr(cmd, " NOT VALID");

				/* clear the error state for retry */
				memset(dst.sqlstate, 0, sizeof(dst.sqlstate));

				if (!pgsql_execute(&dst, cmd->data))
				{
					log_error("Failed to create FK constraint \"%s\" on %s "
							  "even with NOT VALID",
							  fk->conname, fk->tableQname);
					destroyPQExpBuffer(cmd);
					success = false;
					break;
				}

				notValid = true;
				notValidCount++;

				log_warn("FK constraint \"%s\" on %s created as NOT VALID "
						 "due to pre-existing data violations",
						 fk->conname, fk->tableQname);
			}
			else if (strcmp(dst.sqlstate,
							STR_ERRCODE_DUPLICATE_OBJECT) == 0)
			{
				/*
				 * The constraint already exists on the target, likely
				 * because it was defined inline in CREATE TABLE and
				 * created during the pre-data pg_restore phase.
				 */
				log_notice("FK constraint \"%s\" on %s already exists "
						   "on target, skipping",
						   fk->conname, fk->tableQname);
				memset(dst.sqlstate, 0, sizeof(dst.sqlstate));
			}
			else if (strcmp(dst.sqlstate,
							STR_ERRCODE_INVALID_SCHEMA_NAME) == 0 ||
					 strcmp(dst.sqlstate,
							STR_ERRCODE_UNDEFINED_TABLE) == 0)
			{
				/*
				 * The referenced table or schema does not exist on the
				 * target, likely because it was filtered out. Skip this
				 * FK constraint gracefully.
				 */
				log_notice("Skipping FK constraint \"%s\" on %s: "
						   "referenced table or schema does not exist "
						   "on target (likely filtered out)",
						   fk->conname, fk->tableQname);
				skippedCount++;
				memset(dst.sqlstate, 0, sizeof(dst.sqlstate));
			}
			else if (strcmp(dst.sqlstate,
							STR_ERRCODE_INVALID_FOREIGN_KEY) == 0)
			{
				/*
				 * There is no unique/PK constraint on the target matching the
				 * referenced columns (SQLSTATE 42830). This typically happens
				 * when the referenced table's unique index was filtered out
				 * while the FK and its table were kept. We cannot create this
				 * FK, but it must not abort the whole migration: skip it and
				 * capture it for the end-of-run summary so the operator can
				 * reconcile it afterwards.
				 */
				log_warn("Skipping FK constraint \"%s\" on %s: no matching "
						 "unique or primary key constraint exists on the "
						 "referenced table on target (likely filtered out)",
						 fk->conname, fk->tableQname);
				skippedCount++;
				memset(dst.sqlstate, 0, sizeof(dst.sqlstate));
			}
			else if (specs->deferValidateFKs && appendNotValid &&
					 strcmp(dst.sqlstate,
							STR_ERRCODE_WRONG_OBJECT_TYPE) == 0)
			{
				/*
				 * PostgreSQL does not support NOT VALID foreign keys declared
				 * on a partitioned table itself (SQLSTATE 42809: "cannot add
				 * NOT VALID foreign key on partitioned table"). We only reach
				 * here because --defer-validate-fks asked us to append NOT
				 * VALID to a source-validated constraint.
				 *
				 * Creating it VALID instead would run the full validation scan
				 * across the (typically very large) partitioned table, which
				 * is exactly the multi-hour block the flag exists to avoid and
				 * would stall the transition into CDC. So, consistent with the
				 * flag's contract, skip it and capture it loudly: the operator
				 * adds it after cutover on their own schedule (e.g. per-leaf
				 * NOT VALID constraints, then VALIDATE). CDC replay stays safe
				 * meanwhile because every change comes from the source, which
				 * already enforces the constraint.
				 */
				log_warn("Skipping parent-level FK constraint \"%s\" on "
						 "partitioned table %s: PostgreSQL cannot create it as "
						 "NOT VALID, and --defer-validate-fks must not trigger a "
						 "validation scan. Per-partition FK constraints are "
						 "still created directly where the source defines them; "
						 "reconcile the parent-level constraint manually after "
						 "the migration if you need it",
						 fk->conname, fk->tableQname);
				partitionedSkipCount++;

				/* it was counted as deferred at decision time; it isn't */
				deferredNotValidCount--;
				memset(dst.sqlstate, 0, sizeof(dst.sqlstate));
			}
			else
			{
				log_error("Failed to create FK constraint \"%s\" on %s",
						  fk->conname, fk->tableQname);
				destroyPQExpBuffer(cmd);
				success = false;
				break;
			}
		}

		destroyPQExpBuffer(cmd);

		/* compute duration */
		instr_time duration;
		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, startTime);
		uint64_t durationMs = INSTR_TIME_GET_MILLISEC(duration);

		if (!summary_finish_fk_constraint(sourceDB, fk, durationMs, notValid))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		if (!summary_increment_timing(sourceDB,
									  TIMING_SECTION_ALTER_TABLE,
									  1,    /* count */
									  0,    /* bytes */
									  durationMs))
		{
			/* errors have already been logged */
			success = false;
			break;
		}
	}

	if (sourceNotValidCount > 0)
	{
		log_notice("%d FK constraint(s) were already NOT VALID on the "
				   "source database and created as NOT VALID on target",
				   sourceNotValidCount);
	}

	if (notValidCount > 0)
	{
		log_warn("%d FK constraint(s) created as NOT VALID due to "
				 "pre-existing data violations on the source database",
				 notValidCount);
	}

	if (deferredNotValidCount > 0)
	{
		log_warn("%d FK constraint(s) created as NOT VALID because "
				 "--defer-validate-fks was used; validate them manually "
				 "with ALTER TABLE ... VALIDATE CONSTRAINT once the "
				 "migration is complete",
				 deferredNotValidCount);
	}

	if (skippedCount > 0)
	{
		log_warn("%d FK constraint(s) were skipped because their referenced "
				 "table, schema, or unique/primary key constraint does not "
				 "exist on the target (see warnings above); they were NOT "
				 "created and must be reconciled manually if needed",
				 skippedCount);
	}

	if (partitionedSkipCount > 0)
	{
		log_warn("%d parent-level FK constraint(s) on partitioned tables were "
				 "not created as NOT VALID (PostgreSQL limitation), so as not "
				 "to block the transition into CDC with a validation scan. "
				 "Per-partition constraints are created where present; "
				 "reconcile the parent-level constraints manually after the "
				 "migration if you need them",
				 partitionedSkipCount);
	}

	/* cleanup */
	for (int i = 0; i < fkArray.count; i++)
	{
		if (fkArray.array[i].constraintDef != NULL)
		{
			free(fkArray.array[i].constraintDef);
		}
	}

	free(fkArray.array);
	(void) pgsql_finish(&dst);

	return success;
}
