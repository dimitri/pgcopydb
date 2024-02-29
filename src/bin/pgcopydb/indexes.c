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
static bool copydb_create_constraints_hook(void *context, SourceIndex *index);
static bool copydb_copy_all_indexes_hook(void *ctx, SourceIndex *index);


/*
 * copydb_start_index_supervisor starts a CREATE INDEX supervisor process.
 */
bool
copydb_start_index_supervisor(CopyDataSpec *specs)
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

	if (!catalog_delete_process(&(specs->catalogs.source), pid))
	{
		log_warn("Failed to delete catalog process entry for pid %d", pid);
	}

	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&dst);
		return false;
	}

	(void) pgsql_finish(&dst);

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

		if (!specs->skipVacuum)
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


typedef struct IndexesAreDoneContext
{
	bool builtAllIndexes;
	CopyDataSpec *specs;
	SourceTable *table;
	int total;
	int done;
} IndexesAreDoneContext;


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


typedef struct QueueTableIndexesContext
{
	CopyDataSpec *specs;
	CopyTableDataSpec *tableSpecs;
} QueueTableIndexesContext;


/*
 * copydb_add_table_indexes sends a message to the CREATE INDEX process queue
 * to process indexes attached to the given table.
 */
bool
copydb_add_table_indexes(CopyDataSpec *specs, CopyTableDataSpec *tableSpecs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	QueueTableIndexesContext context = {
		.specs = specs,
		.tableSpecs = tableSpecs
	};

	if (!catalog_iter_s_index_table(sourceDB,
									tableSpecs->sourceTable->nspname,
									tableSpecs->sourceTable->relname,
									&context,
									&copydb_add_table_indexes_hook))
	{
		log_error("Failed to send table %s indexes to craete index queue, "
				  "see above for details",
				  tableSpecs->sourceTable->qname);
		return false;
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
	CopyDataSpec *specs = context->specs;
	CopyTableDataSpec *tableSpecs = context->tableSpecs;

	QMessage mesg = {
		.type = QMSG_TYPE_INDEXOID,
		.data.oid = index->indexOid
	};

	log_trace("Queueing index %s [%u] for table %s [%u]",
			  index->indexQname,
			  mesg.data.oid,
			  tableSpecs->sourceTable->qname,
			  tableSpecs->sourceTable->oid);

	if (!queue_send(&(specs->indexQueue), &mesg))
	{
		/* errors have already been logged */
		return false;
	}

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

	/* first start index workers that feed from the indexQueue */
	if (!copydb_start_index_supervisor(specs))
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

	if (!copydb_wait_for_subprocesses(specs->failFast))
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

	/*
	 * Prepare the summary CREATE INDEX command early so that we can insert it
	 * in our SQLite catalogs.
	 */
	if (!copydb_prepare_create_index_command(&indexSpecs, ifNotExists))
	{
		/* errors have already been logged */
		return false;
	}

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


typedef struct CreateConstraintsContext
{
	CopyDataSpec *specs;
	PGSQL *dst;
} CreateConstraintsContext;

/*
 * copydb_create_constraints loops over the index definitions for a given table
 * and creates all the associated constraints, one after the other.
 */
bool
copydb_create_constraints(CopyDataSpec *specs, PGSQL *dst, SourceTable *table)
{
	int errors = 0;

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
	 * Now iterate over the source database catalog list of indexes attached to
	 * the current table, and install indexes/constraints on that same table on
	 * the target database, skipping constraints that already exists on the
	 * target catalog.
	 */
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	CreateConstraintsContext context = {
		.specs = specs,
		.dst = dst
	};

	if (!catalog_iter_s_index_table(sourceDB,
									table->nspname,
									table->relname,
									&context,
									&copydb_create_constraints_hook))
	{
		/* errors have already been logged */
		return false;
	}

	return errors == 0;
}


/*
 * copydb_create_constraints_hook is an iterator callback function.
 */
static bool
copydb_create_constraints_hook(void *ctx, SourceIndex *index)
{
	CreateConstraintsContext *context = (CreateConstraintsContext *) ctx;
	CopyDataSpec *specs = context->specs;

	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	DatabaseCatalog *targetDB = &(specs->catalogs.target);

	/* some indexes are not attached to a constraint at all */
	if (index->constraintOid <= 0 ||
		IS_EMPTY_STRING_BUFFER(index->constraintName))
	{
		return true;
	}

	CopyIndexSpec indexSpecs = { .sourceIndex = index };
	CopyIndexSummary *indexSummary = &(indexSpecs.summary);

	if (!copydb_prepare_create_constraint_command(&indexSpecs))
	{
		log_warn("Failed to prepare SQL command to create constraint \"%s\"",
				 index->constraintName);
		return false;
	}

	if (!summary_add_constraint(sourceDB, &indexSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* skip constraints that already exist on the target database */
	SourceIndex *targetIndex = (SourceIndex *) calloc(1, sizeof(SourceIndex));

	if (targetIndex == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_index_by_name(targetDB,
										index->indexNamespace,
										index->indexRelname,
										targetIndex))
	{
		/* errors have already been logged */
		return false;
	}

	bool foundConstraintOnTarget =
		streq(index->constraintName, targetIndex->constraintName);

	if (!foundConstraintOnTarget)
	{
		log_notice("%s", indexSummary->command);

		/*
		 * Constraints are built by the CREATE INDEX worker process that is
		 * the last one to finish an index for a given table. We do not
		 * have to care about concurrency here: no semaphore locking.
		 */
		if (!pgsql_execute(context->dst, indexSummary->command))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!summary_finish_constraint(sourceDB, &indexSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_increment_timing(sourceDB,
								  TIMING_SECTION_ALTER_TABLE,
								  1, /* count */
								  0, /* bytes */
								  indexSpecs.summary.durationMs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}
