/*
 * src/bin/pgcopydb/compare.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "catalog.h"
#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "progress.h"
#include "signals.h"
#include "summary.h"


static bool compare_queue_table_hook(void *ctx, SourceTable *sourceTable);
static bool compare_schemas_table_hook(void *ctx, SourceTable *sourceTable);
static bool compare_schemas_index_hook(void *ctx, SourceIndex *sourceIndex);
static bool compare_schemas_seq_hook(void *ctx, SourceSequence *sourceSeq);


/*
 * compare_data fetches the schema on the source database and then compute the
 * rowcount and checksum of every selected table's contents to compare them.
 */
bool
compare_data(CopyDataSpec *copySpecs)
{
	Queue compareQueue = { 0 };

	/* use a queue to share the workload */
	if (!queue_create(&compareQueue, "compare"))
	{
		log_error("Failed to create the compare data process queue");
		return false;
	}

	/*
	 * Retrieve catalogs from the source database, the target is
	 * supposed to have the same objects.
	 */
	ConnStrings *dsn = &(copySpecs->connStrings);

	log_info("SOURCE: Connecting to \"%s\"", dsn->safeSourcePGURI.pguri);

	/*
	 * Reduce the catalog queries to the section we need here, and make sure we
	 * don't prepare the target catalogs.
	 */
	copySpecs->section = DATA_SECTION_TABLE_DATA;

	char *target_pguri = copySpecs->connStrings.target_pguri;
	copySpecs->connStrings.target_pguri = NULL;

	if (!copydb_fetch_schema_and_prepare_specs(copySpecs))
	{
		log_fatal("Failed to retrieve source database schema, "
				  "see above for details.");

		(void) queue_unlink(&compareQueue);
		return false;
	}

	/* cache invalidation for the computed checksums */
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	if (!catalog_init(sourceDB))
	{
		log_error("Failed to open internal catalogs in COPY worker process, "
				  "see above for details");
		return false;
	}

	if (!catalog_delete_s_table_chksum_all(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	/* restore the target_pguri, we will need it later */
	copySpecs->connStrings.target_pguri = target_pguri;

	/* we start copySpecs->tableJobs workers to share the workload */
	if (!compare_start_workers(copySpecs, &compareQueue))
	{
		log_fatal("Failed to start %d compare data workers",
				  copySpecs->tableJobs);

		(void) queue_unlink(&compareQueue);
		return false;
	}

	/* now, add the tables to compare to the queue */
	if (!compare_queue_tables(copySpecs, &compareQueue))
	{
		log_fatal("Failed to queue tables to compare");

		(void) queue_unlink(&compareQueue);
		return false;
	}

	/* and wait until the compare data workers are done */
	if (!copydb_wait_for_subprocesses(copySpecs->failFast))
	{
		log_fatal("Some compare data worker process have failed, "
				  "see above for details");

		(void) queue_unlink(&compareQueue);
		return false;
	}

	if (!queue_unlink(&compareQueue))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_close(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * compare_queue_tables adds table to our queue.
 */
bool
compare_queue_tables(CopyDataSpec *copySpecs, Queue *queue)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	/* now append the table OIDs to the queue */
	if (!catalog_iter_s_table(sourceDB, queue, &compare_queue_table_hook))
	{
		log_error("Failed to compare tables, see above for details");
		return false;
	}

	/* now append the STOP messages to the queue */
	for (int i = 0; i < copySpecs->tableJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP, .data.oid = 0 };

		log_trace("Adding STOP message to compare queue %d", queue->qId);

		if (!queue_send(queue, &stop))
		{
			/* errors have already been logged */
			continue;
		}
	}

	return true;
}


/*
 * compare_queue_table_hook is an iterator callback function.
 */
static bool
compare_queue_table_hook(void *ctx, SourceTable *table)
{
	Queue *queue = (Queue *) ctx;

	if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
	{
		log_error("Compare data has been interrupted");
		return false;
	}

	QMessage mesg = {
		.type = QMSG_TYPE_TABLEOID,
		.data.oid = table->oid
	};

	log_trace("compare_queue_tables(%d): %u", queue->qId, table->oid);

	if (!queue_send(queue, &mesg))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * compare_start_workers create as many sub-process as needed, per --table-jobs.
 */
bool
compare_start_workers(CopyDataSpec *copySpecs, Queue *queue)
{
	log_info("Starting %d table compare processes", copySpecs->tableJobs);

	for (int i = 0; i < copySpecs->tableJobs; i++)
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
				(void) set_ps_title("pgcopydb: compare worker");

				if (!compare_data_worker(copySpecs, queue))
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
 * compare_data__worker is a worker process that loops over messages received
 * from a queue, each message being the Oid of a table to compare.
 */
bool
compare_data_worker(CopyDataSpec *copySpecs, Queue *queue)
{
	pid_t pid = getpid();

	log_notice("Started table worker %d [%d]", pid, getppid());

	if (!catalog_init_from_specs(copySpecs))
	{
		log_error("Failed to open internal catalogs in COPY worker process, "
				  "see above for details");
		return false;
	}

	int errors = 0;
	bool stop = false;

	while (!stop)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(queue, &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("Compare data worker has been interrupted");
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
				log_debug("Stop message received by compare data worker");
				break;
			}

			case QMSG_TYPE_TABLEOID:
			{
				if (!compare_data_by_table_oid(copySpecs, mesg.data.oid))
				{
					log_error("Failed to compare table with oid %u, "
							  "see above for details",
							  mesg.data.oid);
					return false;
				}
				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on vacuum queue %d",
						  mesg.type,
						  queue->qId);
				break;
			}
		}
	}

	if (!catalog_close_from_specs(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	bool success = (stop == true && errors == 0);

	if (errors > 0)
	{
		log_error("Compare data worker %d encountered %d errors, "
				  "see above for details",
				  pid,
				  errors);
	}

	return success;
}


/*
 * compare_data_by_table_oid reads the done file for the given table OID,
 * fetches the schemaname and relname from there, and then compare the table
 * contents on the souce and target databases.
 */
bool
compare_data_by_table_oid(CopyDataSpec *copySpecs, uint32_t oid)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	SourceTable *table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_table(sourceDB, oid, 0, table))
	{
		log_error("Failed to lookup for table %u in our internal catalogs",
				  oid);

		free(table);
		return false;
	}

	if (table->oid == 0)
	{
		log_error("Failed to find table with oid %u in our internal catalogs",
				  oid);

		free(table);
		return false;
	}

	if (!catalog_s_table_fetch_attrs(sourceDB, table))
	{
		log_error("Failed to fetch table %s attribute list, "
				  "see above for details",
				  table->qname);
		return false;
	}

	log_trace("compare_data_by_table_oid: %u %s", oid, table->qname);

	if (!compare_table(copySpecs, table))
	{
		log_error("Failed to compute rowcount and checksum for %s, "
				  "see above for details",
				  table->qname);

		return false;
	}

	return true;
}


/*
 * compare_table computes the rowcount and checksum of a table contents on the
 * source and on the target database instances and compare them.
 */
bool
compare_table(CopyDataSpec *copySpecs, SourceTable *source)
{
	ConnStrings *dsn = &(copySpecs->connStrings);

	PGSQL src = { 0 };
	PGSQL dst = { 0 };

	if (!pgsql_init(&src, dsn->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&src))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_init(&dst, dsn->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&src);
		return false;
	}

	if (!pgsql_begin(&dst))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&src);
		return false;
	}

	/*
	 * First, send both the queries to the source and target databases,
	 * async.
	 */
	if (!schema_send_table_checksum(&src, source))
	{
		/* errors have already been logged */
		return false;
	}

	if (!schema_send_table_checksum(&dst, source))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Second, fetch the results from both the connections.
	 */
	TableChecksum *srcChk = &(source->sourceChecksum);
	TableChecksum *dstChk = &(source->targetChecksum);

	bool srcDone = false;
	bool dstDone = false;

	do {
		if (!srcDone)
		{
			if (!schema_fetch_table_checksum(&src, srcChk, &srcDone))
			{
				/* errors have already been logged */
				(void) pgsql_finish(&dst);
				return false;
			}
		}

		if (!dstDone)
		{
			if (!schema_fetch_table_checksum(&dst, dstChk, &dstDone))
			{
				/* errors have already been logged */
				(void) pgsql_finish(&src);
				return false;
			}
		}

		if (!srcDone || !dstDone)
		{
			pg_usleep(10 * 1000); /* 10 ms */
		}
	} while (!srcDone || !dstDone);


	if (!pgsql_commit(&src))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	log_notice("%s %u: %lld rows, checksum %s",
			   source->qname,
			   source->oid,
			   (long long) srcChk->rowcount,
			   srcChk->checksum);

	if (!catalog_add_s_table_chksum(sourceDB, source, srcChk, dstChk))
	{
		log_error("Failed to add checksum information to our internal catalogs, "
				  "see above for details");
		return false;
	}

	if (srcChk->rowcount != dstChk->rowcount)
	{
		log_error("Table %s has %lld rows on source, %lld rows on target",
				  source->qname,
				  (long long) srcChk->rowcount,
				  (long long) dstChk->rowcount);
	}

	/* if the rowcount is different, don't log the checksum mismatch */
	else if (!streq(srcChk->checksum, dstChk->checksum))
	{
		log_error("Table %s has checksum %s on source, %s on target",
				  source->qname,
				  srcChk->checksum,
				  dstChk->checksum);
	}

	log_notice("%s: %lld rows, checksum %s",
			   source->qname,
			   (long long) srcChk->rowcount,
			   srcChk->checksum);

	return true;
}


typedef struct CompareSchemaContext
{
	DatabaseCatalog *sourceDB;
	DatabaseCatalog *targetDB;
	uint64_t diffCount;
} CompareSchemaContext;


/*
 * compare_schemas compares the schemas between source and target instance, in
 * the context and scope of pgcopydb: conpare only the selected tables,
 * indexes, constraints and sequences from the source.
 */
bool
compare_schemas(CopyDataSpec *copySpecs)
{
	/*
	 * Now prepare two specifications with only the source uri.
	 *
	 * We don't free() any memory here as the two CopyDataSpecs copies are
	 * going to share pointers to memory allocated in the main copySpecs
	 * instance.
	 */
	CopyDataSpec sourceSpecs = { 0 };
	CopyDataSpec targetSpecs = { 0 };

	if (!compare_fetch_schemas(copySpecs, &sourceSpecs, &targetSpecs))
	{
		log_fatal("Failed to fetch source and target schemas, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	CatalogCounts sCount = { 0 };
	CatalogCounts tCount = { 0 };

	DatabaseCatalog *sourceDB = &(sourceSpecs.catalogs.source);
	DatabaseCatalog *targetDB = &(targetSpecs.catalogs.source);

	if (!catalog_count_objects(sourceDB, &sCount) ||
		!catalog_count_objects(targetDB, &tCount))
	{
		log_error("Failed to count indexes and constraints in our catalogs");
		return false;
	}

	log_info("[SOURCE] table: %lld, index: %lld, constraint: %lld, sequence: %lld",
			 (long long) sCount.tables,
			 (long long) sCount.indexes,
			 (long long) sCount.constraints,
			 (long long) sCount.sequences);

	log_info("[TARGET] table: %lld, index: %lld, constraint: %lld, sequence: %lld",
			 (long long) tCount.tables,
			 (long long) tCount.indexes,
			 (long long) tCount.constraints,
			 (long long) tCount.sequences);

	CompareSchemaContext context = {
		.sourceDB = sourceDB,
		.targetDB = targetDB,
		.diffCount = 0
	};

	if (!catalog_iter_s_table(sourceDB, &context, &compare_schemas_table_hook))
	{
		log_error("Failed to compare tables, see above for details");
		return false;
	}

	if (!catalog_iter_s_index(sourceDB, &context, &compare_schemas_index_hook))
	{
		log_error("Failed to compare indexes, see above for details");
		return false;
	}

	if (!catalog_iter_s_seq(sourceDB, &context, &compare_schemas_seq_hook))
	{
		log_error("Failed to compare sequences, see above for details");
		return false;
	}

	if (context.diffCount > 0)
	{
		log_fatal("Schemas on source and target database differ");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("pgcopydb schema inspection is successful");

	return true;
}


/*
 * compare_schemas_table_hook is an iterator callback function.
 */
static bool
compare_schemas_table_hook(void *ctx, SourceTable *sourceTable)
{
	CompareSchemaContext *context = (CompareSchemaContext *) ctx;

	SourceTable *targetTable = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (targetTable == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_table_by_name(context->targetDB,
										sourceTable->nspname,
										sourceTable->relname,
										targetTable))
	{
		log_error("Failed to lookup for table \"%s\".\"%s\" in our "
				  "internal target catalogs",
				  sourceTable->nspname,
				  sourceTable->relname);

		free(targetTable);
		return false;
	}

	if (targetTable->oid == 0)
	{
		++context->diffCount;
		log_error("Failed to find table %s in target database",
				  sourceTable->qname);
	}

	/* now fetch table attributes lists */
	if (!catalog_s_table_fetch_attrs(context->sourceDB, sourceTable) ||
		!catalog_s_table_fetch_attrs(context->targetDB, targetTable))
	{
		log_error("Failed to fetch table %s attribute list, "
				  "see above for details",
				  sourceTable->qname);
		return false;
	}

	/* check table columns */
	if (sourceTable->attributes.count != targetTable->attributes.count)
	{
		++context->diffCount;
		log_error("Table %s has %d columns on source, %d columns on target",
				  sourceTable->qname,
				  sourceTable->attributes.count,
				  targetTable->attributes.count);
	}

	for (int c = 0; c < sourceTable->attributes.count; c++)
	{
		char *srcAttName = sourceTable->attributes.array[c].attname;
		char *tgtAttName = targetTable->attributes.array[c].attname;

		if (!streq(srcAttName, tgtAttName))
		{
			++context->diffCount;
			log_error("Table %s attribute number %d "
					  "has name \"%s\" (%d) on source and "
					  "has name \"%s\" (%d) on target",
					  sourceTable->qname,
					  c,
					  srcAttName,
					  sourceTable->attributes.array[c].attnum,
					  tgtAttName,
					  targetTable->attributes.array[c].attnum);
		}
	}

	free(targetTable);

	log_notice("Matched table %s with %d columns ok",
			   sourceTable->qname,
			   sourceTable->attributes.count);

	return true;
}


/*
 * compare_schemas_index_hook is an iterator callback function.
 */
static bool
compare_schemas_index_hook(void *ctx, SourceIndex *sourceIndex)
{
	CompareSchemaContext *context = (CompareSchemaContext *) ctx;

	SourceIndex *targetIndex = (SourceIndex *) calloc(1, sizeof(SourceIndex));

	if (targetIndex == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_index_by_name(context->targetDB,
										sourceIndex->indexNamespace,
										sourceIndex->indexRelname,
										targetIndex))
	{
		log_error("Failed to lookup for index \"%s\".\"%s\" in our "
				  "internal target catalogs",
				  sourceIndex->indexNamespace,
				  sourceIndex->indexRelname);

		free(targetIndex);
		return false;
	}

	if (targetIndex->indexOid == 0)
	{
		++context->diffCount;
		log_error("Failed to find index %s in target database",
				  sourceIndex->indexQname);
	}

	if (!streq(sourceIndex->indexNamespace, targetIndex->indexNamespace) ||
		!streq(sourceIndex->indexRelname, targetIndex->indexRelname))
	{
		++context->diffCount;
		log_error("Table %s index mismatch: %s on source, %s on target",
				  sourceIndex->indexQname,
				  sourceIndex->indexQname,
				  targetIndex->indexQname);
	}

	if (!streq(sourceIndex->indexDef, targetIndex->indexDef))
	{
		++context->diffCount;
		log_error("Table %s index %s mismatch on index definition",
				  sourceIndex->indexQname,
				  sourceIndex->indexQname);

		log_info("Source index %s: %s",
				 sourceIndex->indexQname,
				 sourceIndex->indexDef);

		log_info("Target index %s: %s",
				 targetIndex->indexQname,
				 targetIndex->indexDef);
	}

	if (sourceIndex->isPrimary != targetIndex->isPrimary)
	{
		++context->diffCount;
		log_error("Table %s index %s is %s on source "
				  "and %s on target",
				  sourceIndex->indexQname,
				  sourceIndex->indexQname,
				  sourceIndex->isPrimary ? "primary" : "not primary",
				  targetIndex->isPrimary ? "primary" : "not primary");
	}

	if (sourceIndex->isUnique != targetIndex->isUnique)
	{
		++context->diffCount;
		log_error("Table %s index %s is %s on source "
				  "and %s on target",
				  sourceIndex->indexQname,
				  sourceIndex->indexQname,
				  sourceIndex->isUnique ? "unique" : "not unique",
				  targetIndex->isUnique ? "unique" : "not unique");
	}

	if (!streq(sourceIndex->constraintName, targetIndex->constraintName))
	{
		++context->diffCount;
		log_error("Table %s index %s is supporting "
				  " constraint named %s on source "
				  "and %s on target",
				  sourceIndex->indexQname,
				  sourceIndex->indexQname,
				  sourceIndex->constraintName,
				  targetIndex->constraintName);
	}

	if (sourceIndex->constraintDef != NULL &&
		(targetIndex->constraintDef == NULL ||
		 !streq(sourceIndex->constraintDef, targetIndex->constraintDef)))
	{
		++context->diffCount;
		log_error("Table %s index %s constraint %s "
				  "definition mismatch.",
				  sourceIndex->indexQname,
				  sourceIndex->indexQname,
				  sourceIndex->constraintName);

		log_info("Source index %s constraint %s: %s",
				 sourceIndex->indexQname,
				 sourceIndex->constraintName,
				 sourceIndex->constraintDef);

		log_info("Target index %s constraint %s: %s",
				 targetIndex->indexQname,
				 targetIndex->constraintName,
				 targetIndex->constraintDef);
	}

	free(targetIndex);

	log_notice("Matched index %s ok (%s, %s)",
			   sourceIndex->indexQname,
			   sourceIndex->isPrimary ? "primary" : "not primary",
			   sourceIndex->isUnique ? "unique" : "not unique");

	return true;
}


/*
 * compare_schemas_seq_hook is an iterator callback function.
 */
static bool
compare_schemas_seq_hook(void *ctx, SourceSequence *sourceSeq)
{
	CompareSchemaContext *context = (CompareSchemaContext *) ctx;

	SourceSequence *targetSeq =
		(SourceSequence *) calloc(1, sizeof(SourceSequence));

	if (targetSeq == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_seq_by_name(context->targetDB,
									  sourceSeq->nspname,
									  sourceSeq->relname,
									  targetSeq))
	{
		log_error("Failed to lookup for seq \"%s\".\"%s\" in our "
				  "internal target catalogs",
				  sourceSeq->nspname,
				  sourceSeq->relname);

		free(targetSeq);
		return false;
	}

	if (targetSeq->oid == 0)
	{
		++context->diffCount;
		log_error("Failed to find seq %s in target database",
				  sourceSeq->qname);
	}

	if (sourceSeq->lastValue != targetSeq->lastValue)
	{
		++context->diffCount;
		log_error("Sequence %s lastValue on source is %lld, on target %lld",
				  sourceSeq->qname,
				  (long long) sourceSeq->lastValue,
				  (long long) targetSeq->lastValue);
	}

	if (sourceSeq->isCalled != targetSeq->isCalled)
	{
		++context->diffCount;
		log_error("Sequence %s isCalled on source is %s, on target %s",
				  sourceSeq->qname,
				  sourceSeq->isCalled ? "yes" : "no",
				  targetSeq->isCalled ? "yes" : "no");
	}

	log_notice("Matched sequence %s (last value %lld)",
			   sourceSeq->qname,
			   (long long) sourceSeq->lastValue);

	free(targetSeq);

	return true;
}


/*
 * compare_fetch_schemas fetches the source and target schemas.
 */
bool
compare_fetch_schemas(CopyDataSpec *copySpecs,
					  CopyDataSpec *sourceSpecs,
					  CopyDataSpec *targetSpecs)
{
	/* copy the structure instances over */
	*sourceSpecs = *copySpecs;
	*targetSpecs = *copySpecs;

	/*
	 * Tweak the sourceSpecs so that we bypass retrieving catalog information
	 * about the target database entirely.
	 */
	ConnStrings *sourceConnStrings = &(sourceSpecs->connStrings);

	sourceConnStrings->target_pguri = NULL;

	CopyFilePaths *s_cfPaths = &(sourceSpecs->cfPaths);

	char sourceDir[MAXPGPATH] = { 0 };

	sformat(sourceDir, sizeof(sourceDir), "%s/source", s_cfPaths->schemadir);

	if (!copydb_rmdir_or_mkdir(sourceDir, true))
	{
		/* errors have already been logged */
		return false;
	}

	struct db
	{
		char *name;
		DatabaseCatalog *db;
	}
	dbs[] =
	{
		{ .name = "source", .db = &(sourceSpecs->catalogs.source) },
		{ .name = "filter", .db = &(sourceSpecs->catalogs.filter) },
		{ .name = "target", .db = &(sourceSpecs->catalogs.target) },
		{ NULL, NULL }
	};

	for (int i = 0; dbs[i].name != NULL; i++)
	{
		struct db *d = &(dbs[i]);
		sformat(d->db->dbfile, MAXPGPATH, "%s/%s.db", sourceDir, d->name);
	}

	/* ensure cache invalidation here, also refrain from filtering prep */
	sourceSpecs->fetchCatalogs = true;
	sourceSpecs->fetchFilteredOids = false;

	/*
	 * Retrieve our internal representation of the catalogs for both the source
	 * and the target database.
	 */
	log_info("SOURCE: Connecting to \"%s\"",
			 sourceConnStrings->safeSourcePGURI.pguri);

	if (!copydb_fetch_schema_and_prepare_specs(sourceSpecs))
	{
		log_fatal("Failed to retrieve source database schema, "
				  "see above for details.");
		return false;
	}

	/* copy the source schema to the compare file */
	strlcpy(sourceSpecs->cfPaths.schemafile,
			sourceSpecs->cfPaths.compare.sschemafile,
			MAXPGPATH);

	if (!copydb_prepare_schema_json_file(sourceSpecs))
	{
		log_fatal("Failed to store the source database schema to file \"%s\", "
				  "see above for details",
				  sourceSpecs->cfPaths.schemafile);
		return false;
	}

	/*
	 * Tweak the targetSpecs so that we fetch catalogs using the same code as
	 * for the source database, but target the target catalog database instead.
	 */
	ConnStrings *targetConnStrings = &(targetSpecs->connStrings);

	targetConnStrings->source_pguri = targetConnStrings->target_pguri;
	targetConnStrings->target_pguri = NULL;

	targetConnStrings->safeSourcePGURI = targetConnStrings->safeTargetPGURI;

	CopyFilePaths *t_cfPaths = &(targetSpecs->cfPaths);

	char targetDir[MAXPGPATH] = { 0 };

	sformat(targetDir, sizeof(targetDir), "%s/target", t_cfPaths->schemadir);

	if (!copydb_rmdir_or_mkdir(targetDir, true))
	{
		/* errors have already been logged */
		return false;
	}

	struct db dbt[] =
	{
		{ .name = "source", .db = &(targetSpecs->catalogs.source) },
		{ .name = "filter", .db = &(targetSpecs->catalogs.filter) },
		{ .name = "target", .db = &(targetSpecs->catalogs.target) },
		{ NULL, NULL }
	};

	for (int i = 0; dbs[i].name != NULL; i++)
	{
		struct db *d = &(dbt[i]);
		sformat(d->db->dbfile, MAXPGPATH, "%s/%s.db", targetDir, d->name);
	}

	/* ensure cache invalidation here, also refrain from filtering prep */
	targetSpecs->fetchCatalogs = true;
	targetSpecs->fetchFilteredOids = false;

	log_info("TARGET: Connecting to \"%s\"",
			 targetConnStrings->safeSourcePGURI.pguri);

	if (!copydb_fetch_schema_and_prepare_specs(targetSpecs))
	{
		log_fatal("Failed to retrieve source database schema, "
				  "see above for details.");
		return false;
	}

	/* copy the target schema to the compare file */
	strlcpy(targetSpecs->cfPaths.schemafile,
			targetSpecs->cfPaths.compare.tschemafile,
			MAXPGPATH);

	if (!copydb_prepare_schema_json_file(targetSpecs))
	{
		log_fatal("Failed to store the target database schema to file \"%s\", "
				  "see above for details",
				  targetSpecs->cfPaths.schemafile);
		return false;
	}

	return true;
}


/*
 * compare_write_checksum writes the checksum to file.
 */
bool
compare_write_checksum(SourceTable *table, const char *filename)
{
	JSON_Value *js = json_value_init_object();
	JSON_Object *jsObj = json_value_get_object(js);

	json_object_dotset_number(jsObj, "table.oid", table->oid);
	json_object_dotset_string(jsObj, "table.nspname", table->nspname);
	json_object_dotset_string(jsObj, "table.relname", table->relname);

	json_object_dotset_number(jsObj,
							  "source.rowcount",
							  table->sourceChecksum.rowcount);

	json_object_dotset_string(jsObj,
							  "source.checksum",
							  table->sourceChecksum.checksum);

	json_object_dotset_number(jsObj,
							  "target.rowcount",
							  table->targetChecksum.rowcount);

	json_object_dotset_string(jsObj,
							  "target.checksum",
							  table->targetChecksum.checksum);

	char *serialized_string = json_serialize_to_string_pretty(js);
	size_t len = strlen(serialized_string);

	bool success = write_file(serialized_string, len, filename);

	json_free_serialized_string(serialized_string);
	json_value_free(js);

	if (!success)
	{
		log_error("Failed to write table checksum file \"%s\"", filename);
		return false;
	}

	return true;
}


/*
 * compare_read_checksum reads the checksum file.
 */
bool
compare_read_checksum(SourceTable *table, const char *filename)
{
	JSON_Value *json = json_parse_file(filename);

	if (json == NULL)
	{
		log_error("Failed to parse table checksum file \"%s\"", filename);
		return false;
	}

	JSON_Object *jsObj = json_value_get_object(json);

	if (table->oid != json_object_dotget_number(jsObj, "table.oid"))
	{
		log_error("Failed to match table oid %u (%s) in file \"%s\"",
				  table->oid,
				  table->qname,
				  filename);
		json_value_free(json);
		return false;
	}

	table->sourceChecksum.rowcount =
		json_object_dotget_number(jsObj, "source.rowcount");

	strlcpy(table->sourceChecksum.checksum,
			json_object_dotget_string(jsObj, "source.checksum"),
			sizeof(table->sourceChecksum.checksum));

	table->targetChecksum.rowcount =
		json_object_dotget_number(jsObj, "target.rowcount");

	strlcpy(table->targetChecksum.checksum,
			json_object_dotget_string(jsObj, "target.checksum"),
			sizeof(table->targetChecksum.checksum));

	return true;
}
