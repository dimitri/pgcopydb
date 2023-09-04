/*
 * src/bin/pgcopydb/compare.c
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
#include "progress.h"
#include "signals.h"
#include "summary.h"


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

	/*
	 * Once all the workers are done, grab the individual table checksums and
	 * add them to the global schema file.
	 */
	if (!compare_read_tables_sums(copySpecs))
	{
		log_fatal("Failed to read checksum in table summary files");
		return false;
	}

	/* write the data to file, include the rowcount and checksums */
	if (!copydb_prepare_schema_json_file(copySpecs))
	{
		log_fatal("Failed to store the source database "
				  "schema to file \"%s\", ",
				  copySpecs->cfPaths.schemafile);
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
	/* now append the table OIDs to the queue */
	SourceTableArray *tableArray = &(copySpecs->catalog.sourceTableArray);

	for (int i = 0; i < tableArray->count; i++)
	{
		SourceTable *source = &(tableArray->array[i]);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("Compare data has been interrupted");
			return false;
		}

		QMessage mesg = {
			.type = QMSG_TYPE_TABLEOID,
			.data.oid = source->oid
		};

		log_trace("compare_queue_tables(%d): %u", queue->qId, source->oid);

		if (!queue_send(queue, &mesg))
		{
			/* errors have already been logged */
			return false;
		}
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
 * compare_start_workers create as many sub-process as needed, per --table-jobs.
 */
bool
compare_start_workers(CopyDataSpec *copySpecs, Queue *queue)
{
	log_info("starting %d table compare processes", copySpecs->tableJobs);

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
	SourceTable *sourceTableHashByOid = copySpecs->catalog.sourceTableHashByOid;
	SourceTable *table = NULL;

	uint32_t toid = oid;
	HASH_FIND(hh, sourceTableHashByOid, &toid, sizeof(toid), table);

	if (table == NULL)
	{
		log_error("Failed to find table %u in sourceTableHashByOid", oid);
		return false;
	}

	CopyFilePaths *cfPaths = &(copySpecs->cfPaths);
	TableFilePaths tablePaths = { 0 };

	if (!copydb_init_tablepaths(cfPaths, &tablePaths, oid))
	{
		log_error("Failed to prepare pathnames for table %u", oid);
		return false;
	}

	log_trace("compare_data_by_table_oid: %u %s \"%s\"",
			  oid, table->qname, tablePaths.chksumFile);

	if (!compare_table(copySpecs, table))
	{
		log_error("Failed to compute rowcount and checksum for %s, "
				  "see above for details",
				  table->qname);

		return false;
	}

	/* now write the checksums to file */
	if (!compare_write_checksum(table, tablePaths.chksumFile))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * compare_read_tables_sums reads the individual table summary files to fetch
 * the rowcount and checksum computed by the worker processes, and copies the
 * data in the main table array.
 */
bool
compare_read_tables_sums(CopyDataSpec *copySpecs)
{
	SourceTableArray *tableArray = &(copySpecs->catalog.sourceTableArray);

	for (int i = 0; i < tableArray->count; i++)
	{
		SourceTable *source = &(tableArray->array[i]);
		CopyFilePaths *cfPaths = &(copySpecs->cfPaths);
		TableFilePaths tablePaths = { 0 };

		if (!copydb_init_tablepaths(cfPaths, &tablePaths, source->oid))
		{
			log_error("Failed to prepare pathnames for table %u", source->oid);
			return false;
		}

		if (!compare_read_checksum(source, tablePaths.chksumFile))
		{
			log_error("Failed to read table summary file: \"%s\"",
					  tablePaths.chksumFile);
			return false;
		}
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

	log_info("[SOURCE] table: %d index: %d sequence: %d",
			 sourceSpecs.catalog.sourceTableArray.count,
			 sourceSpecs.catalog.sourceIndexArray.count,
			 sourceSpecs.catalog.sequenceArray.count);

	log_info("[TARGET] table: %d index: %d sequence: %d",
			 targetSpecs.catalog.sourceTableArray.count,
			 targetSpecs.catalog.sourceIndexArray.count,
			 targetSpecs.catalog.sequenceArray.count);

	uint64_t diffCount = 0;

	SourceTable *targetTableHash = targetSpecs.catalog.sourceTableHashByQName;

	for (int i = 0; i < sourceSpecs.catalog.sourceTableArray.count; i++)
	{
		SourceTable *source = &(sourceSpecs.catalog.sourceTableArray.array[i]);
		SourceTable *target = NULL;

		char *qname = source->qname;
		size_t len = strlen(qname);

		HASH_FIND(hhQName, targetTableHash, qname, len, target);

		if (target == NULL)
		{
			++diffCount;
			log_error("Failed to find table %s in target database",
					  qname);
			continue;
		}

		/* check table columns */
		if (source->attributes.count != target->attributes.count)
		{
			++diffCount;
			log_error("Table %s has %d columns on source, %d columns on target",
					  qname,
					  source->attributes.count,
					  target->attributes.count);
			continue;
		}

		for (int c = 0; c < source->attributes.count; c++)
		{
			char *srcAttName = source->attributes.array[c].attname;
			char *tgtAttName = target->attributes.array[c].attname;

			if (!streq(srcAttName, tgtAttName))
			{
				++diffCount;
				log_error("Table %s attribute number %d "
						  "has name \"%s\" (%d) on source and "
						  "has name \"%s\" (%d) on target",
						  qname,
						  c,
						  srcAttName,
						  source->attributes.array[c].attnum,
						  tgtAttName,
						  target->attributes.array[c].attnum);
			}
		}

		/* now check table index list */
		uint64_t indexCount = 0;
		SourceIndexList *sourceIndexList = source->firstIndex;
		SourceIndexList *targetIndexList = target->firstIndex;

		for (; sourceIndexList != NULL; sourceIndexList = sourceIndexList->next)
		{
			SourceIndex *sourceIndex = sourceIndexList->index;

			++indexCount;

			if (targetIndexList == NULL)
			{
				++diffCount;
				log_error("Table %s is missing index \"%s\".\"%s\" on target",
						  qname,
						  sourceIndex->indexNamespace,
						  sourceIndex->indexRelname);

				continue;
			}

			SourceIndex *targetIndex = targetIndexList->index;

			if (!streq(sourceIndex->indexNamespace, targetIndex->indexNamespace) ||
				!streq(sourceIndex->indexRelname, targetIndex->indexRelname))
			{
				++diffCount;
				log_error("Table %s index mismatch: \"%s\".\"%s\" on source, "
						  "\"%s\".\"%s\" on target",
						  qname,
						  sourceIndex->indexNamespace,
						  sourceIndex->indexRelname,
						  targetIndex->indexNamespace,
						  targetIndex->indexRelname);
			}

			if (!streq(sourceIndex->indexDef, targetIndex->indexDef))
			{
				++diffCount;
				log_error("Table %s index \"%s\".\"%s\" mismatch "
						  "on index definition",
						  qname,
						  sourceIndex->indexNamespace,
						  sourceIndex->indexRelname);

				log_info("Source index \"%s\".\"%s\": %s",
						 sourceIndex->indexNamespace,
						 sourceIndex->indexRelname,
						 sourceIndex->indexDef);

				log_info("Target index \"%s\".\"%s\": %s",
						 targetIndex->indexNamespace,
						 targetIndex->indexRelname,
						 targetIndex->indexDef);
			}

			if (sourceIndex->isPrimary != targetIndex->isPrimary)
			{
				++diffCount;
				log_error("Table %s index \"%s\".\"%s\" is %s on source "
						  "and %s on target",
						  qname,
						  sourceIndex->indexNamespace,
						  sourceIndex->indexRelname,
						  sourceIndex->isPrimary ? "primary" : "not primary",
						  targetIndex->isPrimary ? "primary" : "not primary");
			}

			if (sourceIndex->isUnique != targetIndex->isUnique)
			{
				++diffCount;
				log_error("Table %s index \"%s\".\"%s\" is %s on source "
						  "and %s on target",
						  qname,
						  sourceIndex->indexNamespace,
						  sourceIndex->indexRelname,
						  sourceIndex->isUnique ? "unique" : "not unique",
						  targetIndex->isUnique ? "unique" : "not unique");
			}

			if (!streq(sourceIndex->constraintName, targetIndex->constraintName))
			{
				++diffCount;
				log_error("Table %s index \"%s\".\"%s\" is supporting "
						  " constraint named \"%s\" on source "
						  "and \"%s\" on target",
						  qname,
						  sourceIndex->indexNamespace,
						  sourceIndex->indexRelname,
						  sourceIndex->constraintName,
						  targetIndex->constraintName);
			}

			if (sourceIndex->constraintDef != NULL &&
				(targetIndex->constraintDef == NULL ||
				 !streq(sourceIndex->constraintDef, targetIndex->constraintDef)))
			{
				++diffCount;
				log_error("Table %s index \"%s\".\"%s\" constraint \"%s\" "
						  "definition mismatch.",
						  qname,
						  sourceIndex->indexNamespace,
						  sourceIndex->indexRelname,
						  sourceIndex->constraintName);

				log_info("Source index \"%s\".\"%s\" constraint \"%s\": %s",
						 sourceIndex->indexNamespace,
						 sourceIndex->indexRelname,
						 sourceIndex->constraintName,
						 sourceIndex->constraintDef);

				log_info("Target index \"%s\".\"%s\" constraint \"%s\": %s",
						 targetIndex->indexNamespace,
						 targetIndex->indexRelname,
						 targetIndex->constraintName,
						 targetIndex->constraintDef);
			}

			targetIndexList = targetIndexList->next;
		}

		log_notice("Matched table %s: %d columns ok, %lld indexes ok",
				   qname,
				   source->attributes.count,
				   (long long) indexCount);
	}

	/*
	 * Now focus on sequences. First, create the sequence names hash table to
	 * be able to match source sequences with their target counterparts.
	 */
	SourceSequence *targetSeqHash = NULL;

	for (int i = 0; i < targetSpecs.catalog.sequenceArray.count; i++)
	{
		SourceSequence *seq = &(targetSpecs.catalog.sequenceArray.array[i]);

		char *qname = seq->qname;
		size_t len = strlen(qname);

		HASH_ADD(hhQName, targetSeqHash, qname, len, seq);
	}

	/* publish the now fill-in hash table to the catalog */
	targetSpecs.catalog.sourceSeqHashByQname = targetSeqHash;

	for (int i = 0; i < sourceSpecs.catalog.sequenceArray.count; i++)
	{
		SourceSequence *source = &(sourceSpecs.catalog.sequenceArray.array[i]);
		SourceSequence *target = NULL;

		char *qname = source->qname;
		size_t len = strlen(qname);

		HASH_FIND(hhQName, targetSeqHash, qname, len, target);

		if (target == NULL)
		{
			++diffCount;
			log_error("Failed to find sequence %s in target database",
					  qname);
			continue;
		}

		if (source->lastValue != target->lastValue)
		{
			++diffCount;
			log_error("Sequence %s lastValue on source is %lld, on target %lld",
					  qname,
					  (long long) source->lastValue,
					  (long long) target->lastValue);
		}

		if (source->isCalled != target->isCalled)
		{
			++diffCount;
			log_error("Sequence %s isCalled on source is %s, on target %s",
					  qname,
					  source->isCalled ? "yes" : "no",
					  target->isCalled ? "yes" : "no");
		}

		log_notice("Matched sequence %s (last value %lld)",
				   qname,
				   (long long) source->lastValue);
	}

	if (diffCount > 0)
	{
		log_fatal("Schemas on source and target database differ");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("pgcopydb schema inspection is successful");

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

	ConnStrings *sourceConnStrings = &(sourceSpecs->connStrings);

	sourceConnStrings->target_pguri = NULL;

	ConnStrings *targetConnStrings = &(targetSpecs->connStrings);

	targetConnStrings->source_pguri = targetConnStrings->target_pguri;
	targetConnStrings->target_pguri = NULL;

	targetConnStrings->safeSourcePGURI = targetConnStrings->safeTargetPGURI;

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
