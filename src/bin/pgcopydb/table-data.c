/*
 * src/bin/pgcopydb/table-data.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


/*
 * copydb_fetch_schema_and_prepare_specs fetches the list of tables from the
 * source database, and then fetches the list of objects that are filtered-out
 * (schemas, tables, indexes, constraints, then objects that depend on those).
 *
 * Then the per-table CopyTableDataSpec are initialized in preparation of the
 * rest of the work.
 */
bool
copydb_fetch_schema_and_prepare_specs(CopyDataSpec *specs)
{
	/*
	 * Either use the already established connection and transaction that
	 * exports our snapshot in the main process, or establish a transaction
	 * that groups together the filters preparation in temp tables and then the
	 * queries that join with those temp tables.
	 */
	PGSQL *src = NULL;
	PGSQL pgsql = { 0 };

	if (specs->consistent)
	{
		log_debug("re-use snapshot \"%s\"", specs->sourceSnapshot.snapshot);
		src = &(specs->sourceSnapshot.pgsql);
	}
	else
	{
		log_debug("--not-consistent, create a fresh connection");
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

	/* now fetch the list of tables from the source database */
	if (specs->section == DATA_SECTION_ALL ||
		specs->section == DATA_SECTION_TABLE_DATA)
	{
		if (!copydb_prepare_table_specs(specs, src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* fetch the list of all the indexes that are going to be created again */
	if (specs->section == DATA_SECTION_ALL ||
		specs->section == DATA_SECTION_INDEXES ||
		specs->section == DATA_SECTION_CONSTRAINTS)
	{
		SourceIndexArray *indexArray = &(specs->sourceIndexArray);

		if (!schema_list_all_indexes(src, &(specs->filters), indexArray))
		{
			/* errors have already been logged */
			return false;
		}

		log_info("Fetched information for %d indexes", indexArray->count);
	}

	/*
	 * Now build a SourceIndexList per table, when we retrieved both the table
	 * list and the indexes list.
	 */
	if (specs->section == DATA_SECTION_ALL)
	{
		/* now build the index hash-table */
		SourceIndex *sourceIndexHashByOid = NULL;
		SourceIndexArray *indexArray = &(specs->sourceIndexArray);

		for (int i = 0; i < indexArray->count; i++)
		{
			SourceIndex *index = &(indexArray->array[i]);

			/* add the current table to the index Hash-by-OID */
			HASH_ADD(hh, sourceIndexHashByOid, indexOid, sizeof(uint32_t), index);

			/* find the index table, update its index list */
			uint32_t oid = index->tableOid;
			SourceTable *table = NULL;

			HASH_FIND(hh, specs->sourceTableHashByOid, &oid, sizeof(oid), table);

			if (table == NULL)
			{
				log_error("Failed to find table %u (\"%s\".\"%s\") "
						  " in sourceTableHashByOid",
						  oid,
						  indexArray->array[i].tableNamespace,
						  indexArray->array[i].tableRelname);
				return false;
			}

			log_trace("Adding index %u %s to table %u %s",
					  indexArray->array[i].indexOid,
					  indexArray->array[i].indexRelname,
					  table->oid,
					  table->relname);

			if (table->firstIndex == NULL)
			{
				table->firstIndex =
					(SourceIndexList *) calloc(1, sizeof(SourceIndexList));

				if (table->firstIndex == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				table->firstIndex->index = index;
				table->firstIndex->next = NULL;

				table->lastIndex = table->firstIndex;
			}
			else
			{
				SourceIndexList *current = table->lastIndex;

				table->lastIndex =
					(SourceIndexList *) calloc(1, sizeof(SourceIndexList));

				if (table->lastIndex == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				table->lastIndex->index = index;
				table->lastIndex->next = NULL;

				current->next = table->lastIndex;
			}
		}

		/* now attach the final hash table head to the specs */
		specs->sourceIndexHashByOid = sourceIndexHashByOid;
	}

	if (specs->section == DATA_SECTION_ALL ||
		specs->section == DATA_SECTION_SET_SEQUENCES)
	{
		if (!copydb_prepare_sequence_specs(specs, src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* prepare the Oids of objects that are filtered out */
	if (!copydb_fetch_filtered_oids(specs, src))
	{
		/* errors have already been logged */
		return false;
	}

	if (!specs->consistent)
	{
		log_debug("--not-consistent: commit and close SOURCE connection now");
		if (!pgsql_commit(src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


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

		/* ensure we return false, signaling something unexpected happened */
		++errors;
		goto terminate;
	}

	/* Now write that we successfully finished copying all indexes */
	if (!write_file("", 0, specs->cfPaths.done.indexes))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.indexes);
	}

terminate:

	/*
	 * Now that all the sub-processes are done, we can also unlink the table
	 * and index concurrency semaphore, and the vacuum and create index queues.
	 */
	if (!semaphore_finish(&(specs->tableSemaphore)))
	{
		log_warn("Failed to remove table concurrency semaphore %d, "
				 "see above for details",
				 specs->tableSemaphore.semId);
	}

	if (!semaphore_finish(&(specs->indexSemaphore)))
	{
		log_warn("Failed to remove index concurrency semaphore %d, "
				 "see above for details",
				 specs->indexSemaphore.semId);
	}

	if (!queue_unlink(&(specs->vacuumQueue)))
	{
		log_warn("Failed to remove VACUUM process queue %d, "
				 "see above for details",
				 specs->vacuumQueue.qId);
	}

	if (!queue_unlink(&(specs->indexQueue)))
	{
		log_warn("Failed to remove CREATE INDEX process queue %d, "
				 "see above for details",
				 specs->indexQueue.qId);
	}

	return errors == 0;
}


/*
 * copydb_prepare_table_data fetches the list of tables to COPY data from the
 * source and into the target, and initialises our internal
 * CopyTableDataSpecsArray to drive the operations.
 */
bool
copydb_prepare_table_specs(CopyDataSpec *specs, PGSQL *pgsql)
{
	SourceTableArray *tableArray = &(specs->sourceTableArray);
	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

	log_info("Listing ordinary tables in source database");

	/*
	 * Now get the list of the tables we want to COPY over.
	 */
	if (!schema_list_ordinary_tables(pgsql,
									 &(specs->filters),
									 tableArray))
	{
		/* errors have already been logged */
		return false;
	}

	int copySpecsCount = 0;

	if (specs->splitTablesLargerThan > 0)
	{
		log_info("Splitting source candidate tables larger than %s",
				 specs->splitTablesLargerThanPretty);
	}

	/* prepare a SourceTable hash table, indexed by Oid */
	SourceTable *sourceTableHashByOid = NULL;

	/*
	 * Source table might be split in several concurrent COPY processes. In
	 * that case we produce a CopyDataSpec entry for each COPY partition.
	 */
	for (int tableIndex = 0; tableIndex < tableArray->count; tableIndex++)
	{
		SourceTable *source = &(tableArray->array[tableIndex]);

		/* add the current table to the Hash-by-OID */
		HASH_ADD(hh, sourceTableHashByOid, oid, sizeof(uint32_t), source);

		if (specs->splitTablesLargerThan > 0 &&
			specs->splitTablesLargerThan <= source->bytes)
		{
			if (IS_EMPTY_STRING_BUFFER(source->partKey))
			{
				log_info("Table \"%s\".\"%s\" is %s large, "
						 "which is larger than --split-tables-larger-than %s, "
						 "but does not have a unique column of type integer "
						 "(int2/int4/int8).",
						 source->nspname,
						 source->relname,
						 source->bytesPretty,
						 specs->splitTablesLargerThanPretty);

				log_warn("Skipping same-table concurrency for table \"%s\".\"%s\"",
						 source->nspname,
						 source->relname);

				++copySpecsCount;
				continue;
			}

			if (!schema_list_partitions(pgsql,
										source,
										specs->splitTablesLargerThan))
			{
				/* errors have already been logged */
				return false;
			}

			if (source->partsArray.count > 1)
			{
				log_info("Table \"%s\".\"%s\" is %s large, "
						 "%d COPY processes will be used, partitining on \"%s\".",
						 source->nspname,
						 source->relname,
						 source->bytesPretty,
						 source->partsArray.count,
						 source->partKey);

				copySpecsCount += source->partsArray.count;
			}
			else
			{
				++copySpecsCount;
			}
		}
		else
		{
			++copySpecsCount;
		}
	}

	/* now attach the final hash table head to the specs */
	specs->sourceTableHashByOid = sourceTableHashByOid;

	/* only use as many processes as required */
	if (copySpecsCount < specs->tableJobs)
	{
		specs->tableJobs = copySpecsCount;
	}

	specs->tableSpecsArray.count = copySpecsCount;
	specs->tableSpecsArray.array =
		(CopyTableDataSpec *) malloc(copySpecsCount * sizeof(CopyTableDataSpec));

	uint64_t totalBytes = 0;
	uint64_t totalTuples = 0;

	/*
	 * Prepare the copy specs for each COPY source we have: each full table and
	 * each table part when partitionning/splitting is in use.
	 */
	int specsIndex = 0;
	CopyTableDataSpec *tableSpecs = &(tableSpecsArray->array[specsIndex]);

	for (int tableIndex = 0; tableIndex < tableArray->count; tableIndex++)
	{
		/* initialize our TableDataProcess entry now */
		SourceTable *source = &(tableArray->array[tableIndex]);

		/*
		 * The CopyTableDataSpec structure has its own memory area for the
		 * SourceTable entry, which is copied by the following function. This
		 * means that 'SourceTableArray tableArray' is actually local memory.
		 */
		int partCount = source->partsArray.count;

		if (partCount <= 1)
		{
			if (!copydb_init_table_specs(tableSpecs, specs, source, 0))
			{
				/* errors have already been logged */
				return false;
			}

			tableSpecs = &(tableSpecsArray->array[++specsIndex]);
		}
		else
		{
			for (int partIndex = 0; partIndex < partCount; partIndex++)
			{
				if (!copydb_init_table_specs(tableSpecs,
											 specs,
											 source,
											 partIndex))
				{
					/* errors have already been logged */
					return false;
				}

				tableSpecs = &(tableSpecsArray->array[++specsIndex]);
			}
		}

		totalBytes += source->bytes;
		totalTuples += source->reltuples;
	}

	char bytesPretty[BUFSIZE] = { 0 };
	char relTuplesPretty[BUFSIZE] = { 0 };

	(void) pretty_print_bytes(bytesPretty, BUFSIZE, totalBytes);
	(void) pretty_print_count(relTuplesPretty, BUFSIZE, totalTuples);

	log_info("Fetched information for %d tables, "
			 "with an estimated total of %s tuples and %s",
			 tableArray->count,
			 relTuplesPretty,
			 bytesPretty);

	return true;
}


/*
 * copydb_objectid_is_filtered_out returns true when the given oid belongs to a
 * database object that's been filtered out by the filtering setup.
 */
bool
copydb_objectid_is_filtered_out(CopyDataSpec *specs,
								uint32_t oid,
								char *restoreListName)
{
	SourceFilterItem *hOid = specs->hOid;
	SourceFilterItem *hName = specs->hName;

	SourceFilterItem *item = NULL;

	if (oid != 0)
	{
		HASH_FIND(hOid, hOid, &oid, sizeof(uint32_t), item);

		if (item != NULL)
		{
			return true;
		}
	}

	if (restoreListName != NULL && !IS_EMPTY_STRING_BUFFER(restoreListName))
	{
		size_t len = strlen(restoreListName);
		HASH_FIND(hName, hName, restoreListName, len, item);

		if (item != NULL)
		{
			return true;
		}
	}

	return false;
}


/*
 * copydb_fetch_filtered_oids fetches the Postgres objects OID matching the
 * installed filters. The SourceFilterArray associates a boolean with an OID
 * that's used as a key to the array. The boolean is true when the OID has to
 * be filtered out of the pg_restore catalog or other operations.
 */
bool
copydb_fetch_filtered_oids(CopyDataSpec *specs, PGSQL *pgsql)
{
	SourceFilters *filters = &(specs->filters);
	SourceFilterItem *hOid = NULL;
	SourceFilterItem *hName = NULL;

	SourceTableArray tableArray = { 0, NULL };
	SourceIndexArray indexArray = { 0, NULL };
	SourceSequenceArray sequenceArray = { 0, NULL };
	SourceDependArray dependArray = { 0, NULL };

	/*
	 * Take the complement of the filtering, to list the OIDs of objects that
	 * we do not process.
	 */
	SourceFilterType type = filters->type;

	filters->type = filterTypeComplement(type);

	if (filters->type == SOURCE_FILTER_TYPE_NONE)
	{
		return true;
	}

	/*
	 * Now fetch the OIDs of tables, indexes, and sequences that we filter out.
	 */
	if (!schema_list_ordinary_tables(pgsql, filters, &tableArray))
	{
		/* errors have already been logged */
		filters->type = type;
		return false;
	}

	if (!schema_list_all_indexes(pgsql, filters, &indexArray))
	{
		/* errors have already been logged */
		filters->type = type;
		return false;
	}

	if (!schema_list_sequences(pgsql, filters, &sequenceArray))
	{
		/* errors have already been logged */
		filters->type = type;
		return false;
	}

	if (!schema_list_pg_depend(pgsql, filters, &dependArray))
	{
		/* errors have already been logged */
		filters->type = type;
		return false;
	}

	/* re-install the actual filter type */
	filters->type = type;

	/* first the tables */
	for (int i = 0; i < tableArray.count; i++)
	{
		SourceTable *table = &(tableArray.array[i]);

		SourceFilterItem *item = malloc(sizeof(SourceFilterItem));

		if (item == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		item->oid = table->oid;
		item->kind = OBJECT_KIND_TABLE;
		item->table = *table;

		strlcpy(item->restoreListName,
				table->restoreListName,
				RESTORE_LIST_NAMEDATALEN);

		HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), item);

		size_t len = strlen(item->restoreListName);
		HASH_ADD(hName, hName, restoreListName, len, item);
	}

	/* now indexes and constraints */
	for (int i = 0; i < indexArray.count; i++)
	{
		SourceIndex *index = &(indexArray.array[i]);

		SourceFilterItem *idxItem = malloc(sizeof(SourceFilterItem));

		if (idxItem == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		idxItem->oid = index->indexOid;
		idxItem->kind = OBJECT_KIND_INDEX;
		idxItem->index = *index;

		strlcpy(idxItem->restoreListName,
				index->indexRestoreListName,
				RESTORE_LIST_NAMEDATALEN);


		HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), idxItem);

		size_t len = strlen(idxItem->restoreListName);
		HASH_ADD(hName, hName, restoreListName, len, idxItem);

		if (indexArray.array[i].constraintOid > 0)
		{
			SourceFilterItem *conItem = malloc(sizeof(SourceFilterItem));

			if (conItem == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			conItem->oid = index->constraintOid;
			conItem->kind = OBJECT_KIND_CONSTRAINT;
			conItem->index = *index;

			/* at the moment we lack restore names for constraints */
			HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), conItem);
		}
	}

	/* now sequences */
	for (int i = 0; i < sequenceArray.count; i++)
	{
		SourceSequence *seq = &(sequenceArray.array[i]);

		SourceFilterItem *item = malloc(sizeof(SourceFilterItem));

		if (item == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		item->oid = seq->oid;
		item->kind = OBJECT_KIND_SEQUENCE;
		item->sequence = *seq;

		strlcpy(item->restoreListName,
				seq->restoreListName,
				RESTORE_LIST_NAMEDATALEN);

		HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), item);

		size_t len = strlen(seq->restoreListName);
		HASH_ADD(hName, hName, restoreListName, len, item);
	}

	/* finally table dependencies */
	for (int i = 0; i < dependArray.count; i++)
	{
		SourceDepend *depend = &(dependArray.array[i]);

		SourceFilterItem *item = malloc(sizeof(SourceFilterItem));

		if (item == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		item->oid = depend->objid;
		item->kind = OBJECT_KIND_UNKNOWN;

		strlcpy(item->restoreListName,
				depend->identity,
				RESTORE_LIST_NAMEDATALEN);

		HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), item);
	}

	/* publish our hash tables to the main CopyDataSpec instance */
	specs->hOid = hOid;
	specs->hName = hName;

	/* free dynamic memory that's not needed anymore */
	free(tableArray.array);
	free(indexArray.array);
	free(sequenceArray.array);

	return true;
}


/*
 * copydb_ObjectKindToString returns the string representation of an ObjectKind
 * enum value.
 */
char *
copydb_ObjectKindToString(ObjectKind kind)
{
	switch (kind)
	{
		case OBJECT_KIND_UNKNOWN:
		{
			return "unknown";
		}

		case OBJECT_KIND_TABLE:
		{
			return "table";
		}

		case OBJECT_KIND_INDEX:
		{
			return "index";
		}

		case OBJECT_KIND_CONSTRAINT:
		{
			return "constraint";
		}

		case OBJECT_KIND_SEQUENCE:
			return "sequence";
	}

	return "unknown";
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
	 * Now create as many VACUUM ANALYZE sub-processes as needed, per
	 * --table-jobs. Could be exposed separately as --vacuumJobs too, but
	 * that's not been done at this time.
	 */
	log_trace("copydb_process_table_data: \"%s\"", specs->cfPaths.tbldir);

	/*
	 * Are blobs table data? well pg_dump --section sayth yes.
	 */
	if (!copydb_start_blob_process(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_start_index_workers(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!vacuum_start_workers(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_start_seq_process(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now create as many sub-process as needed, per --table-jobs.
	 */
	if (copydb_process_table_data_with_workers(specs))
	{
		/* write that we successfully finished copying all tables */
		if (!write_file("", 0, specs->cfPaths.done.tables))
		{
			log_warn("Failed to write the tracking file \%s\"",
					 specs->cfPaths.done.tables);
		}
	}
	else
	{
		/* errors have been logged, make sure to send stop messages */
		++errors;
	}

	log_info("COPY phase is done, "
			 "now waiting for vacuum, index, blob, and sequences processes");

	/*
	 * Now that the COPY processes are done, signal this is the end to the
	 * vacuum and CREATE INDEX sub-processes by adding the STOP message to
	 * their queues.
	 */
	if (!vacuum_send_stop(specs))
	{
		/* errors have already been logged */
		++errors;
	}

	if (!copydb_index_workers_send_stop(specs))
	{
		/* errors have already been logged */
		++errors;
	}

	if (!copydb_wait_for_subprocesses())
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
 * copydb_start_table_data_workers create a supervisor COPY process, and then
 * as sub-process of that supervisor process creates as many sub-processes as
 * needed, per --table-jobs.
 *
 * The supervisor is needed to make this function sync: we can then just wait
 * until all the known sub-processes are done, without having to take into
 * consideration other processes not in the sub-tree.
 */
bool
copydb_process_table_data_with_workers(CopyDataSpec *specs)
{
	log_notice("Now starting %d COPY processes", specs->tableJobs);

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
			log_error("Failed to fork the COPY supervisor process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			log_notice("Started COPY supervisor %d [%d]", getpid(), getppid());

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
						exit(EXIT_CODE_INTERNAL_ERROR);
					}

					case 0:
					{
						/* child process runs the command */
						if (!copydb_process_table_data_worker(specs))
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

			/* the COPY supervisor waits for the COPY workers */
			if (!copydb_wait_for_subprocesses())
			{
				log_error("Some COPY worker process(es) have exited with error, "
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

	/* wait until the supervisor process exits */
	int status;

	if (waitpid(fpid, &status, 0) != fpid)
	{
		log_error("Failed to wait for COPY supervisor process %d: %m", fpid);
		return false;
	}

	int returnCode = WEXITSTATUS(status);

	if (returnCode != 0)
	{
		log_error("COPY supervisor process exited with return code %d",
				  returnCode);
		return false;
	}

	return true;
}


/*
 * copydb_process_table_data_worker stats a sub-process that walks through the
 * array of tables to COPY over from the source database to the target
 * database.
 *
 * Each process walks through the entire array, and for each entry:
 *
 *  - acquires a semaphore to enter the critical section, alone
 *    - check if the current entry is already done, or being processed
 *    - if not, create the lock file
 *  - exit the critical section
 *  - if we created a lock file, process the selected table
 */
bool
copydb_process_table_data_worker(CopyDataSpec *specs)
{
	int errors = 0;
	int copies = 0;

	log_notice("Started COPY worker %d [%d]", getpid(), getppid());

	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

	/* connect once to the source database for the whole process */
	if (!copydb_set_snapshot(specs))
	{
		/* errors have already been logged */
		return false;
	}

	for (int tableIndex = 0; tableIndex < tableSpecsArray->count; tableIndex++)
	{
		/* initialize our TableDataProcess entry now */
		CopyTableDataSpec *tableSpecs = &(tableSpecsArray->array[tableIndex]);

		/* reuse the same connection to the source database */
		tableSpecs->sourceSnapshot = specs->sourceSnapshot;

		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			int signal = get_current_signal(SIGTERM);
			const char *signalStr = signal_to_string(signal);

			log_debug("Received signal %s, terminating", signalStr);
			break;
		}

		bool isDone = false;
		bool isBeingProcessed = false;

		if (!copydb_table_is_being_processed(specs,
											 tableSpecs,
											 &isDone,
											 &isBeingProcessed))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * Skip tables that have been entirely done already either on a
		 * previous run, or by a concurrent process while we were busy with our
		 * own work.
		 *
		 * Also skip tables that have been claimed by another of the COPY
		 * worker processes.
		 */
		if (isDone || isBeingProcessed)
		{
			continue;
		}

		/*
		 * 1. Now COPY the TABLE DATA from the source to the destination.
		 */

		/* check for exclude-table-data filtering */
		if (!tableSpecs->sourceTable->excludeData)
		{
			++copies;

			if (!copydb_copy_table(specs, tableSpecs))
			{
				/* errors have already been logged */
				return false;
			}
		}

		/* enter the critical section to communicate that we're done */
		if (!copydb_mark_table_as_done(specs, tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * 2. Send the indexes and constraints attached to this table to the
		 *    index job queue.
		 *
		 * 3. Send the table to the VACUUM ANALYZE job queue.
		 *
		 * If a partial COPY is happening, check that all the other parts are
		 * done. This check should be done in the critical section too. Only
		 * one process can see all parts as done already, and that's the one
		 * finishing last.
		 */
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

		if (specs->dirState.indexCopyIsDone &&
			specs->section != DATA_SECTION_CONSTRAINTS)
		{
			log_info("Skipping indexes, already done on a previous run");
		}
		else if (allPartsDone && !indexesAreBeingProcessed)
		{
			if (!copydb_add_table_indexes(specs, tableSpecs))
			{
				log_warn("Failed to add the indexes for %s, "
						 "see above for details",
						 tableSpecs->qname);
				log_warn("Consider `pgcopydb copy indexes` to try again");
				++errors;
			}

			if (!vacuum_add_table(specs, tableSpecs))
			{
				log_warn("Failed to queue VACUUM ANALYZE %s [%u]",
						 tableSpecs->qname,
						 tableSpecs->sourceTable->oid);
				++errors;
			}
		}
	}

	/* terminate our connection to the source database now */
	(void) copydb_close_snapshot(specs);

	log_debug("copydb_process_table_data_worker: done %d copies, %d errors",
			  copies,
			  errors);

	return errors == 0;
}


/*
 * copydb_table_is_being_processed checks lock and done files to see if a given
 * table is already being processed, or has already been processed entirely by
 * another process. In which case the table is to be skipped by the current
 * process.
 */
bool
copydb_table_is_being_processed(CopyDataSpec *specs,
								CopyTableDataSpec *tableSpecs,
								bool *isDone,
								bool *isBeingProcessed)
{
	if (specs->dirState.tableCopyIsDone)
	{
		log_info("Skipping table %s, already done on a previous run",
				 tableSpecs->qname);

		*isDone = true;
		*isBeingProcessed = false;
		return true;
	}

	/* enter the critical section */
	(void) semaphore_lock(&(specs->tableSemaphore));

	/*
	 * If the doneFile exists, then the table has been processed already,
	 * skip it.
	 *
	 * If the lockFile exists, then the table is currently being processed
	 * by another worker process, skip it.
	 */
	if (file_exists(tableSpecs->tablePaths.doneFile))
	{
		*isDone = true;
		*isBeingProcessed = false;
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
			*isBeingProcessed = true;
			(void) semaphore_unlock(&(specs->tableSemaphore));

			log_trace("Skipping table %s processed by concurrent worker %d",
					  tableSpecs->qname, tableSummary.pid);

			return true;
		}
		else
		{
			log_warn("Found stale pid %d in file \"%s\", removing it "
					 "and processing table %s",
					 tableSummary.pid,
					 tableSpecs->tablePaths.lockFile,
					 tableSpecs->qname);

			/* stale pid, remove the old lockFile now, then process the table */
			if (!unlink_file(tableSpecs->tablePaths.lockFile))
			{
				log_error("Failed to remove the lockFile \"%s\"",
						  tableSpecs->tablePaths.lockFile);
				(void) semaphore_unlock(&(specs->tableSemaphore));
				return false;
			}

			/* pass through to the rest of this function */
		}
	}

	/*
	 * Otherwise, the table is not being processed yet.
	 */
	*isBeingProcessed = false;

	/*
	 * First, write the lockFile, with a summary of what's going-on.
	 */
	CopyTableSummary emptySummary = { 0 };
	CopyTableSummary *summary =
		(CopyTableSummary *) calloc(1, sizeof(CopyTableSummary));

	*summary = emptySummary;

	summary->pid = getpid();
	summary->table = tableSpecs->sourceTable;

	if (IS_EMPTY_STRING_BUFFER(tableSpecs->part.copyQuery))
	{
		sformat(summary->command, sizeof(summary->command),
				"COPY %s",
				tableSpecs->qname);
	}
	else
	{
		sformat(summary->command, sizeof(summary->command),
				"COPY %s",
				tableSpecs->part.copyQuery);
	}

	if (!open_table_summary(summary, tableSpecs->tablePaths.lockFile))
	{
		log_info("Failed to create the lock file for table %s at \"%s\"",
				 tableSpecs->qname,
				 tableSpecs->tablePaths.lockFile);

		/* end of the critical section */
		(void) semaphore_unlock(&(specs->tableSemaphore));

		return false;
	}

	/* attach the new summary to the tableSpecs, where it was NULL before */
	tableSpecs->summary = summary;

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
			  tableSpecs->qname,
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
copydb_copy_table(CopyDataSpec *specs, CopyTableDataSpec *tableSpecs)
{
	/* COPY the data from the source table to the target table */
	if (tableSpecs->section != DATA_SECTION_TABLE_DATA &&
		tableSpecs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping table data in section %d", tableSpecs->section);
		return true;
	}

	/* we want to set transaction snapshot to the main one on the source */
	PGSQL *src = &(tableSpecs->sourceSnapshot.pgsql);
	PGSQL dst = { 0 };

	CopyTableSummary *summary = tableSpecs->summary;

	/* initialize our connection to the target database */
	if (!pgsql_init(&dst, tableSpecs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

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
			if (!pgsql_truncate(&dst, tableSpecs->qname))
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

	/* Now copy the data from source to target */
	log_info("%s", summary->command);

	/* COPY FROM tablename, or maybe COPY FROM (SELECT ... WHERE ...) */
	char *copySource = tableSpecs->qname;

	if (tableSpecs->part.partCount > 1)
	{
		copySource = tableSpecs->part.copyQuery;
	}

	if (!pg_copy(src, &dst, copySource, tableSpecs->qname, truncate))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


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
