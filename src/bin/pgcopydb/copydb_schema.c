/*
 * src/bin/pgcopydb/copydb_schema.c
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

		if (IS_EMPTY_STRING_BUFFER(specs->sourceSnapshot.snapshot))
		{
			log_error("Trying to re-use snapshot \"%s\"",
					  specs->sourceSnapshot.snapshot);
			return false;
		}

		src = &(specs->sourceSnapshot.pgsql);
	}
	else
	{
		log_debug("--not-consistent, create a fresh connection");
		if (!pgsql_init(&pgsql, specs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
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

	/* check if we're connected to a standby server, which we don't support */
	bool pg_is_in_recovery = false;

	if (!pgsql_is_in_recovery(src, &pg_is_in_recovery))
	{
		/* errors have already been logged */
		return false;
	}

	if (pg_is_in_recovery)
	{
		log_fatal("Connected to a standby server where pg_is_in_recovery(): "
				  "pgcopydb does not support operating on standby server "
				  "at this point, as it needs to create temp tables");
		return false;
	}

	/* check if we have needed privileges here */
	if (!schema_query_privileges(src,
								 &(specs->hasDBCreatePrivilege),
								 &(specs->hasDBTempPrivilege)))
	{
		log_error("Failed to query database privileges, see above for details");
		return false;
	}

	if (!specs->hasDBTempPrivilege)
	{
		log_fatal("Connecting with a role that does not have TEMP privileges "
				  "on the current database on the source server");
		return false;
	}

	if (specs->hasDBCreatePrivilege)
	{
		if (!pgsql_prepend_search_path(src, "pgcopydb"))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		log_warn("Connecting with a role that does not have CREATE privileges "
				 "on the source database discards pg_table_size() caching");
	}

	/* first, are we doing extensions? */
	if (specs->section == DATA_SECTION_ALL ||
		specs->section == DATA_SECTION_EXTENSION)
	{
		SourceExtensionArray *extensionArray = &(specs->catalog.extensionArray);

		if (!schema_list_extensions(src, extensionArray))
		{
			/* errors have already been logged */
			return false;
		}

		log_info("Fetched information for %d extensions", extensionArray->count);
	}

	/* now, are we skipping collations? */
	if (specs->skipCollations)
	{
		SourceCollationArray *collationArray = &(specs->catalog.collationArray);

		if (!schema_list_collations(src, collationArray))
		{
			/* errors have already been logged */
			return false;
		}

		log_info("Fetched information for %d collations", collationArray->count);
	}

	/*
	 * First, if it doesn't exist yet, create the pgcopydb.table_size table.
	 * Keep track of whether we had to create that table, if we did, it is
	 * expected that we DROP it before the end of this transaction.
	 *
	 * In order to allow for users to prepare that table in advance, we do not
	 * use a TEMP table here.
	 */
	bool createdTableSizeTable = false;

	/* copydb_fetch_filtered_oids() needs the table size table around */
	if (!schema_prepare_pgcopydb_table_size(src,
											&(specs->filters),
											specs->hasDBCreatePrivilege,
											false, /* cache */
											false, /* dropCache */
											&createdTableSizeTable))
	{
		/* errors have already been logged */
		return false;
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
		if (!copydb_prepare_index_specs(specs, src))
		{
			/* errors have already been logged */
			return false;
		}
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

	if (createdTableSizeTable)
	{
		if (!schema_drop_pgcopydb_table_size(src))
		{
			/* errors have already been logged */
			return false;
		}
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

	/*
	 * Now also fetch the list of schemas from the target database.
	 */
	if (!copydb_prepare_target_catalog(specs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_prepare_table_specs fetches the list of tables to COPY data from the
 * source and into the target, and initialises our internal
 * CopyTableDataSpecsArray to drive the operations.
 */
bool
copydb_prepare_table_specs(CopyDataSpec *specs, PGSQL *pgsql)
{
	SourceTableArray *tableArray = &(specs->catalog.sourceTableArray);
	CopyTableDataSpecsArray *tableSpecsArray = &(specs->tableSpecsArray);

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

	if (specs->splitTablesLargerThan.bytes > 0)
	{
		log_info("Splitting source candidate tables larger than %s",
				 specs->splitTablesLargerThan.bytesPretty);
	}

	/* prepare a SourceTable hash table, indexed by Oid, and qualified name */
	SourceTable *sourceTableHashByOid = NULL;
	SourceTable *sourceTableHashByQName = NULL;

	/*
	 * Source table might be split in several concurrent COPY processes. In
	 * that case we produce a CopyDataSpec entry for each COPY partition.
	 */
	for (int tableIndex = 0; tableIndex < tableArray->count; tableIndex++)
	{
		SourceTable *source = &(tableArray->array[tableIndex]);

		/* add the current table to the Hash-by-OID */
		HASH_ADD(hh, sourceTableHashByOid, oid, sizeof(uint32_t), source);

		/* also add the current table to the Hash-by-QName */
		size_t len = strlen(source->qname);
		HASH_ADD(hhQName, sourceTableHashByQName, qname, len, source);

		if (specs->splitTablesLargerThan.bytes > 0 &&
			specs->splitTablesLargerThan.bytes <= source->bytes)
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
						 specs->splitTablesLargerThan.bytesPretty);

				log_warn("Skipping same-table concurrency for table \"%s\".\"%s\"",
						 source->nspname,
						 source->relname);

				++copySpecsCount;
				continue;
			}

			if (!schema_list_partitions(pgsql,
										source,
										specs->splitTablesLargerThan.bytes))
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
	specs->catalog.sourceTableHashByOid = sourceTableHashByOid;
	specs->catalog.sourceTableHashByQName = sourceTableHashByQName;

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
 * copydb_prepare_index_specs fetches the list of indexes to create again on
 * the target database, and set our internal hash table entries with a
 * linked-list of indexes per-table.
 */
bool
copydb_prepare_index_specs(CopyDataSpec *specs, PGSQL *pgsql)
{
	SourceIndexArray *indexArray = &(specs->catalog.sourceIndexArray);

	if (!schema_list_all_indexes(pgsql, &(specs->filters), indexArray))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Fetched information for %d indexes", indexArray->count);

	/*
	 * Now build a SourceIndexList per table, when we retrieved both the table
	 * list and the indexes list.
	 */
	if (specs->section == DATA_SECTION_ALL)
	{
		/* now build the index hash-table */
		SourceIndex *sourceIndexHashByOid = NULL;
		SourceIndexArray *indexArray = &(specs->catalog.sourceIndexArray);

		SourceTable *sourceTableHashByOid = specs->catalog.sourceTableHashByOid;

		for (int i = 0; i < indexArray->count; i++)
		{
			SourceIndex *index = &(indexArray->array[i]);

			/* add the current index to the index Hash-by-OID */
			HASH_ADD(hh, sourceIndexHashByOid, indexOid, sizeof(uint32_t), index);

			/* find the index table, update its index list */
			uint32_t oid = index->tableOid;
			SourceTable *table = NULL;

			HASH_FIND(hh, sourceTableHashByOid, &oid, sizeof(oid), table);

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
		specs->catalog.sourceIndexHashByOid = sourceIndexHashByOid;
	}

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

	if (specs->skipExtensions)
	{
		SourceSchemaArray schemaArray = { 0, NULL };

		/* fetch the list of schemas that extensions depend on */
		if (!schema_list_ext_schemas(pgsql, &schemaArray))
		{
			/* errors have already been logged */
			return false;
		}

		for (int i = 0; i < schemaArray.count; i++)
		{
			SourceSchema *schema = &(schemaArray.array[i]);

			SourceFilterItem *item = malloc(sizeof(SourceFilterItem));

			if (item == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			item->oid = schema->oid;
			item->kind = OBJECT_KIND_SCHEMA;
			item->schema = *schema;

			strlcpy(item->restoreListName,
					schema->restoreListName,
					RESTORE_LIST_NAMEDATALEN);

			HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), item);

			size_t len = strlen(item->restoreListName);
			HASH_ADD(hName, hName, restoreListName, len, item);
		}

		/* free dynamic memory that's not needed anymore */
		free(schemaArray.array);

		/*
		 * The main extensionArray can be used both for filtering the
		 * pg_restore archive catalog, as we either filter all of the
		 * extensions or none of them.
		 */
		for (int i = 0; i < specs->catalog.extensionArray.count; i++)
		{
			SourceExtension *ext = &(specs->catalog.extensionArray.array[i]);

			SourceFilterItem *item = malloc(sizeof(SourceFilterItem));

			if (item == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			item->oid = ext->oid;
			item->kind = OBJECT_KIND_EXTENSION;
			item->extension = *ext;

			/* an extension's pg_restore list name is just its name */
			strlcpy(item->restoreListName,
					ext->extname,
					RESTORE_LIST_NAMEDATALEN);

			HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), item);

			size_t len = strlen(item->restoreListName);
			HASH_ADD(hName, hName, restoreListName, len, item);
		}
	}

	if (specs->skipCollations)
	{
		/*
		 * Add all the listed collations OIDs so as to skip them later.
		 */
		for (int i = 0; i < specs->catalog.collationArray.count; i++)
		{
			SourceCollation *coll = &(specs->catalog.collationArray.array[i]);
			SourceFilterItem *item = malloc(sizeof(SourceFilterItem));

			if (item == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			item->oid = coll->oid;
			item->kind = OBJECT_KIND_COLLATION;
			item->collation = *coll;

			strlcpy(item->restoreListName,
					coll->restoreListName,
					RESTORE_LIST_NAMEDATALEN);

			/*
			 * schema_list_collations might return same collation several
			 * times, so we need to be careful here when adding entries to the
			 * hash table.
			 */
			uint32_t oid = item->oid;
			SourceFilterItem *found = { 0 };

			HASH_FIND(hOid, hOid, &oid, sizeof(uint32_t), found);

			if (found == NULL)
			{
				HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), item);

				size_t len = strlen(item->restoreListName);
				HASH_ADD(hName, hName, restoreListName, len, item);
			}
		}
	}

	/*
	 * Take the complement of the filtering, to list the OIDs of objects that
	 * we do not process.
	 */
	SourceFilterType type = filters->type;

	filters->type = filterTypeComplement(type);

	if (filters->type == SOURCE_FILTER_TYPE_NONE)
	{
		if (specs->skipExtensions || specs->skipCollations)
		{
			/* publish our hash tables to the main CopyDataSpec instance */
			specs->hOid = hOid;
			specs->hName = hName;
		}
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

		/*
		 * Filtering-out sequences work with the following 3 Archive Catalog
		 * entry kinds:
		 *
		 *  - SEQUENCE, matched by sequence oid
		 *  - SEQUENCE OWNED BY, matched by sequence restore name
		 *  - DEFAULT, matched by attribute oid
		 *
		 * In some cases we want to create the sequence, but we might want to
		 * skip the SEQUENCE OWNED BY statement, because we didn't actually
		 * create the owner table.
		 *
		 * In those cases we will find the sequence both in the catalogs of
		 * objects we want to migrate, and also in the list of objects we want
		 * to skip. The catalog entry typically has seq->ownedby !=
		 * seq->attrelid, where the ownedby table is skipped from the migration
		 * because of the filtering.
		 */
		SourceSequence *sourceSeqHashByOid = specs->catalog.sourceSeqHashByOid;

		uint32_t sOid = seq->oid;
		SourceSequence *sequence = NULL;

		HASH_FIND(hh, sourceSeqHashByOid, &sOid, sizeof(sOid), sequence);

		/*
		 * When we find the sequence in our catalog selection, then we still
		 * create it and refrain to add the sequence oid to our hash table
		 * here.
		 */
		if (sequence == NULL)
		{
			HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), item);
		}

		/* find if the SEQUENCE OWNED BY table is in our catalog selection */
		SourceTable *sourceTableHashByOid = specs->catalog.sourceTableHashByOid;

		uint32_t tOid = seq->ownedby;
		SourceTable *table = NULL;

		HASH_FIND(hh, sourceTableHashByOid, &tOid, sizeof(tOid), table);

		/*
		 * Only filter-out the SEQUENCE OWNED BY when our catalog selection
		 * does not contain the target table.
		 */
		if (table == NULL)
		{
			size_t len = strlen(seq->restoreListName);
			HASH_ADD(hName, hName, restoreListName, len, item);
		}

		/*
		 * Also add pg_attribute.oid when it's not null (non-zero here). This
		 * takes care of the DEFAULT entries in the pg_dump Archive Catalog,
		 * and these entries target the attroid directly.
		 */
		if (seq->attroid > 0)
		{
			SourceFilterItem *attrItem = malloc(sizeof(SourceFilterItem));

			if (attrItem == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			attrItem->oid = seq->attroid;
			attrItem->kind = OBJECT_KIND_DEFAULT;
			attrItem->sequence = *seq;

			strlcpy(attrItem->restoreListName,
					seq->restoreListName,
					RESTORE_LIST_NAMEDATALEN);

			HASH_ADD(hOid, hOid, oid, sizeof(uint32_t), attrItem);
		}
	}

	/* finally table dependencies */
	for (int i = 0; i < dependArray.count; i++)
	{
		SourceDepend *depend = &(dependArray.array[i]);

		/*
		 * In some cases with sequences we might want to skip adding a
		 * dependency in our hash table here. See the previous discussion for
		 * details.
		 */
		SourceSequence *sourceSeqHashByOid = specs->catalog.sourceSeqHashByOid;

		uint32_t sOid = depend->objid;
		SourceSequence *sequence = NULL;

		HASH_FIND(hh, sourceSeqHashByOid, &sOid, sizeof(sOid), sequence);

		if (sequence != NULL)
		{
			continue;
		}

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

		case OBJECT_KIND_SCHEMA:
		{
			return "schema";
		}

		case OBJECT_KIND_EXTENSION:
		{
			return "extension";
		}

		case OBJECT_KIND_COLLATION:
		{
			return "collation";
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
		{
			return "sequence";
		}

		case OBJECT_KIND_DEFAULT:
		{
			return "default";
		}
	}

	return "unknown";
}


/*
 * copydb_prepare_target_catalog connects to the target database and fetches
 * pieces of the catalogs that we need, such as the list of the already
 * existing schemas.
 */
bool
copydb_prepare_target_catalog(CopyDataSpec *specs)
{
	PGSQL dst = { 0 };

	if (specs->connStrings.target_pguri == NULL)
	{
		log_notice("Skipping target catalog preparation");
		return true;
	}

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	SourceSchema *schemaHashByName = NULL;
	SourceSchemaArray *schemaArray = &(specs->targetCatalog.schemaArray);

	if (!schema_list_schemas(&dst, schemaArray))
	{
		log_error("Failed to list schemas on the target database");
		return false;
	}

	for (int i = 0; i < schemaArray->count; i++)
	{
		SourceSchema *schema = &(schemaArray->array[i]);
		size_t len = strlen(schema->nspname);

		HASH_ADD(hh, schemaHashByName, nspname, len, schema);
	}

	specs->targetCatalog.schemaHashByName = schemaHashByName;

	return true;
}


/*
 * copydb_schema_already_exists checks if the given SCHEMA name extracted from
 * a pg_dump Archive matches an existing schema name on the target database.
 */
bool
copydb_schema_already_exists(CopyDataSpec *specs,
							 const char *restoreListName,
							 bool *exists)
{
	SourceSchema *schemaHashByName = specs->targetCatalog.schemaHashByName;

	if (strncmp(restoreListName, "- ", 2) != 0)
	{
		log_error("Failed to parse restore list name \"%s\"", restoreListName);
		return false;
	}

	char *name = strdup(restoreListName + 2);

	if (name == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *spc = strchr(name, ' ');

	if (spc == NULL)
	{
		log_error("Failed to parse restore list name \"%s\"", restoreListName);
		return false;
	}

	*spc = '\0';

	SourceSchema *schema = NULL;
	size_t len = strlen(name);

	HASH_FIND(hh, schemaHashByName, name, len, schema);

	*exists = schema != NULL;

	return true;
}
