/*
 * src/bin/pgcopydb/progress.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parson.h"

#include "catalog.h"
#include "copydb.h"
#include "env_utils.h"
#include "filtering.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "progress.h"
#include "schema.h"
#include "string_utils.h"
#include "summary.h"


static bool copydb_setup_as_json(CopyDataSpec *copySpecs,
								 JSON_Object *jsobj,
								 const char *key);

static bool copydb_filtering_as_json(CopyDataSpec *copySpecs,
									 JSON_Object *jsobj,
									 const char *key);

static bool copydb_table_array_as_json(DatabaseCatalog *sourceDB,
									   JSON_Object *jsobj,
									   const char *key);

static bool copydb_index_array_as_json(DatabaseCatalog *sourceDB,
									   JSON_Object *jsobj,
									   const char *key);

static bool copydb_seq_array_as_json(DatabaseCatalog *sourceDB,
									 JSON_Object *jsobj,
									 const char *key);

static bool copydb_table_array_as_json_hook(void *ctx, SourceTable *table);
static bool copydb_index_array_as_json_hook(void *ctx, SourceIndex *index);
static bool copydb_seq_array_as_json_hook(void *ctx, SourceSequence *seq);

static bool copydb_table_parts_array_as_json_hook(void *ctx,
												  SourceTableParts *part);

static bool copydb_update_progress_table_hook(void *ctx, SourceTable *table);
static bool copydb_update_progress_index_hook(void *ctx, SourceIndex *index);


/*
 * copydb_prepare_schema_json_file prepares a JSON formatted file that contains
 * the list of all the tables and indexes and sequences that are going to be
 * migrated.
 */
bool
copydb_prepare_schema_json_file(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	JSON_Value *js = json_value_init_object();
	JSON_Object *jsobj = json_value_get_object(js);

	log_trace("copydb_prepare_schema_json_file");

	/* main options for the setup */
	if (!copydb_setup_as_json(copySpecs, jsobj, "setup"))
	{
		/* errors have already been logged */
		return false;
	}

	/* filtering, if any */
	if (!copydb_filtering_as_json(copySpecs, jsobj, "filters"))
	{
		/* errors have already been logged */
		return false;
	}

	/* array of tables */
	if (!copydb_table_array_as_json(sourceDB, jsobj, "tables"))
	{
		/* errors have already been logged */
		return false;
	}

	/* array of indexes */
	if (!copydb_index_array_as_json(sourceDB, jsobj, "indexes"))
	{
		/* errors have already been logged */
		return false;
	}

	/* array of sequences */
	if (!copydb_seq_array_as_json(sourceDB, jsobj, "sequences"))
	{
		/* errors have already been logged */
		return false;
	}

	/* now pretty-print the JSON to file */
	char *serialized_string = json_serialize_to_string_pretty(js);
	size_t len = strlen(serialized_string);

	log_notice("Storing migration schema in JSON file \"%s\"",
			   copySpecs->cfPaths.schemafile);

	if (!write_file(serialized_string, len, copySpecs->cfPaths.schemafile))
	{
		log_error("Failed to write schema JSON file, see above for details");
		return false;
	}

	json_free_serialized_string(serialized_string);
	json_value_free(js);

	return true;
}


/*
 * copydb_setup_as_json prepares the filtering setup of the CopyDataSpecs
 * as a JSON object within the given JSON_Value.
 */
static bool
copydb_setup_as_json(CopyDataSpec *copySpecs,
					 JSON_Object *jsobj,
					 const char *key)
{
	JSON_Value *jsSetup = json_value_init_object();
	JSON_Object *jsSetupObj = json_value_get_object(jsSetup);

	/* snapshot */
	if (!IS_EMPTY_STRING_BUFFER(copySpecs->sourceSnapshot.snapshot))
	{
		char *snapshot = copySpecs->sourceSnapshot.snapshot;
		json_object_set_string(jsSetupObj, "snapshot", snapshot);
	}

	/* source and target URIs, without passwords */
	ConnStrings *dsn = &(copySpecs->connStrings);
	char *source = dsn->safeSourcePGURI.pguri;
	char *target = dsn->safeTargetPGURI.pguri;

	json_object_set_string(jsSetupObj, "source_pguri", source);
	json_object_set_string(jsSetupObj, "target_pguri", target);

	json_object_set_number(jsSetupObj,
						   "table-jobs",
						   (double) copySpecs->tableJobs);

	json_object_set_number(jsSetupObj,
						   "index-jobs",
						   (double) copySpecs->indexJobs);

	json_object_set_number(jsSetupObj,
						   "split-tables-larger-than",
						   (double) copySpecs->splitTablesLargerThan.bytes);

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsSetup);

	return true;
}


/*
 * copydb_filtering_as_json prepares the filtering setup of the CopyDataSpecs
 * as a JSON object within the given JSON_Value.
 */
static bool
copydb_filtering_as_json(CopyDataSpec *copySpecs,
						 JSON_Object *jsobj,
						 const char *key)
{
	/* skip section entirely when filtering has not been used */
	if (copySpecs->filters.type == SOURCE_FILTER_TYPE_NONE)
	{
		return true;
	}

	log_trace("copydb_filtering_as_json: filtering");

	SourceFilters *filters = &(copySpecs->filters);
	JSON_Value *jsFilters = json_value_init_object();

	if (!filters_as_json(filters, jsFilters))
	{
		/* errors have already been logged */
		return false;
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsFilters);

	return true;
}


typedef struct TableContext
{
	DatabaseCatalog *sourceDB;
	JSON_Array *jsTableArray;
} TableContext;

/*
 * copydb_table_array_as_json prepares the given tableArray as a JSON array of
 * objects within the given JSON_Value.
 */
static bool
copydb_table_array_as_json(DatabaseCatalog *sourceDB,
						   JSON_Object *jsobj,
						   const char *key)
{
	JSON_Value *jsTables = json_value_init_array();
	JSON_Array *jsTableArray = json_value_get_array(jsTables);

	TableContext context = {
		.sourceDB = sourceDB,
		.jsTableArray = jsTableArray
	};

	if (!catalog_iter_s_table(sourceDB,
							  &context,
							  &copydb_table_array_as_json_hook))
	{
		log_error("Failed to prepare a JSON array for our catalog of tables, "
				  "see above for details");
		return false;
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsTables);

	return true;
}


/*
 * copydb_table_array_as_json_hook is an iterator callback function.
 */
static bool
copydb_table_array_as_json_hook(void *ctx, SourceTable *table)
{
	TableContext *context = (TableContext *) ctx;
	DatabaseCatalog *sourceDB = context->sourceDB;
	JSON_Array *jsTableArray = context->jsTableArray;

	JSON_Value *jsTable = json_value_init_object();
	JSON_Object *jsTableObj = json_value_get_object(jsTable);

	json_object_set_number(jsTableObj, "oid", (double) table->oid);
	json_object_set_string(jsTableObj, "schema", table->nspname);
	json_object_set_string(jsTableObj, "name", table->relname);
	json_object_set_string(jsTableObj, "qname", table->qname);

	json_object_set_number(jsTableObj, "reltuples", (double) table->reltuples);
	json_object_set_number(jsTableObj, "bytes", (double) table->bytes);
	json_object_set_string(jsTableObj, "bytes-pretty", table->bytesPretty);

	json_object_set_boolean(jsTableObj, "exclude-data", table->excludeData);

	json_object_set_string(jsTableObj,
						   "restore-list-name",
						   table->restoreListName);

	json_object_set_string(jsTableObj, "part-key", table->partKey);

	/* now add table attributes (columns) */
	if (!catalog_s_table_fetch_attrs(sourceDB, table))
	{
		/* errors have already been logged */
		return false;
	}

	SourceTableAttributeArray *attributes = &(table->attributes);

	JSON_Value *jsAttrs = json_value_init_array();
	JSON_Array *jsAttrArray = json_value_get_array(jsAttrs);

	for (int attrIndex = 0; attrIndex < attributes->count; attrIndex++)
	{
		SourceTableAttribute *attr = &(attributes->array[attrIndex]);

		JSON_Value *jsAttr = json_value_init_object();
		JSON_Object *jsAttrObj = json_value_get_object(jsAttr);

		json_object_set_number(jsAttrObj, "attnum", attr->attnum);
		json_object_set_number(jsAttrObj, "atttypid", attr->atttypid);
		json_object_set_string(jsAttrObj, "attname", attr->attname);
		json_object_set_boolean(jsAttrObj, "attisprimary", attr->attisprimary);
		json_object_set_boolean(jsAttrObj, "attisgenerated", attr->attisgenerated);

		json_array_append_value(jsAttrArray, jsAttr);
	}

	json_object_set_value(jsTableObj, "cols", jsAttrs);

	/* if we have COPY partitioning, create an array of parts */
	JSON_Value *jsParts = json_value_init_array();
	JSON_Array *jsPartArray = json_value_get_array(jsParts);

	if (table->partition.partCount > 1)
	{
		if (!catalog_iter_s_table_parts(sourceDB,
										table->oid,
										jsPartArray,
										&copydb_table_parts_array_as_json_hook))
		{
			/* errors have already been logged */
			return false;
		}

		json_object_set_value(jsTableObj, "parts", jsParts);
	}

	/* append source and target checksums if we have them */
	if (table->sourceChecksum.rowcount > 0)
	{
		json_object_dotset_number(jsTableObj,
								  "check.source.rowcount",
								  table->sourceChecksum.rowcount);

		json_object_dotset_string(jsTableObj,
								  "check.source.checksum",
								  table->sourceChecksum.checksum);
	}

	if (table->targetChecksum.rowcount > 0)
	{
		json_object_dotset_number(jsTableObj,
								  "check.target.rowcount",
								  table->targetChecksum.rowcount);

		json_object_dotset_string(jsTableObj,
								  "check.target.checksum",
								  table->targetChecksum.checksum);
	}

	json_array_append_value(jsTableArray, jsTable);

	return true;
}


/*
 * copydb_table_parts_array_as_json_hook is an iterator callback function.
 */
static bool
copydb_table_parts_array_as_json_hook(void *ctx, SourceTableParts *part)
{
	JSON_Array *jsPartArray = (JSON_Array *) ctx;

	JSON_Value *jsPart = json_value_init_object();
	JSON_Object *jsPartObj = json_value_get_object(jsPart);

	json_object_set_number(jsPartObj, "number", (double) part->partNumber);
	json_object_set_number(jsPartObj, "total", (double) part->partCount);
	json_object_set_number(jsPartObj, "min", (double) part->min);
	json_object_set_number(jsPartObj, "max", (double) part->max);
	json_object_set_number(jsPartObj, "count", (double) part->count);

	json_array_append_value(jsPartArray, jsPart);

	return true;
}


/*
 * copydb_index_array_as_json prepares the given indexArray as a JSON array of
 * objects within the given JSON_Value.
 */
static bool
copydb_index_array_as_json(DatabaseCatalog *sourceDB,
						   JSON_Object *jsobj,
						   const char *key)
{
	JSON_Value *jsIndexes = json_value_init_array();
	JSON_Array *jsIndexArray = json_value_get_array(jsIndexes);

	if (!catalog_iter_s_index(sourceDB,
							  jsIndexArray,
							  &copydb_index_array_as_json_hook))
	{
		log_error("Failed to prepare a JSON array for our catalog of indexes, "
				  "see above for details");
		return false;
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsIndexes);

	return true;
}


/*
 * copydb_index_array_as_json_hook is an iterator callback function.
 */
static bool
copydb_index_array_as_json_hook(void *ctx, SourceIndex *index)
{
	JSON_Array *jsIndexArray = (JSON_Array *) ctx;

	JSON_Value *jsIndex = json_value_init_object();
	JSON_Object *jsIndexObj = json_value_get_object(jsIndex);

	json_object_set_number(jsIndexObj, "oid", (double) index->indexOid);
	json_object_set_string(jsIndexObj, "schema", index->indexNamespace);
	json_object_set_string(jsIndexObj, "name", index->indexRelname);
	json_object_set_string(jsIndexObj, "qname", index->indexQname);

	json_object_set_boolean(jsIndexObj, "isPrimary", index->isPrimary);
	json_object_set_boolean(jsIndexObj, "isUnique", index->isUnique);

	json_object_set_string(jsIndexObj, "columns", index->indexColumns);
	json_object_set_string(jsIndexObj, "sql", index->indexDef);

	json_object_set_string(jsIndexObj,
						   "restore-list-name",
						   index->indexRestoreListName);

	/* add a table object */
	JSON_Value *jsTable = json_value_init_object();
	JSON_Object *jsTableObj = json_value_get_object(jsTable);

	json_object_set_number(jsTableObj, "oid", (double) index->tableOid);
	json_object_set_string(jsTableObj, "schema", index->tableNamespace);
	json_object_set_string(jsTableObj, "name", index->tableRelname);
	json_object_set_string(jsTableObj, "qname", index->tableQname);

	json_object_set_value(jsIndexObj, "table", jsTable);

	/* add a constraint object */
	if (index->constraintOid != 0)
	{
		JSON_Value *jsConstraint = json_value_init_object();
		JSON_Object *jsConstraintObj = json_value_get_object(jsConstraint);

		json_object_set_number(jsConstraintObj,
							   "oid",
							   (double) index->constraintOid);

		json_object_set_string(jsConstraintObj,
							   "name",
							   index->constraintName);

		json_object_set_string(jsConstraintObj,
							   "sql",
							   index->constraintDef);

		json_object_set_string(jsConstraintObj,
							   "restore-list-name",
							   index->constraintRestoreListName);

		json_object_set_value(jsIndexObj, "constraint", jsConstraint);
	}

	/* append the JSON index to the index table */
	json_array_append_value(jsIndexArray, jsIndex);

	return true;
}


/*
 * copydb_seq_array_as_json prepares the given sequencesArray as a JSON array
 * of objects within the given JSON_Value.
 */
static bool
copydb_seq_array_as_json(DatabaseCatalog *sourceDB,
						 JSON_Object *jsobj,
						 const char *key)
{
	JSON_Value *jsSeqs = json_value_init_array();
	JSON_Array *jsSeqArray = json_value_get_array(jsSeqs);

	if (!catalog_iter_s_seq(sourceDB,
							jsSeqArray,
							&copydb_seq_array_as_json_hook))
	{
		log_error("Failed to prepare a JSON array for our catalog of sequences, "
				  "see above for details");
		return false;
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsSeqs);

	return true;
}


/*
 * copydb_seq_array_as_json_hook is an iterator callback function.
 */
static bool
copydb_seq_array_as_json_hook(void *ctx, SourceSequence *seq)
{
	JSON_Array *jsSeqArray = (JSON_Array *) ctx;

	JSON_Value *jsSeq = json_value_init_object();
	JSON_Object *jsSeqObj = json_value_get_object(jsSeq);

	json_object_set_number(jsSeqObj, "oid", (double) seq->oid);
	json_object_set_string(jsSeqObj, "schema", seq->nspname);
	json_object_set_string(jsSeqObj, "name", seq->relname);
	json_object_set_string(jsSeqObj, "qname", seq->qname);

	json_object_set_number(jsSeqObj, "last-value", (double) seq->lastValue);
	json_object_set_boolean(jsSeqObj, "is-called", (double) seq->isCalled);

	json_object_set_string(jsSeqObj,
						   "restore-list-name",
						   seq->restoreListName);

	json_array_append_value(jsSeqArray, jsSeq);

	return true;
}


typedef struct TableProgressContext
{
	CopyDataSpec *copySpecs;
	CopyProgress *progress;
} TableProgressContext;


/*
 * copydb_update_progress updates the progress counters with information found
 * on-disk in the work directory (lock and done files, etc).
 */
bool
copydb_update_progress(CopyDataSpec *copySpecs, CopyProgress *progress)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count indexes and constraints in our catalogs");
		return false;
	}

	progress->tableCount = count.tables;
	progress->indexCount = count.indexes;

	log_debug("copydb_update_progress for %d tables, %d indexes",
			  progress->tableCount,
			  progress->indexCount);

	/* count table in progress, table done */
	progress->tableDoneCount = 0;
	progress->tableInProgress.count = 0;

	/* we can't have more table in progress than tableJobs */
	progress->tableInProgress.array =
		(SourceTable *) calloc(copySpecs->tableJobs, sizeof(SourceTable));

	if (progress->tableInProgress.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	progress->tableSummaryArray.count = 0;
	progress->tableSummaryArray.array =
		(CopyTableSummary *) calloc(copySpecs->tableJobs,
									sizeof(CopyTableSummary));

	if (progress->tableSummaryArray.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	TableProgressContext context = {
		.copySpecs = copySpecs,
		.progress = progress
	};

	if (!catalog_iter_s_table_in_copy(sourceDB,
									  &context,
									  &copydb_update_progress_table_hook))
	{
		/* errors have already been logged */
		return false;
	}

	/* count index in progress, index done */
	progress->indexDoneCount = 0;
	progress->indexInProgress.count = 0;

	/* we can't have more index in progress than indexJobs */
	progress->indexInProgress.array =
		(SourceIndex *) calloc(copySpecs->indexJobs, sizeof(SourceIndex));

	if (progress->indexInProgress.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	progress->indexSummaryArray.count = 0;
	progress->indexSummaryArray.array =
		(CopyIndexSummary *) calloc(copySpecs->indexJobs,
									sizeof(CopyIndexSummary));

	if (progress->indexSummaryArray.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_iter_s_index_in_progress(sourceDB,
										  &context,
										  &copydb_update_progress_index_hook))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_update_progress_table_hook is an iterator callback function.
 */
static bool
copydb_update_progress_table_hook(void *ctx, SourceTable *table)
{
	TableProgressContext *context = (TableProgressContext *) ctx;

	CopyDataSpec *copySpecs = context->copySpecs;
	CopyProgress *progress = context->progress;

	SourceTableArray *tableInProgress = &(progress->tableInProgress);
	CopyTableSummaryArray *summaryArray = &(progress->tableSummaryArray);

	int partCount = table->partition.partCount;

	CopyTableDataSpec tableSpecs = { 0 };

	if (!copydb_init_table_specs(&tableSpecs, copySpecs, table, 0))
	{
		/* errors have already been logged */
		return false;
	}

	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	if (!summary_lookup_table(sourceDB, &tableSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Copy the SourceTable struct in-place to the tableInProgress array.
	 */
	tableInProgress->array[progress->tableInProgress.count++] = *table;
	summaryArray->array[progress->tableSummaryArray.count++] = tableSpecs.summary;

	bool done = false;

	if (partCount <= 1)
	{
		if (tableSpecs.summary.doneTime > 0)
		{
			done = true;
		}
	}
	else
	{
		if (!summary_lookup_table_parts_done(sourceDB, &tableSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		done = tableSpecs.partsDonePid > 0;
	}

	if (done)
	{
		++progress->tableDoneCount;
	}

	return true;
}


/*
 * copydb_update_progress_index_hook is an iterator callback function.
 */
static bool
copydb_update_progress_index_hook(void *ctx, SourceIndex *index)
{
	TableProgressContext *context = (TableProgressContext *) ctx;

	CopyDataSpec *copySpecs = context->copySpecs;
	CopyProgress *progress = context->progress;

	SourceIndexArray *indexInProgress = &(progress->indexInProgress);
	CopyIndexSummaryArray *summaryArrayIdx = &(progress->indexSummaryArray);

	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);
	CopyIndexSpec indexSpecs = { .sourceIndex = index };

	if (!summary_lookup_index(sourceDB, &indexSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (indexSpecs.summary.pid > 0)
	{
		/*
		 * Copy the SourceIndex struct in-place to the indexInProgress
		 * array.
		 */
		indexInProgress->array[progress->indexInProgress.count++] =
			*index;

		summaryArrayIdx->array[progress->indexSummaryArray.count++] =
			indexSpecs.summary;
	}

	return true;
}


/*
 * copydb_progress_as_json prepares the given JSON value with the current
 * progress from a pgcopydb command (that might be running still).
 */
bool
copydb_progress_as_json(CopyDataSpec *copySpecs,
						CopyProgress *progress,
						JSON_Value *js)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	JSON_Object *jsobj = json_value_get_object(js);

	json_object_set_number(jsobj, "table-jobs", copySpecs->tableJobs);
	json_object_set_number(jsobj, "index-jobs", copySpecs->indexJobs);

	/* table counts */
	JSON_Value *jsTable = json_value_init_object();
	JSON_Object *jsTableObj = json_value_get_object(jsTable);

	json_object_set_number(jsTableObj, "total", progress->tableCount);
	json_object_set_number(jsTableObj, "done", progress->tableDoneCount);

	/* in-progress */
	SourceTableArray *tableArray = &(progress->tableInProgress);

	if (!copydb_table_array_as_json(sourceDB, jsTableObj, "in-progress"))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now patch the JSON array table objects with information from the summary
	 * file, such as the PID, startTime etc.
	 */
	JSON_Array *jsTableArray = json_object_get_array(jsTableObj, "in-progress");

	for (int i = 0; i < tableArray->count; i++)
	{
		JSON_Object *jsTableObj = json_array_get_object(jsTableArray, i);
		CopyTableSummary *summary = &(progress->tableSummaryArray.array[i]);

		if (!prepare_table_summary_as_json(summary, jsTableObj, "process"))
		{
			/* errors have already been logged */
			return false;
		}
	}

	json_object_set_value(jsobj, "tables", jsTable);

	/* index counts */
	JSON_Value *jsIndex = json_value_init_object();
	JSON_Object *jsIndexObj = json_value_get_object(jsIndex);

	json_object_set_number(jsIndexObj, "total", progress->indexCount);
	json_object_set_number(jsIndexObj, "done", progress->indexDoneCount);

	/* in-progress */
	SourceIndexArray *indexArray = &(progress->indexInProgress);

	if (!copydb_index_array_as_json(sourceDB, jsIndexObj, "in-progress"))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Now patch the JSON array index objects with information from the summary
	 * file, such as the PID, startTime etc.
	 */
	JSON_Array *jsIndexArray = json_object_get_array(jsIndexObj, "in-progress");

	for (int i = 0; i < indexArray->count; i++)
	{
		JSON_Object *jsIndexObj = json_array_get_object(jsIndexArray, i);
		CopyIndexSummary *summary = &(progress->indexSummaryArray.array[i]);

		if (!prepare_index_summary_as_json(summary, jsIndexObj, "process"))
		{
			/* errors have already been logged */
			return false;
		}
	}

	json_object_set_value(jsobj, "indexes", jsIndex);

	return true;
}
