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

#include "copydb.h"
#include "env_utils.h"
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

static bool copydb_table_array_as_json(SourceTableArray *tableArray,
									   JSON_Object *jsobj,
									   const char *key);

static bool copydb_index_array_as_json(SourceIndexArray *indexArray,
									   JSON_Object *jsobj,
									   const char *key);

static bool copydb_seq_array_as_json(SourceSequenceArray *sequenceArray,
									 JSON_Object *jsobj,
									 const char *key);

/*
 * copydb_prepare_schema_json_file prepares a JSON formatted file that contains
 * the list of all the tables and indexes and sequences that are going to be
 * migrated.
 */
bool
copydb_prepare_schema_json_file(CopyDataSpec *copySpecs)
{
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
	SourceTableArray *tableArray = &(copySpecs->catalog.sourceTableArray);

	log_trace("copydb_prepare_schema_json_file: %d tables", tableArray->count);

	if (!copydb_table_array_as_json(tableArray, jsobj, "tables"))
	{
		/* errors have already been logged */
		return false;
	}

	/* array of indexes */
	SourceIndexArray *indexArray = &(copySpecs->catalog.sourceIndexArray);

	log_trace("copydb_prepare_schema_json_file: %d indexes", indexArray->count);

	if (!copydb_index_array_as_json(indexArray, jsobj, "indexes"))
	{
		/* errors have already been logged */
		return false;
	}

	/* array of sequences */
	SourceSequenceArray *sequenceArray = &(copySpecs->catalog.sequenceArray);

	log_trace("copydb_prepare_schema_json_file: %d sequences",
			  sequenceArray->count);

	if (!copydb_seq_array_as_json(sequenceArray, jsobj, "sequences"))
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

	JSON_Value *jsFilter = json_value_init_object();
	JSON_Object *jsFilterObj = json_value_get_object(jsFilter);

	json_object_set_string(jsFilterObj,
						   "type",
						   filterTypeToString(filters->type));

	/* include-only-schema */
	if (filters->includeOnlySchemaList.count > 0)
	{
		JSON_Value *jsSchema = json_value_init_array();
		JSON_Array *jsSchemaArray = json_value_get_array(jsSchema);

		for (int i = 0; i < filters->includeOnlySchemaList.count; i++)
		{
			char *nspname = filters->includeOnlySchemaList.array[i].nspname;

			json_array_append_string(jsSchemaArray, nspname);
		}

		json_object_set_value(jsFilterObj, "include-only-schema", jsSchema);
	}

	/* exclude-schema */
	if (filters->excludeSchemaList.count > 0)
	{
		JSON_Value *jsSchema = json_value_init_array();
		JSON_Array *jsSchemaArray = json_value_get_array(jsSchema);

		for (int i = 0; i < filters->excludeSchemaList.count; i++)
		{
			char *nspname = filters->excludeSchemaList.array[i].nspname;

			json_array_append_string(jsSchemaArray, nspname);
		}

		json_object_set_value(jsFilterObj, "exclude-schema", jsSchema);
	}

	/* exclude table lists */
	struct section
	{
		char name[PG_NAMEDATALEN];
		SourceFilterTableList *list;
	};

	struct section sections[] = {
		{ "exclude-table", &(filters->excludeTableList) },
		{ "exclude-table-data", &(filters->excludeTableDataList) },
		{ "exclude-index", &(filters->excludeIndexList) },
		{ "include-only-table", &(filters->includeOnlyTableList) },
		{ "", NULL },
	};

	for (int i = 0; sections[i].list != NULL; i++)
	{
		char *sectionName = sections[i].name;
		SourceFilterTableList *list = sections[i].list;

		if (list->count > 0)
		{
			JSON_Value *jsList = json_value_init_array();
			JSON_Array *jsListArray = json_value_get_array(jsList);

			for (int i = 0; i < list->count; i++)
			{
				SourceFilterTable *table = &(list->array[i]);

				JSON_Value *jsTable = json_value_init_object();
				JSON_Object *jsTableObj = json_value_get_object(jsTable);

				json_object_set_string(jsTableObj, "schema", table->nspname);
				json_object_set_string(jsTableObj, "name", table->relname);

				json_array_append_value(jsListArray, jsTable);
			}

			json_object_set_value(jsFilterObj, sectionName, jsList);
		}
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsFilter);

	return true;
}


/*
 * copydb_table_array_as_json prepares the given tableArray as a JSON array of
 * objects within the given JSON_Value.
 */
static bool
copydb_table_array_as_json(SourceTableArray *tableArray,
						   JSON_Object *jsobj,
						   const char *key)
{
	JSON_Value *jsTables = json_value_init_array();
	JSON_Array *jsTableArray = json_value_get_array(jsTables);

	for (int tableIndex = 0; tableIndex < tableArray->count; tableIndex++)
	{
		SourceTable *table = &(tableArray->array[tableIndex]);

		log_trace("copydb_prepare_schema_json_file: tables[%d]: %s.%s",
				  tableIndex,
				  table->nspname,
				  table->relname);

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

			json_array_append_value(jsAttrArray, jsAttr);
		}

		json_object_set_value(jsTableObj, "cols", jsAttrs);

		/* if we have COPY partitioning, create an array of parts */
		JSON_Value *jsParts = json_value_init_array();
		JSON_Array *jsPartArray = json_value_get_array(jsParts);

		if (table->partsArray.count > 1)
		{
			for (int i = 0; i < table->partsArray.count; i++)
			{
				SourceTableParts *part = &(table->partsArray.array[i]);

				JSON_Value *jsPart = json_value_init_object();
				JSON_Object *jsPartObj = json_value_get_object(jsPart);

				json_object_set_number(jsPartObj, "number",
									   (double) part->partNumber);

				json_object_set_number(jsPartObj, "total",
									   (double) part->partCount);

				json_object_set_number(jsPartObj, "min",
									   (double) part->min);

				json_object_set_number(jsPartObj, "max",
									   (double) part->max);

				json_object_set_number(jsPartObj, "count",
									   (double) part->count);

				json_array_append_value(jsPartArray, jsPart);
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
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsTables);

	return true;
}


/*
 * copydb_index_array_as_json prepares the given indexArray as a JSON array of
 * objects within the given JSON_Value.
 */
static bool
copydb_index_array_as_json(SourceIndexArray *indexArray,
						   JSON_Object *jsobj,
						   const char *key)
{
	JSON_Value *jsIndexes = json_value_init_array();
	JSON_Array *jsIndexArray = json_value_get_array(jsIndexes);

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);

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
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsIndexes);

	return true;
}


/*
 * copydb_seq_array_as_json prepares the given sequencesArray as a JSON array
 * of objects within the given JSON_Value.
 */
static bool
copydb_seq_array_as_json(SourceSequenceArray *sequenceArray,
						 JSON_Object *jsobj,
						 const char *key)
{
	JSON_Value *jsSeqs = json_value_init_array();
	JSON_Array *jsSeqArray = json_value_get_array(jsSeqs);

	for (int seqIndex = 0; seqIndex < sequenceArray->count; seqIndex++)
	{
		SourceSequence *seq = &(sequenceArray->array[seqIndex]);

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
	}

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsSeqs);

	return true;
}


/*
 * copydb_parse_schema_json_file parses the JSON file prepared with
 * copydb_prepare_schema_json_file and fills-in the given CopyDataSpec
 * structure with the information found in the JSON file.
 */
bool
copydb_parse_schema_json_file(CopyDataSpec *copySpecs)
{
	log_notice("Reading catalogs from file \"%s\"",
			   copySpecs->cfPaths.schemafile);

	if (!file_exists(copySpecs->cfPaths.schemafile))
	{
		log_error("Failed to parse JSON file \"%s\": file does not exists",
				  copySpecs->cfPaths.schemafile);
		return false;
	}

	JSON_Value *json = json_parse_file(copySpecs->cfPaths.schemafile);

	if (json == NULL)
	{
		log_error("Failed to parse JSON file \"%s\"",
				  copySpecs->cfPaths.schemafile);
		return false;
	}

	JSON_Object *jsObj = json_value_get_object(json);

	/* setup section */
	copySpecs->tableJobs = json_object_dotget_number(jsObj, "setup.table-jobs");
	copySpecs->indexJobs = json_object_dotget_number(jsObj, "setup.index-jobs");

	/* table section */
	JSON_Array *jsTableArray = json_object_get_array(jsObj, "tables");
	int tableCount = json_array_get_count(jsTableArray);

	log_debug("copydb_parse_schema_json_file: parsing %d tables", tableCount);

	copySpecs->catalog.sourceTableArray.count = tableCount;
	copySpecs->catalog.sourceTableArray.array =
		(SourceTable *) calloc(tableCount, sizeof(SourceTable));

	if (copySpecs->catalog.sourceTableArray.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* prepare the SourceTable hash tables */
	SourceTable *sourceTableHashByOid = NULL;
	SourceTable *sourceTableHashByQName = NULL;

	for (int tableIndex = 0; tableIndex < tableCount; tableIndex++)
	{
		SourceTable *table = &(copySpecs->catalog.sourceTableArray.array[tableIndex]);
		JSON_Object *jsTable = json_array_get_object(jsTableArray, tableIndex);

		table->oid = json_object_get_number(jsTable, "oid");

		char *schema = (char *) json_object_get_string(jsTable, "schema");
		char *name = (char *) json_object_get_string(jsTable, "name");
		char *qname = (char *) json_object_get_string(jsTable, "qname");

		char *bytesPretty =
			(char *) json_object_get_string(jsTable, "bytes-pretty");

		char *restoreListName =
			(char *) json_object_get_string(jsTable, "restore-list-name");

		char *partKey = (char *) json_object_get_string(jsTable, "part-key");

		strlcpy(table->nspname, schema, sizeof(table->nspname));
		strlcpy(table->relname, name, sizeof(table->relname));
		strlcpy(table->qname, qname, sizeof(table->qname));

		/* add the current table to the Hash-by-OID */
		HASH_ADD(hh, sourceTableHashByOid, oid, sizeof(uint32_t), table);

		/* also add the current table to the Hash-by-QName */
		size_t len = strlen(table->qname);
		HASH_ADD(hhQName, sourceTableHashByQName, qname, len, table);

		table->reltuples = json_object_get_number(jsTable, "reltuples");
		table->bytes = json_object_get_number(jsTable, "bytes");
		table->excludeData = json_object_get_boolean(jsTable, "exclude-data");

		strlcpy(table->bytesPretty, bytesPretty, sizeof(table->bytesPretty));

		strlcpy(table->restoreListName,
				restoreListName,
				sizeof(table->restoreListName));

		strlcpy(table->partKey, partKey, sizeof(table->partKey));

		if (json_object_has_value(jsTable, "cols"))
		{
			JSON_Array *jsAttrsArray = json_object_get_array(jsTable, "cols");
			int attrsCount = json_array_get_count(jsAttrsArray);

			table->attributes.count = attrsCount;
			table->attributes.array =
				(SourceTableAttribute *)
				calloc(attrsCount, sizeof(SourceTableAttribute));

			if (table->attributes.array == NULL)
			{
				log_fatal(ALLOCATION_FAILED_ERROR);
				return false;
			}

			for (int i = 0; i < attrsCount; i++)
			{
				SourceTableAttribute *attr = &(table->attributes.array[i]);
				JSON_Object *jsAttr = json_array_get_object(jsAttrsArray, i);

				attr->attnum = json_object_get_number(jsAttr, "attnum");
				attr->atttypid = json_object_get_number(jsAttr, "atttypid");

				strlcpy(attr->attname,
						json_object_get_string(jsAttr, "attname"),
						sizeof(attr->attname));

				attr->attisprimary =
					json_object_get_boolean(jsAttr, "attisprimary");
			}
		}

		if (json_object_has_value(jsTable, "parts"))
		{
			JSON_Array *jsPartsArray = json_object_get_array(jsTable, "parts");
			int partsCount = json_array_get_count(jsPartsArray);

			table->partsArray.count = partsCount;
			table->partsArray.array =
				(SourceTableParts *) calloc(partsCount, sizeof(SourceTableParts));

			if (table->partsArray.array == NULL)
			{
				log_fatal(ALLOCATION_FAILED_ERROR);
				return false;
			}

			for (int i = 0; i < partsCount; i++)
			{
				SourceTableParts *part = &(table->partsArray.array[i]);
				JSON_Object *jsPart = json_array_get_object(jsPartsArray, i);

				part->partNumber = json_object_get_number(jsPart, "number");
				part->partCount = json_object_get_number(jsPart, "total");
				part->min = json_object_get_number(jsPart, "min");
				part->max = json_object_get_number(jsPart, "max");
				part->count = json_object_get_number(jsPart, "count");
			}
		}
		else
		{
			table->partsArray.count = 0;
			table->partsArray.array = NULL;
		}
	}

	/* now attach the final hash table head to the specs */
	copySpecs->catalog.sourceTableHashByOid = sourceTableHashByOid;
	copySpecs->catalog.sourceTableHashByQName = sourceTableHashByQName;

	/* index section */
	JSON_Array *jsIndexArray = json_object_get_array(jsObj, "indexes");
	int indexCount = json_array_get_count(jsIndexArray);

	log_debug("copydb_parse_schema_json_file: parsing %d indexes", indexCount);

	copySpecs->catalog.sourceIndexArray.count = indexCount;
	copySpecs->catalog.sourceIndexArray.array =
		(SourceIndex *) calloc(indexCount, sizeof(SourceIndex));

	if (copySpecs->catalog.sourceIndexArray.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* also build the index hash-table */
	SourceIndex *sourceIndexHashByOid = copySpecs->catalog.sourceIndexHashByOid;

	for (int i = 0; i < indexCount; i++)
	{
		SourceIndex *index = &(copySpecs->catalog.sourceIndexArray.array[i]);
		JSON_Object *jsIndex = json_array_get_object(jsIndexArray, i);

		index->indexOid = json_object_get_number(jsIndex, "oid");

		/* add the current index to the index Hash-by-OID */
		HASH_ADD(hh, sourceIndexHashByOid, indexOid, sizeof(uint32_t), index);

		char *schema = (char *) json_object_get_string(jsIndex, "schema");
		char *name = (char *) json_object_get_string(jsIndex, "name");
		char *qname = (char *) json_object_get_string(jsIndex, "qname");
		char *cols = (char *) json_object_get_string(jsIndex, "columns");
		char *def = (char *) json_object_get_string(jsIndex, "sql");
		char *listName =
			(char *) json_object_get_string(jsIndex, "restore-list-name");

		strlcpy(index->indexNamespace, schema, sizeof(index->indexNamespace));
		strlcpy(index->indexRelname, name, sizeof(index->indexRelname));
		strlcpy(index->indexQname, qname, sizeof(index->indexQname));

		int lenCols = strlen(cols) + 1;

		index->indexColumns = (char *) calloc(lenCols, sizeof(char));

		if (index->indexColumns == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(index->indexColumns, cols, lenCols);

		int lenDef = strlen(def) + 1;

		index->indexDef = (char *) calloc(lenDef, sizeof(char));

		if (index->indexDef == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(index->indexDef, def, lenDef);

		strlcpy(index->indexRestoreListName,
				listName,
				sizeof(index->indexRestoreListName));

		index->isPrimary = json_object_get_boolean(jsIndex, "isPrimary");
		index->isUnique = json_object_get_boolean(jsIndex, "isUnique");

		index->tableOid = json_object_dotget_number(jsIndex, "table.oid");

		schema = (char *) json_object_dotget_string(jsIndex, "table.schema");
		name = (char *) json_object_dotget_string(jsIndex, "table.name");
		qname = (char *) json_object_dotget_string(jsIndex, "table.qname");

		strlcpy(index->tableNamespace, schema, sizeof(index->tableNamespace));
		strlcpy(index->tableRelname, name, sizeof(index->tableRelname));
		strlcpy(index->tableQname, name, sizeof(index->tableQname));

		if (json_object_has_value(jsIndex, "constraint"))
		{
			index->constraintOid =
				json_object_dotget_number(jsIndex, "constraint.oid");

			name = (char *) json_object_dotget_string(jsIndex, "constraint.name");
			def = (char *) json_object_dotget_string(jsIndex, "constraint.sql");

			listName =
				(char *) json_object_get_string(jsIndex,
												"constraint.restore-list-name");

			strlcpy(index->constraintName, name, sizeof(index->constraintName));

			int len = strlen(def) + 1;
			index->constraintDef = (char *) calloc(len, sizeof(char));

			if (index->constraintDef == NULL)
			{
				log_fatal(ALLOCATION_FAILED_ERROR);
				return false;
			}

			strlcpy(index->constraintDef, def, len);

			if (listName != NULL)
			{
				strlcpy(index->constraintRestoreListName,
						listName,
						sizeof(index->constraintRestoreListName));
			}
		}
		else
		{
			index->constraintOid = 0;
		}
	}

	/* now attach the final hash table head to the specs */
	copySpecs->catalog.sourceIndexHashByOid = sourceIndexHashByOid;

	return true;
}


/*
 * copydb_update_progress updates the progress counters with information found
 * on-disk in the work directory (lock and done files, etc).
 */
bool
copydb_update_progress(CopyDataSpec *copySpecs, CopyProgress *progress)
{
	SourceTableArray *tableArray = &(copySpecs->catalog.sourceTableArray);
	SourceIndexArray *indexArray = &(copySpecs->catalog.sourceIndexArray);

	progress->tableCount = tableArray->count;
	progress->indexCount = indexArray->count;

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

	SourceTableArray *tableInProgress = &(progress->tableInProgress);
	CopyTableSummaryArray *summaryArray = &(progress->tableSummaryArray);

	for (int i = 0; i < progress->tableCount; i++)
	{
		SourceTable *source = &(tableArray->array[i]);

		int partCount = source->partsArray.count;

		bool done = false;

		if (partCount <= 1)
		{
			CopyTableDataSpec tableSpecs = { 0 };

			if (!copydb_init_table_specs(&tableSpecs, copySpecs, source, 0))
			{
				/* errors have already been logged */
				return false;
			}

			if (file_exists(tableSpecs.tablePaths.doneFile))
			{
				done = true;
			}
			else if (file_exists(tableSpecs.tablePaths.lockFile))
			{
				CopyTableSummary summary = { .table = source };

				if (!read_table_summary(&summary,
										tableSpecs.tablePaths.lockFile))
				{
					/* errors have already been logged */
					return false;
				}

				/*
				 * Copy the SourceTable struct in-place to the tableInProgress
				 * array.
				 */
				tableInProgress->array[progress->tableInProgress.count++] =
					*source;

				summaryArray->array[progress->tableSummaryArray.count++] =
					summary;
			}
		}
		else
		{
			bool allPartsAreDone = true;

			for (int partIndex = 0; partIndex < partCount; partIndex++)
			{
				CopyTableDataSpec tableSpecs = { 0 };

				if (!copydb_init_table_specs(&tableSpecs,
											 copySpecs,
											 source,
											 partIndex))
				{
					/* errors have already been logged */
					return false;
				}

				if (!file_exists(tableSpecs.tablePaths.doneFile))
				{
					allPartsAreDone = false;
				}

				if (file_exists(tableSpecs.tablePaths.lockFile))
				{
					CopyTableSummary summary = { .table = source };

					if (!read_table_summary(&summary,
											tableSpecs.tablePaths.lockFile))
					{
						/* errors have already been logged */
						return false;
					}

					/*
					 * Copy the SourceTable struct in-place to the
					 * tableInProgress array.
					 */
					tableInProgress->array[progress->tableInProgress.count++] =
						*source;

					summaryArray->array[progress->tableSummaryArray.count++] =
						summary;
				}
			}

			done = allPartsAreDone;
		}

		if (done)
		{
			++progress->tableDoneCount;
		}
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

	SourceIndexArray *indexInProgress = &(progress->indexInProgress);
	CopyIndexSummaryArray *summaryArrayIdx = &(progress->indexSummaryArray);

	IndexFilePathsArray indexPathsArray = { 0, NULL };

	/* build the index file paths we need for the upcoming operations */
	if (!copydb_init_indexes_paths(&(copySpecs->cfPaths),
								   indexArray,
								   &indexPathsArray))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < progress->indexCount; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);
		IndexFilePaths *indexPaths = &(indexPathsArray.array[i]);

		if (file_exists(indexPaths->doneFile))
		{
			++progress->indexDoneCount;
		}
		else if (file_exists(indexPaths->lockFile))
		{
			CopyIndexSummary summary = { .index = index };

			if (!read_index_summary(&summary, indexPaths->lockFile))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * Copy the SourceIndex struct in-place to the indexInProgress
			 * array.
			 */
			indexInProgress->array[progress->indexInProgress.count++] =
				*index;

			summaryArrayIdx->array[progress->indexSummaryArray.count++] =
				summary;
		}
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

	if (!copydb_table_array_as_json(tableArray, jsTableObj, "in-progress"))
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

	if (!copydb_index_array_as_json(indexArray, jsIndexObj, "in-progress"))
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
