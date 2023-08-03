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
	/*
	 * Retrieve catalogs from the source database, the target is supposed to
	 * have the same objects.
	 */
	ConnStrings *dsn = &(copySpecs->connStrings);

	log_info("SOURCE: Connecting to \"%s\"", dsn->safeSourcePGURI.pguri);

	if (!copydb_fetch_schema_and_prepare_specs(copySpecs))
	{
		log_fatal("Failed to retrieve source database schema, "
				  "see above for details.");
		return false;
	}

	PGSQL src = { 0 };
	char *srcURI = copySpecs->connStrings.source_pguri;

	if (!pgsql_init(&src, srcURI, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&src))
	{
		/* errors have already been logged */
		return false;
	}

	PGSQL dst = { 0 };
	char *dstURI = copySpecs->connStrings.target_pguri;

	if (!pgsql_init(&dst, dstURI, PGSQL_CONN_TARGET))
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

	uint64_t diffCount = 0;

	SourceTableArray *tableArray = &(copySpecs->catalog.sourceTableArray);

	for (int i = 0; i < tableArray->count; i++)
	{
		SourceTable *source = &(tableArray->array[i]);

		int c = 0;

		if (!compare_table(&src, &dst, source, &c))
		{
			log_error("Failed to compute rowcount and checksum for %s, "
					  "see above for details",
					  source->qname);

			(void) pgsql_finish(&src);
			(void) pgsql_finish(&dst);

			return false;
		}

		diffCount += c;
	}

	if (!pgsql_commit(&src))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&dst);
		return false;
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	/* write the data to file, include the rowcount and checksums */
	if (!copydb_prepare_schema_json_file(copySpecs))
	{
		log_fatal("Failed to store the source database schema to file \"%s\", "
				  "see above for details",
				  copySpecs->cfPaths.schemafile);
		return false;
	}

	if (diffCount == 0)
	{
		log_info("pgcopydb data inspection is successful");
	}

	return true;
}


/*
 * compare_table computes the rowcount and checksum of a table contents on the
 * source and on the target database instances and compare them.
 */
bool
compare_table(PGSQL *src, PGSQL *dst, SourceTable *source, int *diffCount)
{
	TableChecksum *srcChk = &(source->sourceChecksum);
	TableChecksum *dstChk = &(source->targetChecksum);

	*diffCount = 0;

	/*
	 * First, send both the queries to the source and target databases,
	 * async.
	 */
	if (!schema_send_table_checksum(src, source))
	{
		/* errors have already been logged */
		return false;
	}

	if (!schema_send_table_checksum(dst, source))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Second, fetch the results from both the connections.
	 */
	bool srcDone = false;
	bool dstDone = false;

	do {
		if (!srcDone)
		{
			if (!schema_fetch_table_checksum(src, srcChk, &srcDone))
			{
				/* errors have already been logged */
				return false;
			}
		}

		if (!dstDone)
		{
			if (!schema_fetch_table_checksum(dst, dstChk, &dstDone))
			{
				/* errors have already been logged */
				return false;
			}
		}

		if (!srcDone || !dstDone)
		{
			pg_usleep(10 * 1000); /* 10 ms */
		}
	} while (!srcDone || !dstDone);

	if (srcChk->rowcount != dstChk->rowcount)
	{
		++(*diffCount);
		log_error("Table %s has %lld rows on source, %lld rows on target",
				  source->qname,
				  (long long) srcChk->rowcount,
				  (long long) dstChk->rowcount);
	}

	/* if the rowcount is different, don't log the checksum mismatch */
	if (diffCount == 0 && !streq(srcChk->checksum, dstChk->checksum))
	{
		++(*diffCount);
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
