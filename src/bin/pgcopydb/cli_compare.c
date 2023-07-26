/*
 * src/bin/pgcopydb/cli_compare.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "commandline.h"
#include "copydb.h"
#include "env_utils.h"
#include "ld_stream.h"
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "progress.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"

static int cli_compare_getopts(int argc, char **argv);
static void cli_compare_schema(int argc, char **argv);
static void cli_compare_data(int argc, char **argv);

static bool cli_compare_fetch_schemas(CopyDataSpec *copySpecs,
									  CopyDataSpec *sourceSpecs,
									  CopyDataSpec *targetSpecs);

static CommandLine compare_schema_command =
	make_command(
		"schema",
		"Compare source and target schema",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n",
		cli_compare_getopts,
		cli_compare_schema);

static CommandLine compare_data_command =
	make_command(
		"data",
		"Compare source and target data",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n",
		cli_compare_getopts,
		cli_compare_data);

static CommandLine *compare_subcommands[] = {
	&compare_schema_command,
	&compare_data_command,
	NULL
};

CommandLine compare_commands =
	make_command_set("compare",
					 "Compare source and target databases",
					 NULL, NULL, NULL, compare_subcommands);

CopyDBOptions compareOptions = { 0 };


static int
cli_compare_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "notice", no_argument, NULL, 'v' },
		{ "debug", no_argument, NULL, 'd' },
		{ "trace", no_argument, NULL, 'z' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	/* read values from the environment */
	if (!cli_copydb_getenv(&options))
	{
		log_fatal("Failed to read default values from the environment");
		exit(EXIT_CODE_BAD_ARGS);
	}

	while ((c = getopt_long(argc, argv, "S:T:D:Vvdzqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'S':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --source connection string, "
							  "see above for details.");
					exit(EXIT_CODE_BAD_ARGS);
				}
				options.connStrings.source_pguri = pg_strdup(optarg);
				log_trace("--source %s", options.connStrings.source_pguri);
				break;
			}

			case 'T':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --target connection string, "
							  "see above for details.");
					++errors;
				}
				options.connStrings.target_pguri = pg_strdup(optarg);
				log_trace("--target %s", options.connStrings.target_pguri);
				break;
			}

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 'V':
			{
				/* keeper_cli_print_version prints version and exits. */
				cli_print_version(argc, argv);
				break;
			}

			case 'v':
			{
				++verboseCount;
				switch (verboseCount)
				{
					case 1:
					{
						log_set_level(LOG_NOTICE);
						break;
					}

					case 2:
					{
						log_set_level(LOG_SQL);
						break;
					}

					case 3:
					{
						log_set_level(LOG_DEBUG);
						break;
					}

					default:
					{
						log_set_level(LOG_TRACE);
						break;
					}
				}
				break;
			}

			case 'd':
			{
				verboseCount = 3;
				log_set_level(LOG_DEBUG);
				break;
			}

			case 'z':
			{
				verboseCount = 4;
				log_set_level(LOG_TRACE);
				break;
			}

			case 'q':
			{
				log_set_level(LOG_ERROR);
				break;
			}

			case 'h':
			{
				commandline_help(stderr);
				exit(EXIT_CODE_QUIT);
				break;
			}
		}
	}

	if (options.connStrings.source_pguri == NULL ||
		options.connStrings.target_pguri == NULL)
	{
		log_fatal("Option --source and --target are mandatory");
		++errors;
	}

	/* prepare safe versions of the connection strings (without password) */
	if (!cli_prepare_pguris(&(options.connStrings)))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	compareOptions = options;

	return optind;
}


/*
 * cli_compare_schema compares the schema on the source and target databases.
 */
static void
cli_compare_schema(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	char *dir =
		IS_EMPTY_STRING_BUFFER(compareOptions.dir)
		? NULL
		: compareOptions.dir;

	bool createWorkDir = true;
	bool service = true;
	char *serviceName = "snapshot";

	/* pretend that --resume --not-consistent have been used */
	compareOptions.resume = true;
	compareOptions.notConsistent = true;

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 service,
							 serviceName,
							 compareOptions.restart,
							 compareOptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &compareOptions, DATA_SECTION_ALL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Now prepare two specifications with only the source uri.
	 *
	 * We don't free() any memory here as the two CopyDataSpecs copies are
	 * going to share pointers to memory allocated in the main copySpecs
	 * instance.
	 */
	CopyDataSpec sourceSpecs = { 0 };
	CopyDataSpec targetSpecs = { 0 };

	if (!cli_compare_fetch_schemas(&copySpecs, &sourceSpecs, &targetSpecs))
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
}


/*
 * cli_compare_data compares the data on the source and target databases.
 */
static void
cli_compare_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	char *dir =
		IS_EMPTY_STRING_BUFFER(compareOptions.dir)
		? NULL
		: compareOptions.dir;

	bool createWorkDir = true;
	bool service = true;
	char *serviceName = "snapshot";

	/* pretend that --resume --not-consistent have been used */
	compareOptions.resume = true;
	compareOptions.notConsistent = true;

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 service,
							 serviceName,
							 compareOptions.restart,
							 compareOptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &compareOptions, DATA_SECTION_ALL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Now prepare two specifications with only the source uri.
	 *
	 * We don't free() any memory here as the two CopyDataSpecs copies are
	 * going to share pointers to memory allocated in the main copySpecs
	 * instance.
	 */
	CopyDataSpec sourceSpecs = { 0 };
	CopyDataSpec targetSpecs = { 0 };

	if (!cli_compare_fetch_schemas(&copySpecs, &sourceSpecs, &targetSpecs))
	{
		log_fatal("Failed to fetch source and target schemas, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Comparing data for %d tables",
			 sourceSpecs.catalog.sourceTableArray.count);

	PGSQL src = { 0 };
	char *srcURI = copySpecs.connStrings.source_pguri;

	if (!pgsql_init(&src, srcURI, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_begin(&src))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	PGSQL dst = { 0 };
	char *dstURI = copySpecs.connStrings.target_pguri;

	if (!pgsql_init(&dst, dstURI, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&src);
		exit(EXIT_CODE_TARGET);
	}

	if (!pgsql_begin(&dst))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&src);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

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

		if (!schema_checksum_table(&src, source))
		{
			/* errors have already been logged */
			(void) pgsql_finish(&src);
			(void) pgsql_finish(&dst);
			exit(EXIT_CODE_SOURCE);
		}

		if (!schema_checksum_table(&dst, target))
		{
			/* errors have already been logged */
			(void) pgsql_finish(&src);
			(void) pgsql_finish(&dst);
			exit(EXIT_CODE_TARGET);
		}

		if (source->rowcount != target->rowcount)
		{
			++diffCount;
			log_error("Table %s has %lld rows on source, %lld rows on target",
					  qname,
					  (long long) source->rowcount,
					  (long long) target->rowcount);
		}

		if (source->checksum != target->checksum)
		{
			++diffCount;
			log_error("Table %s has checksum %lld on source, %lld on target",
					  qname,
					  (long long) source->checksum,
					  (long long) target->checksum);
		}

		log_notice("%s: %lld rows, checksum %lld",
				   qname,
				   (long long) source->rowcount,
				   (long long) source->checksum);
	}

	if (!pgsql_commit(&src))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&dst);
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (diffCount == 0)
	{
		log_info("pgcopydb data inspection is successful");
	}

	fformat(stdout, "%30s | %20s | %20s \n",
			"Table Name", "Row Count", "Checksum");

	fformat(stdout, "%30s-+-%20s-+-%20s \n",
			"------------------------------",
			"--------------------",
			"--------------------");

	for (int i = 0; i < sourceSpecs.catalog.sourceTableArray.count; i++)
	{
		SourceTable *source = &(sourceSpecs.catalog.sourceTableArray.array[i]);

		fformat(stdout, "%30s | %20lld | %20llx \n",
				source->qname,
				source->rowcount,
				source->checksum);
	}

	fformat(stdout, "\n");
}


/*
 * cli_compare_fetch_schemas fetches the source and target schemas.
 */
static bool
cli_compare_fetch_schemas(CopyDataSpec *copySpecs,
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
