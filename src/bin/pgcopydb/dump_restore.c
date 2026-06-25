/*
 * src/bin/pgcopydb/dump_restore.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "postgres_fe.h"
#include "pqexpbuffer.h"
#include "dumputils.h"

#include "catalog.h"
#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


static bool copydb_append_table_hook(void *context, SourceTable *table);

static bool copydb_copy_database_properties_hook(void *ctx,
												 SourceProperty *property);

static bool copydb_write_restore_list_hook(void *ctx,
										   ArchiveContentItem *item);

static bool copydb_collect_matview_toc_order(CopyDataSpec *specs);


/*
 * copydb_objectid_has_been_processed_already returns true when the given
 * target object OID is found in our SQLite summary catalogs. This only applies
 * to indexes or constraints, as TABLE DATA is not part of either the
 * --pre-data parts of the schema nor the --post-data parts of the schema.
 */
bool
copydb_objectid_has_been_processed_already(CopyDataSpec *specs,
										   ArchiveContentItem *item)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	uint32_t oid = item->objectOid;

	switch (item->desc)
	{
		case ARCHIVE_TAG_INDEX:
		{
			SourceIndex index = { .indexOid = oid };
			CopyIndexSpec indexSpecs = { .sourceIndex = &index };

			if (!summary_lookup_index(sourceDB, &indexSpecs))
			{
				/* errors have aleady been logged */
				return false;
			}

			return indexSpecs.summary.doneTime > 0;
		}

		case ARCHIVE_TAG_CONSTRAINT:
		{
			SourceIndex index = { .constraintOid = oid };
			CopyIndexSpec indexSpecs = { .sourceIndex = &index };

			if (!summary_lookup_constraint(sourceDB, &indexSpecs))
			{
				/* errors have aleady been logged */
				return false;
			}

			return indexSpecs.summary.doneTime > 0;
		}

		/* we don't have internal pgcopydb support for other objects */
		default:
		{
			return false;
		}
	}

	/* keep compiler happy */
	return false;
}


/*
 * copydb_dump_source_schema uses pg_dump -Fc --schema --schema-only
 * to dump the source database schema to file.
 */
bool
copydb_dump_source_schema(CopyDataSpec *specs,
						  const char *snapshot)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (specs->runState.schemaDumpIsDone)
	{
		log_info("Skipping pg_dump for pre-data/post-data section, "
				 "done on a previous run");
		return true;
	}

	if (!summary_start_timing(sourceDB, TIMING_SECTION_DUMP_SCHEMA))
	{
		/* errors have already been logged */
		return false;
	}

	if (file_exists(specs->dumpPaths.dumpFilename))
	{
		log_info("Skipping source schema dump, "
				 "as \"%s\" already exists",
				 specs->dumpPaths.dumpFilename);
	}
	else if (!pg_dump_db(&(specs->pgPaths),
						 &(specs->connStrings),
						 snapshot,
						 &(specs->filters),
						 &(specs->catalogs.filter),
						 specs->dumpPaths.dumpFilename))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_DUMP_SCHEMA))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_target_prepare_schema restores the schema.dump file into the target
 * database.
 */
bool
copydb_target_prepare_schema(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!file_exists(specs->dumpPaths.dumpFilename))
	{
		log_fatal("File \"%s\" does not exists", specs->dumpPaths.dumpFilename);
		return false;
	}

	if (specs->runState.schemaPreDataHasBeenRestored)
	{
		log_info("Skipping pg_restore for pre-data section, "
				 "done on a previous run");
		return true;
	}

	if (!summary_start_timing(sourceDB, TIMING_SECTION_PREPARE_SCHEMA))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * First restore the database properties (ALTER DATABASE SET).
	 */
	if (!copydb_copy_database_properties(specs))
	{
		log_error("Failed to restore the database properties, "
				  "see above for details");
		return false;
	}

	/*
	 * When --requirements is given, use a two-pass approach so that extension
	 * namespaces exist before the extensions themselves are created, and
	 * extensions exist before pg_restore creates objects that depend on their
	 * types (e.g. PostGIS geometry columns):
	 *
	 *   Pass 1: restore schemas only → namespaces created with correct
	 *           ownership and grants.
	 *   Then:   create pinned extensions (schemas already exist).
	 *   Pass 2: restore everything except schemas and extensions → tables,
	 *           functions, etc. can reference extension types.
	 *
	 * Without --requirements, a single pg_restore pass suffices: extensions
	 * are either handled by pg_restore (skipExtensions=false) or skipped
	 * entirely (skipExtensions=true).
	 */
	if (specs->extRequirements != NULL)
	{
		if (!copydb_write_schemas_restore_list(specs))
		{
			log_error("Failed to prepare the schemas-only pg_restore list, "
					  "see above for details");
			return false;
		}

		specs->restoreOptions.section = PG_RESTORE_SECTION_PRE_DATA;
		if (!pg_restore_db(&(specs->pgPaths),
						   &(specs->connStrings),
						   &(specs->filters),
						   specs->dumpPaths.dumpFilename,
						   specs->dumpPaths.schemaListFilename,
						   specs->restoreOptions))
		{
			log_error("Failed to restore schemas (first pass), "
					  "see above for details");
			return false;
		}
	}

	if (!copydb_create_pinned_extensions(specs))
	{
		log_error("Failed to create extensions with version pinning, "
				  "see above for details");
		return false;
	}

	/*
	 * Now prepare the pg_restore --use-list file.  When --requirements is
	 * used, SCHEMA entries are skipped here because they were already restored
	 * in the first pass above.
	 */
	if (!copydb_write_restore_list(specs, PG_DUMP_SECTION_PRE_DATA))
	{
		log_error("Failed to prepare the pg_restore --use-list catalogs, "
				  "see above for details");
		return false;
	}

	/*
	 * Determine the target PostgreSQL version.  On PG17+,
	 * RestrictSearchPath() inside RefreshMatViewByOid() forces search_path
	 * to 'pg_catalog, pg_temp' for every REFRESH (and CREATE MATERIALIZED
	 * VIEW ... WITH DATA), making the original bug (#484/#501) impossible.
	 * pg_restore then handles REFRESH directly, and pgcopydb does not need
	 * to own that step.  The version is inherited by forked worker processes.
	 */
	if (specs->targetPgVersionNum == 0)
	{
		PGSQL dst = { 0 };

		if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_server_version(&dst))
		{
			/* errors have already been logged */
			(void) pgsql_finish(&dst);
			return false;
		}

		specs->targetPgVersionNum = dst.pgversion_num;
		(void) pgsql_finish(&dst);

		log_info("Target PostgreSQL version: %d", specs->targetPgVersionNum);
	}

	/*
	 * Scan the full archive TOC (already written to preListOutFilename) to
	 * assign toc_seq to each REFRESH MATERIALIZED VIEW entry.  This must be
	 * done before the data phase starts so the INDEX supervisor can feed
	 * vacuum workers in the correct pg_dump dependency order.
	 * On PG17+ targets this is skipped: pg_restore handles REFRESH directly.
	 */
	if (!copydb_collect_matview_toc_order(specs))
	{
		log_error("Failed to collect materialized view refresh order from "
				  "pg_dump TOC");
		return false;
	}

	/*
	 * pg_restore --clean --if-exists gets easily confused when dealing with
	 * partial schema information, such as when using only section=pre-data, or
	 * when using the --use-list option as we do here.
	 *
	 * As a result, we implement --drop-if-exists our own way first, with a big
	 * DROP IF EXISTS ... CASCADE statement that includes all our target tables.
	 */
	if (specs->restoreOptions.dropIfExists)
	{
		if (!copydb_target_drop_tables(specs))
		{
			/* errors have already been logged */
			return false;
		}
	}

	specs->restoreOptions.section = PG_RESTORE_SECTION_PRE_DATA;
	if (!pg_restore_db(&(specs->pgPaths),
					   &(specs->connStrings),
					   &(specs->filters),
					   specs->dumpPaths.dumpFilename,
					   specs->dumpPaths.preListFilename,
					   specs->restoreOptions))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Some extensions such as timescaledb need a pre data step.
	 */
	if (!copydb_prepare_extensions_restore(specs))
	{
		log_error("Failed to call pg_restore preparation steps for extensions, "
				  "see above for details");
		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_PREPARE_SCHEMA))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


typedef struct CopyPropertiesContext
{
	CopyDataSpec *specs;
	PGSQL *dst;
} CopyPropertiesContext;


/*
 * copydb_copy_database_properties uses ALTER DATABASE SET commands to set the
 * properties on the target database to look the same way as on the source
 * database.
 */
bool
copydb_copy_database_properties(CopyDataSpec *specs)
{
	const char *s_dbname = specs->connStrings.safeSourcePGURI.uriParams.dbname;

	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	CopyPropertiesContext context = {
		.specs = specs,
		.dst = &dst
	};

	if (!catalog_iter_s_database_guc(sourceDB,
									 s_dbname,
									 &context,
									 &copydb_copy_database_properties_hook))
	{
		/* errors have already been logged */
		(void) pgsql_rollback(&dst);
		return false;
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_copy_database_properties_hook is an iterator callback function.
 */
static bool
copydb_copy_database_properties_hook(void *ctx, SourceProperty *property)
{
	CopyPropertiesContext *context = (CopyPropertiesContext *) ctx;

	CopyDataSpec *specs = context->specs;

	PGSQL *dst = context->dst;
	PGconn *conn = dst->connection;

	char *t_dbname = specs->connStrings.safeTargetPGURI.uriParams.dbname;
	char *t_escaped_dbname = pgsql_escape_identifier(dst, t_dbname);

	if (t_escaped_dbname == NULL)
	{
		/* errors are already logged */
		return false;
	}

	/*
	 * ALTER ROLE rolname IN DATABASE datname SET ...
	 */
	if (property->roleInDatabase)
	{
		DatabaseCatalog *targetDB = &(specs->catalogs.target);

		SourceRole *role = (SourceRole *) calloc(1, sizeof(SourceRole));

		if (role == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		if (!catalog_lookup_s_role_by_name(targetDB, property->rolname, role))
		{
			/* errors have already been logged */
			return false;
		}

		if (role->oid > 0)
		{
			PQExpBuffer command = createPQExpBuffer();

			makeAlterConfigCommand(conn, property->setconfig,
								   "ROLE", property->rolname,
								   "DATABASE",
								   t_escaped_dbname,
								   command);

			/* chomp the \n */
			if (command->data[command->len - 1] == '\n')
			{
				command->data[command->len - 1] = '\0';
			}

			log_info("%s", command->data);

			if (!pgsql_execute(dst, command->data))
			{
				/* errors have already been logged */
				return false;
			}

			destroyPQExpBuffer(command);
		}
		else
		{
			log_warn("Skipping database properties for role %s which "
					 "does not exists on the target database",
					 property->rolname);
		}
	}

	/*
	 * ALTER DATABASE datname SET ...
	 */
	else
	{
		bool exists = false;

		if (!pgsql_configuration_exists(dst, property->setconfig, &exists))
		{
			/* errors have already been logged */
			return false;
		}

		if (!exists)
		{
			log_warn("Skipping database property %s which "
					 "does not exists on the target database",
					 property->setconfig);
			return true;
		}

		PQExpBuffer command = createPQExpBuffer();

		makeAlterConfigCommand(conn, property->setconfig,
							   "DATABASE", t_escaped_dbname,
							   NULL, NULL,
							   command);

		if (command->data[command->len - 1] == '\n')
		{
			command->data[command->len - 1] = '\0';
		}

		log_info("%s", command->data);

		if (!pgsql_execute(dst, command->data))
		{
			/* errors have already been logged */
			return false;
		}

		destroyPQExpBuffer(command);
	}

	return true;
}


typedef struct DropTableContext
{
	PQExpBuffer query;
	uint64_t tableIndex;
} DropTableContext;

/*
 * copydb_target_drop_tables prepares and executes a SQL query that prepares
 * our target database by means of a DROP IF EXISTS ... CASCADE statement that
 * includes all our target tables.
 */
bool
copydb_target_drop_tables(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	CatalogStats stats = { 0 };

	if (!catalog_stats(sourceDB, &stats))
	{
		/* errors have already been logged */
		return false;
	}

	if (stats.count.tables == 0)
	{
		log_info("No tables to migrate, skipping drop tables "
				 "on the target database");
		return true;
	}

	log_info("Drop tables on the target database, per --drop-if-exists");

	PQExpBuffer query = createPQExpBuffer();

	DropTableContext context = { .query = query, .tableIndex = 0 };

	appendPQExpBufferStr(query, "DROP TABLE IF EXISTS");

	if (!catalog_iter_s_table(sourceDB, &context, &copydb_append_table_hook))
	{
		log_error("Failed to create DROP IF EXISTS query: "
				  "see above for details");
		destroyPQExpBuffer(query);
		return false;
	}

	appendPQExpBufferStr(query, " CASCADE");

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(query))
	{
		log_error("Failed to create DROP IF EXISTS query: out of memory");
		destroyPQExpBuffer(query);
		return false;
	}

	log_notice("%s", query->data);

	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(query);
		return false;
	}

	if (!pgsql_execute(&dst, query->data))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(query);
		return false;
	}

	destroyPQExpBuffer(query);

	return true;
}


/*
 * copydb_append_table_hook is an iterator callback function.
 */
static bool
copydb_append_table_hook(void *ctx, SourceTable *table)
{
	if (table == NULL)
	{
		log_error("BUG: copydb_append_table_hook called with a NULL table");
		return false;
	}

	DropTableContext *context = (DropTableContext *) ctx;

	appendPQExpBuffer(context->query, "%s %s.%s",
					  context->tableIndex++ == 0 ? " " : ",",
					  table->nspname,
					  table->relname);

	return true;
}


/*
 * copydb_target_finalize_schema finalizes the schema after all the data has
 * been copied over, and after indexes and their constraints have been created
 * too.
 */
bool
copydb_target_finalize_schema(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!file_exists(specs->dumpPaths.dumpFilename))
	{
		log_fatal("File \"%s\" does not exists", specs->dumpPaths.dumpFilename);
		return false;
	}

	if (specs->runState.schemaPostDataHasBeenRestored)
	{
		log_info("Skipping pg_restore for --section=post-data, "
				 "done on a previous run");
		return true;
	}

	if (!summary_start_timing(sourceDB, TIMING_SECTION_FINALIZE_SCHEMA))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_write_restore_list(specs, PG_DUMP_SECTION_POST_DATA))
	{
		log_error("Failed to prepare the pg_restore --use-list catalogs, "
				  "see above for details");
		return false;
	}

	specs->restoreOptions.section = PG_RESTORE_SECTION_POST_DATA;
	if (!pg_restore_db(&(specs->pgPaths),
					   &(specs->connStrings),
					   &(specs->filters),
					   specs->dumpPaths.dumpFilename,
					   specs->dumpPaths.postListFilename,
					   specs->restoreOptions))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Some extensions such as timescaledb need a post restore step.
	 */
	if (!copydb_finalize_extensions_restore(specs))
	{
		log_error("Failed to call pg_restore preparation steps for extensions, "
				  "see above for details");
		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_FINALIZE_SCHEMA))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


typedef struct RestoreListContext
{
	CopyDataSpec *specs;
	FILE *outStream;
	bool schemasOnly;            /* write only SCHEMA entries (first pass) */
	bool assignTocSeq;           /* true when scanning TOC to assign toc_seq */
	int matviewTocSeq;           /* counter for REFRESH entries seen so far */
} RestoreListContext;

/*
 * copydb_write_restore_list fetches the pg_restore --list output, parses it,
 * and then writes it again to file and applies the filtering to the archive
 * catalog that is meant to be used as pg_restore --use-list argument.
 */
bool
copydb_write_restore_list(CopyDataSpec *specs, PostgresDumpSection section)
{
	char *dumpFilename = NULL;
	char *listFilename = NULL;
	char *listOutFilename = NULL;

	switch (section)
	{
		case PG_DUMP_SECTION_PRE_DATA:
		{
			dumpFilename = specs->dumpPaths.dumpFilename;
			listFilename = specs->dumpPaths.preListFilename;
			listOutFilename = specs->dumpPaths.preListOutFilename;
			break;
		}

		case PG_DUMP_SECTION_POST_DATA:
		{
			dumpFilename = specs->dumpPaths.dumpFilename;
			listFilename = specs->dumpPaths.postListFilename;
			listOutFilename = specs->dumpPaths.postListOutFilename;
			break;
		}

		default:
		{
			log_error("BUG: copydb_write_restore_list: "
					  "unknown pg_dump section %d",
					  section);
			return false;
		}
	}

	/*
	 * The schema.dump archive file contains all the objects to create in the
	 * target database. We want to filter out the schemas and tables excluded
	 * from the filtering setup.
	 *
	 * This archive file also contains all the objects to create once the
	 * table data has been copied over. It contains in particular the
	 * constraints and indexes that we have already built concurrently in the
	 * previous step, so we want to filter those out.
	 *
	 * Here's how to filter out some objects with pg_restore:
	 *
	 *   1. pg_restore -f post.list --list schema.dump
	 *   2. edit post.list to comment out lines and save as filtered.list
	 *   3. pg_restore --section post-data --use-list filtered.list schema.dump
	 */
	if (!pg_restore_list(&(specs->pgPaths),
						 dumpFilename,
						 listOutFilename))
	{
		/* errors have already been logged */
		return false;
	}

	/* edit our post.list file now */
	FILE *out = fopen_with_umask(listFilename, "ab", FOPEN_FLAGS_A, 0644);

	if (out == NULL)
	{
		/* errors have already been logged */
		return false;
	}

	RestoreListContext context = {
		.specs = specs,
		.outStream = out,
	};

	if (!archive_iter_toc(listOutFilename,
						  &context,
						  copydb_write_restore_list_hook))
	{
		log_error("Failed to prepare the pg_restore list file, "
				  "see above for details");
		return false;
	}

	if (fclose(out) == EOF)
	{
		log_error("Failed to write file \"%s\"", listFilename);
		return false;
	}

	return true;
}


/*
 * copydb_write_schemas_restore_list writes a pg_restore --use-list file that
 * contains only SCHEMA entries from the pre-data section.  This is used as the
 * first pass when --requirements is active: pg_restore runs with this list to
 * create all schemas (with correct ownership and grants), after which
 * copydb_create_pinned_extensions can safely create extensions into those
 * schemas without needing to pre-create them itself.
 */
bool
copydb_write_schemas_restore_list(CopyDataSpec *specs)
{
	char *dumpFilename = specs->dumpPaths.dumpFilename;
	char *listOutFilename = specs->dumpPaths.preListOutFilename;
	char *listFilename = specs->dumpPaths.schemaListFilename;

	if (!pg_restore_list(&(specs->pgPaths), dumpFilename, listOutFilename))
	{
		/* errors have already been logged */
		return false;
	}

	FILE *out = fopen_with_umask(listFilename, "ab", FOPEN_FLAGS_A, 0644);

	if (out == NULL)
	{
		/* errors have already been logged */
		return false;
	}

	RestoreListContext context = {
		.specs = specs,
		.outStream = out,
		.schemasOnly = true
	};

	if (!archive_iter_toc(listOutFilename,
						  &context,
						  copydb_write_restore_list_hook))
	{
		log_error("Failed to prepare the schemas-only pg_restore list file, "
				  "see above for details");
		(void) fclose(out);
		return false;
	}

	if (fclose(out) == EOF)
	{
		log_error("Failed to write file \"%s\"", listFilename);
		return false;
	}

	return true;
}


/*
 * copydb_write_restore_list_hook is an iterator callback function.
 */
static bool
copydb_write_restore_list_hook(void *ctx, ArchiveContentItem *item)
{
	RestoreListContext *context = (RestoreListContext *) ctx;
	CopyDataSpec *specs = context->specs;

	uint32_t oid = item->objectOid;
	uint32_t catOid = item->catalogOid;
	char *name = item->restoreListName;

	bool skip = false;

	/*
	 * Always skip DATABASE objets as pgcopydb does not create the target
	 * database.
	 */
	if (item->desc == ARCHIVE_TAG_DATABASE)
	{
		skip = true;
		log_debug("Skipping DATABASE \"%s\"", name);
	}

	/*
	 * In schemas-only mode (first pass when --requirements is used), skip all
	 * non-SCHEMA archive entries so that only CREATE SCHEMA statements reach
	 * pg_restore.  This guarantees extension namespaces exist before
	 * copydb_create_pinned_extensions runs.
	 */
	if (!skip && context->schemasOnly && catOid != PG_NAMESPACE_OID)
	{
		skip = true;
	}

	/*
	 * Skip COMMENT ON EXTENSION when either of the option
	 * --skip-extensions or --skip-ext-comment has been used.
	 */
	if ((specs->skipExtensions ||
		 specs->skipCommentOnExtension) &&
		item->isCompositeTag &&
		item->tagKind == ARCHIVE_TAG_KIND_COMMENT &&
		item->tagType == ARCHIVE_TAG_TYPE_EXTENSION)
	{
		skip = true;
		log_debug("Skipping COMMENT ON EXTENSION \"%s\"", name);
	}

	/*
	 * Skip COMMENT ON EXTENSION for extensions named in [exclude-extension].
	 * Bare EXTENSION TOC entries are handled via the filter table in
	 * copydb_objectid_is_filtered_out; COMMENT ON EXTENSION has objectOid=0
	 * so it needs an explicit name-based check here.
	 */
	if (!skip &&
		item->isCompositeTag &&
		item->tagKind == ARCHIVE_TAG_KIND_COMMENT &&
		item->tagType == ARCHIVE_TAG_TYPE_EXTENSION)
	{
		SourceFilterExtensionList *excl =
			&(specs->filters.excludeExtensionList);

		for (int i = 0; i < excl->count; i++)
		{
			if (streq(name, excl->array[i].extname))
			{
				skip = true;
				log_debug("Skipping COMMENT ON EXTENSION \"%s\" "
						  "(exclude-extension)", name);
				break;
			}
		}
	}

	/*
	 * Skip COMMENT ON EXTENSION for extensions not in [include-only-extension].
	 */
	if (!skip &&
		item->isCompositeTag &&
		item->tagKind == ARCHIVE_TAG_KIND_COMMENT &&
		item->tagType == ARCHIVE_TAG_TYPE_EXTENSION)
	{
		SourceFilterExtensionList *incl =
			&(specs->filters.includeOnlyExtensionList);

		if (incl->count > 0)
		{
			bool found = false;

			for (int i = 0; i < incl->count; i++)
			{
				if (streq(name, incl->array[i].extname))
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				skip = true;
				log_debug("Skipping COMMENT ON EXTENSION \"%s\" "
						  "(not in include-only-extension)", name);
			}
		}
	}

	if (!skip && catOid == PG_NAMESPACE_OID)
	{
		if (!context->schemasOnly && specs->extRequirements != NULL)
		{
			/*
			 * When --requirements is used, schemas are restored in a
			 * dedicated first pass via copydb_write_schemas_restore_list.
			 * Always skip them in the main second pass.
			 */
			skip = true;

			log_debug("Skipping SCHEMA (restored in first pass) dumpId %d: %s",
					  item->dumpId, name);
		}
		else
		{
			bool exists = false;

			if (!copydb_schema_already_exists(specs, oid, &exists))
			{
				log_error("Failed to check if restore name \"%s\" "
						  "already exists",
						  name);
				return false;
			}

			if (exists)
			{
				skip = true;

				log_debug("Skipping already existing dumpId %d: %s %u %s",
						  item->dumpId,
						  item->description,
						  item->objectOid,
						  item->restoreListName);
			}
		}
	}

	if (!skip && copydb_objectid_has_been_processed_already(specs, item))
	{
		skip = true;

		log_debug("Skipping already processed dumpId %d: %s %u %s",
				  item->dumpId,
				  item->description,
				  item->objectOid,
				  item->restoreListName);
	}

	/*
	 * For SEQUENCE catalog entries, we want to limit the scope of the hash
	 * table search to the OID, and bypass searching by restore name. We
	 * only use the restore name for the SEQUENCE OWNED BY statements.
	 *
	 * This also allows complex filtering of sequences that are owned by
	 * table a and used as a default value in table b, where table a has
	 * been filtered-out from pgcopydb scope of operations, but not table
	 * b.
	 *
	 * The kind-based matching in filter_kind_matches_archive_desc ensures
	 * that 'default' and 'sequence owned by' filter entries do not match
	 * SEQUENCE archive items, so no name override is needed here.
	 */

	/*
	 * There could be a case where the materalized view is included in the
	 * dump, but the refresh is filtered out using [exclude-table-data].
	 * In this case, we want to skip the refresh.
	 */
	if (!skip &&
		item->desc == ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW &&
		copydb_matview_refresh_is_filtered_out(specs, oid))
	{
		skip = true;

		log_debug("Skipping materialized view refresh dumpId %d: %s %u %s",
				  item->dumpId,
				  item->description,
				  item->objectOid,
				  item->restoreListName);
	}

	if (!skip && copydb_objectid_is_filtered_out(specs, item))
	{
		skip = true;

		log_debug("Skipping filtered-out dumpId %d: %s %u %u %s",
				  item->dumpId,
				  item->description,
				  item->catalogOid,
				  item->objectOid,
				  item->restoreListName);
	}

	/*
	 * On PG <= 16 targets, pgcopydb owns REFRESH MATERIALIZED VIEW: entries
	 * are stripped from the pg_restore list here and dispatched via vacuum
	 * workers instead, using fresh libpq connections that inherit the
	 * database-level search_path (avoiding the pg_restore empty-search_path
	 * bug, issues #484/#501).
	 *
	 * On PG17+ targets the bug cannot arise (RestrictSearchPath() in
	 * RefreshMatViewByOid() forces a safe search_path unconditionally), so
	 * REFRESH entries are left in the list for pg_restore to handle.
	 */
	if (!skip &&
		item->desc == ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW &&
		specs->targetPgVersionNum < 170000)
	{
		skip = true;

		if (context->assignTocSeq && oid != 0)
		{
			DatabaseCatalog *sourceDB = &(specs->catalogs.source);
			int seq = ++context->matviewTocSeq;

			if (!catalog_update_s_matview_toc_seq(sourceDB, oid, seq))
			{
				log_error("Failed to record toc_seq for matview oid %u", oid);
				return false;
			}

			log_debug("Scheduled REFRESH MATERIALIZED VIEW oid %u "
					  "(dumpId %d, toc_seq %d) via vacuum worker queue",
					  oid, item->dumpId, seq);
		}
		else
		{
			log_debug("Skipping REFRESH MATERIALIZED VIEW dumpId %d "
					  "(handled by vacuum workers)", item->dumpId);
		}
	}

	/* In TOC-scan mode (outStream == NULL) we only assign toc_seq, no output. */
	if (context->outStream == NULL)
	{
		return true;
	}

	PQExpBuffer buf = createPQExpBuffer();

	printfPQExpBuffer(buf, "%s%d; %u %u %s %s\n",
					  skip ? ";" : "",
					  item->dumpId,
					  item->catalogOid,
					  item->objectOid,
					  item->description,
					  item->restoreListName != NULL ? item->restoreListName : "");

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(buf))
	{
		log_error("Failed to create pg_restore list file: out of memory");
		destroyPQExpBuffer(buf);
		return false;
	}

	if (!write_to_stream(context->outStream, buf->data, buf->len))
	{
		log_error("Failed to write pg_restore list file, "
				  "see above for details");
		return false;
	}

	destroyPQExpBuffer(buf);

	return true;
}


/*
 * copydb_collect_matview_toc_order scans the pg_dump archive TOC (already
 * produced in preListOutFilename by the pre-data pass) and assigns a monotone
 * toc_seq to each REFRESH MATERIALIZED VIEW entry.  The TOC is in pg_dump
 * dependency order, so this sequence is safe for the vacuum workers to follow
 * when refreshing: each worker that receives a QMSG_TYPE_MATVIEW_OID checks
 * s_matview_dep and waits until all dep matviews are done before proceeding.
 *
 * This must be called after copydb_write_restore_list(PRE_DATA) so that
 * preListOutFilename exists, and before the data phase begins so the INDEX
 * supervisor can read the toc_seq when all indexes are built.
 */
static bool
copydb_collect_matview_toc_order(CopyDataSpec *specs)
{
	/*
	 * On PG17+ targets, pg_restore owns REFRESH MATERIALIZED VIEW (the
	 * search_path bug that motivated pgcopydb owning it cannot arise there).
	 * Skip the toc_seq assignment entirely; vacuum workers will receive no
	 * QMSG_TYPE_MATVIEW_OID messages, and the REFRESH entries are left in
	 * the pg_restore post-data list for pg_restore to execute.
	 */
	if (specs->targetPgVersionNum >= 170000)
	{
		log_info("Target is PG%d (>= 17): REFRESH MATERIALIZED VIEW "
				 "handled by pg_restore, not pgcopydb vacuum workers",
				 specs->targetPgVersionNum / 10000);
		return true;
	}

	/*
	 * The preListOutFilename file was produced by pg_restore --list during
	 * the pre-data pass.  It lists ALL entries from the dump (not just
	 * pre-data), so REFRESH MATERIALIZED VIEW entries appear here too.
	 */
	char *listOutFilename = specs->dumpPaths.preListOutFilename;

	if (!file_exists(listOutFilename))
	{
		/* no dump file yet — nothing to do (e.g. --resume after full run) */
		return true;
	}

	RestoreListContext context = {
		.specs = specs,
		.outStream = NULL,
		.assignTocSeq = true,
	};

	if (!archive_iter_toc(listOutFilename, &context,
						  copydb_write_restore_list_hook))
	{
		log_error("Failed to assign toc_seq from pg_dump TOC, "
				  "see above for details");
		return false;
	}

	log_debug("Assigned toc_seq 1..%d to materialized view REFRESH entries",
			  context.matviewTocSeq);

	return true;
}
