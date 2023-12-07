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
 * copydb_objectid_has_been_processed_already returns true when a doneFile
 * could be found on-disk for the given target object OID.
 */
bool
copydb_objectid_has_been_processed_already(CopyDataSpec *specs, uint32_t oid)
{
	char doneFile[MAXPGPATH] = { 0 };

	/* build the doneFile for the target index or constraint */
	sformat(doneFile, sizeof(doneFile), "%s/%u.done",
			specs->cfPaths.idxdir,
			oid);

	return file_exists(doneFile);
}


/*
 * copydb_dump_source_schema uses pg_dump -Fc --schema --section=pre-data or
 * --section=post-data to dump the source database schema to files.
 */
bool
copydb_dump_source_schema(CopyDataSpec *specs,
						  const char *snapshot,
						  PostgresDumpSection section)
{
	SourceExtensionArray *extensionArray = NULL;

	if (specs->skipExtensions)
	{
		extensionArray = &(specs->catalog.extensionArray);
	}

	if (section == PG_DUMP_SECTION_SCHEMA ||
		section == PG_DUMP_SECTION_PRE_DATA ||
		section == PG_DUMP_SECTION_ALL)
	{
		if (file_exists(specs->cfPaths.done.preDataDump))
		{
			log_info("Skipping pg_dump --section=pre-data, "
					 "as \"%s\" already exists",
					 specs->cfPaths.done.preDataDump);
		}
		else if (!pg_dump_db(&(specs->pgPaths),
							 &(specs->connStrings),
							 snapshot,
							 "pre-data",
							 &(specs->filters),
							 extensionArray,
							 specs->dumpPaths.preFilename))
		{
			/* errors have already been logged */
			return false;
		}

		/* now write the doneFile to keep track */
		if (!write_file("", 0, specs->cfPaths.done.preDataDump))
		{
			log_error("Failed to write the tracking file \"%s\"",
					  specs->cfPaths.done.preDataDump);
			return false;
		}
	}

	if (section == PG_DUMP_SECTION_SCHEMA ||
		section == PG_DUMP_SECTION_POST_DATA ||
		section == PG_DUMP_SECTION_ALL)
	{
		if (file_exists(specs->cfPaths.done.postDataDump))
		{
			log_info("Skipping pg_dump --section=post-data, "
					 "as \"%s\" already exists",
					 specs->cfPaths.done.postDataDump);
		}
		else if (!pg_dump_db(&(specs->pgPaths),
							 &(specs->connStrings),
							 snapshot,
							 "post-data",
							 &(specs->filters),
							 extensionArray,
							 specs->dumpPaths.postFilename))
		{
			/* errors have already been logged */
			return false;
		}

		/* now write the doneFile to keep track */
		if (!write_file("", 0, specs->cfPaths.done.postDataDump))
		{
			log_error("Failed to write the tracking file \"%s\"",
					  specs->cfPaths.done.postDataDump);
			return false;
		}
	}

	return true;
}


/*
 * copydb_target_prepare_schema restores the pre.dump file into the target
 * database.
 */
bool
copydb_target_prepare_schema(CopyDataSpec *specs)
{
	if (!file_exists(specs->dumpPaths.preFilename))
	{
		log_fatal("File \"%s\" does not exists", specs->dumpPaths.preFilename);
		return false;
	}

	if (file_exists(specs->cfPaths.done.preDataRestore))
	{
		log_info("Skipping pg_restore of pre-data section, "
				 "done on a previous run");
		return true;
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
	 * Now prepare the pg_restore --use-list file.
	 */
	if (!copydb_write_restore_list(specs, PG_DUMP_SECTION_PRE_DATA))
	{
		log_error("Failed to prepare the pg_restore --use-list catalogs, "
				  "see above for details");
		return true;
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

	if (!pg_restore_db(&(specs->pgPaths),
					   &(specs->connStrings),
					   &(specs->filters),
					   specs->dumpPaths.preFilename,
					   specs->dumpPaths.preListFilename,
					   specs->restoreOptions))
	{
		/* errors have already been logged */
		return false;
	}

	/* now write the doneFile to keep track */
	if (!write_file("", 0, specs->cfPaths.done.preDataRestore))
	{
		log_error("Failed to write the tracking file \"%s\"",
				  specs->cfPaths.done.preDataRestore);
		return false;
	}

	return true;
}


/*
 * copydb_copy_database_properties uses ALTER DATABASE SET commands to set the
 * properties on the target database to look the same way as on the source
 * database.
 */
bool
copydb_copy_database_properties(CopyDataSpec *specs)
{
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

	PGconn *conn = dst.connection;
	SourceRole *rolesHashByName = specs->targetCatalog.rolesHashByName;

	if (specs->catalog.gucsArray.count > 0)
	{
		for (int i = 0; i < specs->catalog.gucsArray.count; i++)
		{
			SourceProperty *property = &(specs->catalog.gucsArray.array[i]);

			/*
			 * ALTER ROLE rolname IN DATABASE datname SET ...
			 */
			if (property->roleInDatabase)
			{
				char *rolname = property->rolname;
				int len = strlen(rolname);

				SourceRole *role = NULL;

				HASH_FIND(hh, rolesHashByName, rolname, len, role);

				if (role != NULL)
				{
					PQExpBuffer command = createPQExpBuffer();

					makeAlterConfigCommand(conn, property->setconfig, "ROLE",
										   property->rolname, "DATABASE",
										   specs->connStrings.safeTargetPGURI.uriParams.
										   dbname, command);

					/* chomp the \n */
					if (command->data[command->len - 1] == '\n')
					{
						command->data[command->len - 1] = '\0';
					}

					log_info("%s", command->data);

					if (!pgsql_execute(&dst, command->data))
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
							 rolname);
				}
			}

			/*
			 * ALTER DATABASE datname SET ...
			 */
			else
			{
				PQExpBuffer command = createPQExpBuffer();

				makeAlterConfigCommand(conn, property->setconfig, "DATABASE",
									   specs->connStrings.safeTargetPGURI.uriParams.dbname,
									   NULL, NULL, command);

				if (command->data[command->len - 1] == '\n')
				{
					command->data[command->len - 1] = '\0';
				}

				log_info("%s", command->data);

				if (!pgsql_execute(&dst, command->data))
				{
					/* errors have already been logged */
					return false;
				}

				destroyPQExpBuffer(command);
			}
		}
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_target_drop_tables prepares and executes a SQL query that prepares
 * our target database by means of a DROP IF EXISTS ... CASCADE statement that
 * includes all our target tables.
 */
bool
copydb_target_drop_tables(CopyDataSpec *specs)
{
	log_info("Drop tables on the target database, per --drop-if-exists");

	SourceTableArray *tableArray = &(specs->catalog.sourceTableArray);

	if (tableArray->count == 0)
	{
		log_info("No tables to migrate, skipping drop tables on the target database");
		return true;
	}

	PQExpBuffer query = createPQExpBuffer();

	appendPQExpBufferStr(query, "DROP TABLE IF EXISTS");

	for (int tableIndex = 0; tableIndex < tableArray->count; tableIndex++)
	{
		SourceTable *source = &(tableArray->array[tableIndex]);

		appendPQExpBuffer(query, "%s %s.%s",
						  tableIndex == 0 ? " " : ",",
						  source->nspname,
						  source->relname);
	}

	appendPQExpBufferStr(query, " CASCADE");

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(query))
	{
		log_error("Failed to create DROP IF EXISTS query: out of memory");
		destroyPQExpBuffer(query);
		return false;
	}

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
 * copydb_target_finalize_schema finalizes the schema after all the data has
 * been copied over, and after indexes and their constraints have been created
 * too.
 */
bool
copydb_target_finalize_schema(CopyDataSpec *specs)
{
	if (!file_exists(specs->dumpPaths.postFilename))
	{
		log_fatal("File \"%s\" does not exists", specs->dumpPaths.postFilename);
		return false;
	}

	if (file_exists(specs->cfPaths.done.postDataRestore))
	{
		log_info("Skipping pg_restore --section=pre-data, "
				 "done on a previous run");
		return true;
	}

	if (!copydb_write_restore_list(specs, PG_DUMP_SECTION_POST_DATA))
	{
		log_error("Failed to prepare the pg_restore --use-list catalogs, "
				  "see above for details");
		return false;
	}

	if (!pg_restore_db(&(specs->pgPaths),
					   &(specs->connStrings),
					   &(specs->filters),
					   specs->dumpPaths.postFilename,
					   specs->dumpPaths.postListFilename,
					   specs->restoreOptions))
	{
		/* errors have already been logged */
		return false;
	}

	/* now write the doneFile to keep track */
	if (!write_file("", 0, specs->cfPaths.done.postDataRestore))
	{
		log_error("Failed to write the tracking file \"%s\"",
				  specs->cfPaths.done.postDataRestore);
		return false;
	}

	return true;
}


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
			dumpFilename = specs->dumpPaths.preFilename;
			listFilename = specs->dumpPaths.preListFilename;
			listOutFilename = specs->dumpPaths.preListOutFilename;
			break;
		}

		case PG_DUMP_SECTION_POST_DATA:
		{
			dumpFilename = specs->dumpPaths.postFilename;
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
	 * The pre.dump archive file contains all the objects to create in the
	 * target database. We want to filter out the schemas and tables excluded
	 * from the filtering setup.
	 *
	 * The post.dump archive file contains all the objects to create once the
	 * table data has been copied over. It contains in particular the
	 * constraints and indexes that we have already built concurrently in the
	 * previous step, so we want to filter those out.
	 *
	 * Here's how to filter out some objects with pg_restore:
	 *
	 *   1. pg_restore -f post.list --list post.dump
	 *   2. edit post.list to comment out lines and save as filtered.list
	 *   3. pg_restore --use-list filtered.list post.dump
	 */
	ArchiveContentArray contents = { 0 };

	if (!pg_restore_list(&(specs->pgPaths),
						 dumpFilename,
						 listOutFilename,
						 &contents))
	{
		/* errors have already been logged */
		FreeArchiveContentArray(&contents);
		return false;
	}

	/* edit our post.list file now */
	PQExpBuffer listContents = createPQExpBuffer();

	if (listContents == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		destroyPQExpBuffer(listContents);
		return false;
	}

	/* for each object in the list, comment when we already processed it */
	for (int i = 0; i < contents.count; i++)
	{
		ArchiveContentItem *item = &(contents.array[i]);
		uint32_t oid = item->objectOid;
		uint32_t catOid = item->catalogOid;
		char *name = item->restoreListName;

		bool skip = false;

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
			log_notice("Skipping COMMENT ON EXTENSION \"%s\"", name);
		}

		if (!skip && catOid == PG_NAMESPACE_OID)
		{
			bool exists = false;

			if (!copydb_schema_already_exists(specs, name, &exists))
			{
				log_error("Failed to check if restore name \"%s\" "
						  "already exists",
						  name);
				destroyPQExpBuffer(listContents);
				return false;
			}

			if (exists)
			{
				skip = true;

				log_notice("Skipping already existing dumpId %d: %s %u %s",
						   contents.array[i].dumpId,
						   contents.array[i].description,
						   contents.array[i].objectOid,
						   contents.array[i].restoreListName);
			}
		}

		if (!skip && copydb_objectid_has_been_processed_already(specs, oid))
		{
			skip = true;

			log_notice("Skipping already processed dumpId %d: %s %u %s",
					   contents.array[i].dumpId,
					   contents.array[i].description,
					   contents.array[i].objectOid,
					   contents.array[i].restoreListName);
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
		 */
		if (item->desc == ARCHIVE_TAG_SEQUENCE)
		{
			name = NULL;
		}

		if (!skip && copydb_objectid_is_filtered_out(specs, oid, name))
		{
			skip = true;

			log_notice("Skipping filtered-out dumpId %d: %s %u %u %s",
					   contents.array[i].dumpId,
					   contents.array[i].description,
					   contents.array[i].catalogOid,
					   contents.array[i].objectOid,
					   contents.array[i].restoreListName);
		}

		appendPQExpBuffer(listContents, "%s%d; %u %u %s %s\n",
						  skip ? ";" : "",
						  contents.array[i].dumpId,
						  contents.array[i].catalogOid,
						  contents.array[i].objectOid,
						  contents.array[i].description,
						  contents.array[i].restoreListName);
	}

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(listContents))
	{
		log_error("Failed to create pg_restore list file: out of memory");
		destroyPQExpBuffer(listContents);
		FreeArchiveContentArray(&contents);
		return false;
	}

	log_notice("Write filtered pg_restore list file at \"%s\"", listFilename);

	if (!write_file(listContents->data, listContents->len, listFilename))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(listContents);
		FreeArchiveContentArray(&contents);
		return false;
	}

	destroyPQExpBuffer(listContents);
	FreeArchiveContentArray(&contents);

	return true;
}
