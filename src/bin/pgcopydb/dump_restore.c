/*
 * src/bin/pgcopydb/dump_restore.c
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
							 specs->source_pguri,
							 snapshot,
							 "pre-data",
							 specs->dumpPaths.preFilename))
		{
			/* errors have already been logged */
			return false;
		}

		/* now write the doneFile to keep track */
		if (!write_file("", 0, specs->cfPaths.done.preDataDump))
		{
			log_error("Failed to write the tracking file \%s\"",
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
							 specs->source_pguri,
							 snapshot,
							 "post-data",
							 specs->dumpPaths.postFilename))
		{
			/* errors have already been logged */
			return false;
		}

		/* now write the doneFile to keep track */
		if (!write_file("", 0, specs->cfPaths.done.postDataDump))
		{
			log_error("Failed to write the tracking file \%s\"",
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

	if (!copydb_write_restore_list(specs, PG_DUMP_SECTION_PRE_DATA))
	{
		log_error("Failed to prepare the pg_restore --use-list catalogs, "
				  "see above for details");
		return true;
	}

	if (!pg_restore_db(&(specs->pgPaths),
					   specs->target_pguri,
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
		log_error("Failed to write the tracking file \%s\"",
				  specs->cfPaths.done.preDataRestore);
		return false;
	}

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
		return true;
	}

	if (!pg_restore_db(&(specs->pgPaths),
					   specs->target_pguri,
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
		log_error("Failed to write the tracking file \%s\"",
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

	switch (section)
	{
		case PG_DUMP_SECTION_PRE_DATA:
		{
			dumpFilename = specs->dumpPaths.preFilename;
			listFilename = specs->dumpPaths.preListFilename;
			break;
		}

		case PG_DUMP_SECTION_POST_DATA:
		{
			dumpFilename = specs->dumpPaths.postFilename;
			listFilename = specs->dumpPaths.postListFilename;
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
	 *   1. pg_restore -f- --list post.dump > post.list
	 *   2. edit post.list to comment out lines
	 *   3. pg_restore --use-list post.list post.dump
	 */
	ArchiveContentArray contents = { 0 };

	if (!pg_restore_list(&(specs->pgPaths), dumpFilename, &contents))
	{
		/* errors have already been logged */
		return false;
	}

	/* edit our post.list file now */
	PQExpBuffer listContents = createPQExpBuffer();

	if (listContents == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(listContents);
		return false;
	}

	/* for each object in the list, comment when we already processed it */
	for (int i = 0; i < contents.count; i++)
	{
		uint32_t oid = contents.array[i].objectOid;
		char *name = contents.array[i].restoreListName;
		char *prefix = "";

		if (copydb_objectid_has_been_processed_already(specs, oid))
		{
			prefix = ";";

			log_notice("Skipping already processed dumpId %d: %s %u %s",
					   contents.array[i].dumpId,
					   contents.array[i].desc,
					   contents.array[i].objectOid,
					   contents.array[i].restoreListName);
		}
		else if (copydb_objectid_is_filtered_out(specs, oid, name))
		{
			prefix = ";";

			log_notice("Skipping filtered-out dumpId %d: %s %u %s",
					   contents.array[i].dumpId,
					   contents.array[i].desc,
					   contents.array[i].objectOid,
					   contents.array[i].restoreListName);
		}

		appendPQExpBuffer(listContents, "%s%d; %u %u %s %s\n",
						  prefix,
						  contents.array[i].dumpId,
						  contents.array[i].catalogOid,
						  contents.array[i].objectOid,
						  contents.array[i].desc,
						  contents.array[i].restoreListName);
	}

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(listContents))
	{
		log_error("Failed to create pg_restore list file: out of memory");
		destroyPQExpBuffer(listContents);
		return false;
	}

	if (!write_file(listContents->data, listContents->len, listFilename))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(listContents);
		return false;
	}

	destroyPQExpBuffer(listContents);

	return true;
}
