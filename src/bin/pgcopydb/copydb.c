/*
 * src/bin/pgcopydb/copydb.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_copy.h"
#include "cli_root.h"
#include "copydb.h"
#include "env_utils.h"
#include "log.h"
#include "pidfile.h"
#include "string_utils.h"


/*
 * copydb_init_tempdir initialises the file paths that are going to be used to
 * store temporary information while the pgcopydb process is running.
 */
bool
copydb_init_workdir(CopyFilePaths *cfPaths, char *dir)
{
	pid_t pid = getpid();

	if (dir != NULL && !IS_EMPTY_STRING_BUFFER(dir))
	{
		strlcpy(cfPaths->topdir, dir, sizeof(cfPaths->topdir));
	}
	else
	{
		char tmpdir[MAXPGPATH] = { 0 };

		if (!get_env_copy_with_fallback("XDG_RUNTIME_DIR",
										tmpdir,
										sizeof(tmpdir),
										"/tmp"))
		{
			/* errors have already been logged */
			return false;
		}

		sformat(cfPaths->topdir, MAXPGPATH, "%s/pgcopydb", tmpdir);
	}

	/* now that we have our topdir, prepare all the others from there */
	sformat(cfPaths->pidfile, MAXPGPATH, "%s/pgcopydb.pid", cfPaths->topdir);
	sformat(cfPaths->schemadir, MAXPGPATH, "%s/schema", cfPaths->topdir);
	sformat(cfPaths->rundir, MAXPGPATH, "%s/run", cfPaths->topdir);
	sformat(cfPaths->tbldir, MAXPGPATH, "%s/run/tables", cfPaths->topdir);

	sformat(cfPaths->idxfilepath, MAXPGPATH,
			"%s/run/indexes.json", cfPaths->topdir);

	sformat(cfPaths->listdonefilepath, MAXPGPATH,
			"%s/objects.list", cfPaths->topdir);

	/* now create the target directories that we depend on. */
	if (directory_exists(cfPaths->topdir))
	{
		pid_t onFilePid = 0;

		if (file_exists(cfPaths->pidfile))
		{
			/*
			 * Only implement the "happy path": read_pidfile removes the file
			 * when if fails to read it, or when the pid contained in there in
			 * a stale pid (doesn't belong to any currently running process).
			 */
			if (read_pidfile(cfPaths->pidfile, &onFilePid))
			{
				log_fatal("Working directory \"%s\" already exists and "
						  "contains a pidfile for process %d, "
						  "which is currently running",
						  cfPaths->topdir,
						  onFilePid);
				return false;
			}
		}

		/* warn about trashing data from a previous run */
		if (dir == NULL)
		{
			log_warn("Directory \"%s\" already exists: removing it entirely",
					 cfPaths->topdir);
		}
	}

	if (dir == NULL)
	{
		log_debug("mkdir -p \"%s\"", cfPaths->topdir);
		if (!ensure_empty_dir(cfPaths->topdir, 0700))
		{
			/* errors have already been logged. */
			return false;
		}
	}

	/* now populate our pidfile */
	if (!create_pidfile(cfPaths->pidfile, pid))
	{
		return false;
	}

	/* and now for the other sub-directories */
	const char *dirs[] = {
		cfPaths->schemadir,
		cfPaths->rundir,
		cfPaths->tbldir,
		NULL
	};

	if (dir == NULL)
	{
		for (int i = 0; dirs[i] != NULL; i++)
		{
			if (!ensure_empty_dir(dirs[i], 0700))
			{
				return false;
			}
		}
	}
	else
	{
		/* with dir is not null, refrain from removing anything */
		for (int i = 0; dirs[i] != NULL; i++)
		{
			if (!directory_exists(dir))
			{
				if (pg_mkdir_p((char *) dirs[i], 0700) == -1)
				{
					log_fatal("Failed to create directory \"%s\"", dir);
					return false;
				}
			}
		}
	}

	return true;
}


/*
 * copydb_dump_source_schema uses pg_dump -Fc --schema --section=pre-data or
 * --section=post-data to dump the source database schema to files.
 */
bool
copydb_dump_source_schema(PostgresPaths *pgPaths,
						  CopyFilePaths *cfPaths,
						  const char *pguri)
{
	char preFilename[MAXPGPATH] = { 0 };
	char postFilename[MAXPGPATH] = { 0 };

	sformat(preFilename, MAXPGPATH, "%s/%s", cfPaths->schemadir, "pre.dump");
	sformat(postFilename, MAXPGPATH, "%s/%s", cfPaths->schemadir, "post.dump");

	if (!pg_dump_db(pgPaths, pguri, "pre-data", preFilename))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pg_dump_db(pgPaths, pguri, "post-data", postFilename))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_target_prepare_schema restores the pre.dump file into the target
 * database.
 */
bool
copydb_target_prepare_schema(PostgresPaths *pgPaths,
							 CopyFilePaths *cfPaths,
							 const char *pguri)
{
	char preFilename[MAXPGPATH] = { 0 };

	sformat(preFilename, MAXPGPATH, "%s/%s", cfPaths->schemadir, "pre.dump");

	if (!file_exists(preFilename))
	{
		log_fatal("File \"%s\" does not exists", preFilename);
		return false;
	}

	if (!pg_restore_db(pgPaths, pguri, preFilename))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}
