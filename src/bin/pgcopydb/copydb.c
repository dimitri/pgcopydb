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
#include "schema.h"
#include "signals.h"
#include "string_utils.h"


static int waitUntilOneSubprocessIsDone(TableDataProcessArray *subProcessArray);


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
	PGSQL pgsql = { 0 };
	SourceTableArray tableArray = { 0, NULL };

	TableDataProcessArray tableProcessArray = { specs->tableJobs, NULL };

	tableProcessArray.array =
		(TableDataProcess *) malloc(specs->tableJobs * sizeof(TableDataProcess));

	if (tableProcessArray.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
	}

	/* ensure the memory area is initialized to all zeroes */
	memset((void *) tableProcessArray.array,
		   0,
		   specs->tableJobs * sizeof(TableDataProcess));

	log_info("Listing ordinary tables in \"%s\"", specs->source_pguri);

	if (!pgsql_init(&pgsql, specs->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!schema_list_ordinary_tables(&pgsql, &tableArray))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Fetched information for %d tables", tableArray.count);

	for (int tableIndex = 0; tableIndex < tableArray.count; tableIndex++)
	{
		int subProcessIndex =
			waitUntilOneSubprocessIsDone(&tableProcessArray);

		/* initialize our TableDataProcess entry now */
		SourceTable *source = &(tableArray.array[tableIndex]);
		TableDataProcess *process = &(tableProcessArray.array[subProcessIndex]);

		process->oid = source->oid;

		sformat(process->lockFile, sizeof(process->lockFile),
				"%s/%d", specs->cfPaths->tbldir, process->oid);

		sformat(process->doneFile, sizeof(process->doneFile),
				"%s/%d.done", specs->cfPaths->tbldir, process->oid);

		log_debug("process slot[%d] oid %d file \"%s\" ",
				  subProcessIndex, process->oid, process->doneFile);

		/* okay now start the subprocess for this table */
		CopyTableDataSpec tableSpecs = {
			.cfPaths = specs->cfPaths,
			.pgPaths = specs->pgPaths,

			.source_pguri = specs->source_pguri,
			.target_pguri = specs->target_pguri,

			.sourceTable = source,
			.process = process,

			.tableJobs = specs->tableJobs,
			.indexJobs = specs->indexJobs
		};

		if (!copydb_start_table_data(&tableSpecs))
		{
			log_fatal("Failed to start a table data copy process for table "
					  "\"%s\".\"%s\", see above for details",
					  tableSpecs.sourceTable->nspname,
					  tableSpecs.sourceTable->relname);

			(void) copydb_fatal_exit(&tableProcessArray);
			return false;
		}

		log_debug("[%d] is processing table %d \"%s\".\"%s\" with oid %d",
				  tableSpecs.process->pid,
				  tableIndex,
				  tableSpecs.sourceTable->nspname,
				  tableSpecs.sourceTable->relname,
				  tableSpecs.process->oid);
	}

	/* now we have a unknown count of subprocesses still running */
	return copydb_wait_for_subprocesses();
}


/*
 * copydb_fatal_exit sends a termination signal to all the subprocess and waits
 * until all the known subprocess are finished, then returns true.
 */
bool
copydb_fatal_exit(TableDataProcessArray *subProcessArray)
{
	log_fatal("Terminating all subprocesses");
	return copydb_wait_for_subprocesses();
}


/*
 * copydb_wait_for_subprocesses calls waitpid() until no child process is known
 * running. It also fetches the return code of all the sub-processes, and
 * returns true only when all the subprocesses have returned zero (success).
 */
bool
copydb_wait_for_subprocesses()
{
	bool allReturnCodeAreZero = true;

	log_debug("Waiting for sub-processes to finish");

	for(;;)
	{
		int status;

		/* ignore errors */
		pid_t pid = waitpid(-1, &status, WNOHANG);

		switch (pid)
		{
			case -1:
			{
				if (errno == ECHILD)
				{
					/* no more childrens */
					return true;
				}

				pg_usleep(100 * 1000); /* 100 ms */
				break;
			}

			case 0:
			{
				/*
				 * We're using WNOHANG, 0 means there are no stopped or exited
				 * children sleep for awhile and ask again later.
				 */
				pg_usleep(100 * 1000); /* 100 ms */
				break;
			}

			default:
			{
				int returnCode = WEXITSTATUS(status);

				if (returnCode == 0)
				{
					log_debug("Sub-processes exited with code %d", returnCode);
				}
				else
				{
					allReturnCodeAreZero = false;

					log_error("Sub-processes exited with code %d", returnCode);
				}
			}
		}
	}

	return allReturnCodeAreZero;
}


/*
 * waitUntilOneSubprocessIsDone waits until one of the subprocess (any one of
 * them) has created its doneFile, and returns the array index of that process.
 *
 * The subProcessArray entry that is found done is also cleaned-up.
 */
static int
waitUntilOneSubprocessIsDone(TableDataProcessArray *subProcessArray)
{
	for (;;)
	{
		/* don't block user's interrupt (C-c and the like) */
		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			(void) copydb_fatal_exit(subProcessArray);
			return false;
		}

		/* probe each sub-process' doneFile */
		for (int procIndex = 0; procIndex < subProcessArray->count; procIndex++)
		{
			/* when the subprocess pid is zero, we have found an entry */
			if (subProcessArray->array[procIndex].pid == 0)
			{
				log_debug("waitUntilOneSubprocessIsDone: %d", procIndex);
				return procIndex;
			}

			/* when we have a doneFile, the process is done */
			if (file_exists(subProcessArray->array[procIndex].doneFile))
			{
				log_debug("waitUntilOneSubprocessIsDone: found \"%s\"",
						  subProcessArray->array[procIndex].doneFile);

				if (!unlink_file(subProcessArray->array[procIndex].doneFile))
				{
					/* errors have already been logged */
					(void) copydb_fatal_exit(subProcessArray);
					return -1;
				}

				/* clean-up the array entry, stop tracking this PID */
				subProcessArray->array[procIndex].pid = 0;

				/* now we have a slot to process the current table */
				log_debug("waitUntilOneSubprocessIsDone: %d", procIndex);
				return procIndex;
			}
		}

		/*
		 * If all subprocesses are still running, wait for a little
		 * while and then check again. The check being cheap enough
		 * (it's just an access(2) system call), we sleep for 150ms
		 * only.
		 */
		pg_usleep(150 * 1000);
	}
}


/*
 * copydb_start_table_data forks a sub-process that handles the table data
 * copying, using pg_dump | pg_restore for a specific given table.
 */
bool
copydb_start_table_data(CopyTableDataSpec *tableSpecs)
{
	/* Flush stdio channels just before fork, to avoid double-output problems */
	fflush(stdout);
	fflush(stderr);

	/* time to create the node_active sub-process */
	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a process for copying table data for "
					  "\"%s\".\"%s\"",
					  tableSpecs->sourceTable->nspname,
					  tableSpecs->sourceTable->relname);
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			if (!copydb_copy_table(tableSpecs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			tableSpecs->process->pid = fpid;

			return true;
		}
	}
}


/*
 * copydb_copy_table implements the sub-process activity to pg_dump |
 * pg_restore the table's data and then create the indexes and the constraints
 * in parallel.
 */
bool
copydb_copy_table(CopyTableDataSpec *tableSpecs)
{
	PGSQL pgsql = { 0 };
	SourceIndexArray indexArray = { 0, NULL };

	/* First, write the lockFile file */
	write_file("", 0, tableSpecs->process->lockFile);

	log_info("pg_dump --data-only --schema \"%s\" --table \"%s\" | "
			 "pg_restore",
			 tableSpecs->sourceTable->nspname,
			 tableSpecs->sourceTable->relname);

	/* pretend we're copying the data around */
	sleep(3);

	/* now say we're done with the table data */
	write_file("", 0, tableSpecs->process->doneFile);

	/* then fetch the index list for this table */
	if (!pgsql_init(&pgsql, tableSpecs->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!schema_list_table_indexes(&pgsql,
								   tableSpecs->sourceTable->nspname,
								   tableSpecs->sourceTable->relname,
								   &indexArray))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Creating %d indexes for table \"%s\".\"%s\"",
			 indexArray.count,
			 tableSpecs->sourceTable->nspname,
			 tableSpecs->sourceTable->relname);

	/* pretend we're creating the indexes */
	for (int i = 0; i < indexArray.count; i++)
	{
		SourceIndex *index = &(indexArray.array[i]);

		log_info("%s", index->indexDef);

		sleep(1);

		if (!IS_EMPTY_STRING_BUFFER(index->constraintName))
		{
			log_info("alter table \"%s\".\"%s\" add constraint \"%s\" %s",
					 index->tableNamespace,
					 index->tableRelname,
					 index->constraintName,
					 index->constraintDef);

			sleep(1);
		}
	}

	/* finally, vacuum analyze the table and its indexes */
	log_info("vacuum analyze \"%s\".\"%s\"",
			 tableSpecs->sourceTable->nspname,
			 tableSpecs->sourceTable->relname);
	/* now we're done */
	return true;
}
