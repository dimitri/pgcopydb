/*
 * src/bin/pgcopydb/multi_db.c
 *	 Clone all databases from a source Postgres instance.
 *
 * Phase C: global cross-database COPY queue.
 *
 *   Phase I  (pre-data)  — sequential, one subprocess per database:
 *                           schema fetch, pg_dump, pg_restore --pre-data
 *   Phase II (global COPY) — tableJobs shared COPY workers dequeue from a
 *                           single size-ordered queue spanning all databases
 *   Phase III (post-data) — sequential, one subprocess per database:
 *                           pg_restore --post-data, set sequences
 */

#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

#include "catalog.h"
#include "copydb.h"
#include "defaults.h"
#include "file_utils.h"
#include "log.h"
#include "multi_db.h"
#include "parsing_utils.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


static bool multidb_is_system_database(const char *datname);
static bool multidb_target_database_exists(PGSQL *dst, const char *datname,
										   bool *exists);
static bool multidb_create_target_database(PGSQL *dst, const char *datname,
										   bool dropIfExists);
static bool multidb_build_uri_for_database(const char *pguri,
										   const char *datname,
										   char **result_uri);
static bool multidb_build_conn_strings(ConnStrings *parent,
									   const char *datname,
									   ConnStrings *dbConnStrings);
static bool multidb_init_db_specs(CopyDataSpec *dbSpecs,
								  CopyDataSpec *parent,
								  ConnStrings *connStrings);
static bool multidb_setup_one_database(CopyDataSpec *parentSpecs,
									   SourceDatabase *db,
									   CopyDataSpec *dbSpecs,
									   ConnStrings *dbConnStrings,
									   MultiDbInfo *info);

/* Phase I: parallel pre-data (schema fetch + pg_dump + pg_restore --pre-data) */
static bool multidb_pre_data_supervisor(CopyDataSpec *parentSpecs);
static bool multidb_pre_data_queue_filler(CopyDataSpec *parentSpecs);
static bool multidb_pre_data_worker(CopyDataSpec *parentSpecs);
static bool multidb_pre_data_one_db(CopyDataSpec *parentSpecs,
									const char *datname);

static bool multidb_clone_one_database_post_data(CopyDataSpec *parentSpecs,
												  SourceDatabase *db,
												  CopyDataSpec *dbSpecs,
												  ConnStrings *dbConnStrings);
static bool multidb_global_copy(CopyDataSpec *parentSpecs);
static bool multidb_wait_child(pid_t pid, const char *label);

/* connection cache helpers (also called from table-data.c) */
static bool multidb_init_entry(MultiDbEntry *entry,
								CopyDataSpec *parentSpecs,
								const char *datname);


/*
 * clone_all_databases clones all user databases from a source Postgres
 * instance in three phases:
 *
 *  Phase I  — per-database pre-data (sequential subprocesses)
 *  Phase II — global COPY with shared worker pool sorted by table size
 *  Phase III — per-database post-data (sequential subprocesses)
 */
bool
clone_all_databases(CopyDataSpec *parentSpecs)
{
	/*
	 * The top-level parentSpecs has queues created by copydb_init_specs that we
	 * won't use — release them now so we don't leak System V resources.
	 */
	if (parentSpecs->vacuumQueue.qId != -1)
	{
		(void) queue_unlink(&parentSpecs->vacuumQueue);
		parentSpecs->vacuumQueue.qId = -1;
	}

	if (parentSpecs->indexQueue.qId != -1)
	{
		(void) queue_unlink(&parentSpecs->indexQueue);
		parentSpecs->indexQueue.qId = -1;
	}

	/*
	 * Initialise the top-level source catalog (stores the database list).
	 */
	if (!catalog_init_from_specs(parentSpecs))
	{
		log_error("Failed to initialise the instance-level catalog");
		return false;
	}

	/* connect to the source instance and populate the database list */
	PGSQL src = { 0 };

	if (!pgsql_init(&src, parentSpecs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
	{
		log_error("Failed to initialise source connection");
		return false;
	}

	DatabaseCatalog *instanceCatalog = &(parentSpecs->catalogs.source);

	if (!schema_list_databases(&src, instanceCatalog))
	{
		log_error("Failed to list databases on the source instance");
		pgsql_finish(&src);
		return false;
	}

	pgsql_finish(&src);

	/* copy instance-level roles once */
	log_info("Copying instance-level roles");

	if (!pg_copy_roles(&parentSpecs->pgPaths,
					   &parentSpecs->connStrings,
					   parentSpecs->dumpPaths.rolesFilename,
					   parentSpecs->noRolesPasswords))
	{
		log_error("Failed to copy roles from source instance");
		return false;
	}

	/*
	 * Count databases so we can allocate arrays.
	 */
	CatalogCounts counts = { 0 };

	if (!catalog_count_objects(instanceCatalog, &counts))
	{
		log_error("Failed to count objects in the instance catalog");
		return false;
	}

	int maxDbs = (int) counts.databases;

	if (maxDbs == 0)
	{
		log_info("No databases found on the source instance");
		(void) catalog_close_from_specs(parentSpecs);
		return true;
	}

	/* allocate per-database arrays */
	SourceDatabase *dbArray =
		(SourceDatabase *) calloc(maxDbs, sizeof(SourceDatabase));
	CopyDataSpec *dbSpecsArray =
		(CopyDataSpec *) calloc(maxDbs, sizeof(CopyDataSpec));
	ConnStrings *dbConnStringsArray =
		(ConnStrings *) calloc(maxDbs, sizeof(ConnStrings));
	MultiDbInfo *multiDbInfos =
		(MultiDbInfo *) calloc(maxDbs, sizeof(MultiDbInfo));

	if (dbArray == NULL || dbSpecsArray == NULL ||
		dbConnStringsArray == NULL || multiDbInfos == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* open a connection to the target instance for CREATE DATABASE */
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, parentSpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		log_error("Failed to initialise target connection");
		return false;
	}

	bool ok = true;
	int dbCount = 0;

	SourceDatabaseIterator dbIter = {
		.catalog = instanceCatalog,
		.dat = NULL
	};

	if (!catalog_iter_s_database_init(&dbIter))
	{
		pgsql_finish(&dst);
		ok = false;
		goto cleanup_arrays;
	}

	for (;;)
	{
		if (!catalog_iter_s_database_next(&dbIter))
		{
			ok = false;
			break;
		}

		SourceDatabase *db = dbIter.dat;

		if (db == NULL)
			break;        /* end of results */

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_info("Stopping --all-databases setup due to signal");
			ok = false;
			break;
		}

		if (multidb_is_system_database(db->datname))
		{
			log_debug("Skipping system database \"%s\"", db->datname);
			continue;
		}

		if (dbCount >= maxDbs)
		{
			log_error("More databases than expected (%d), internal error",
					  maxDbs);
			ok = false;
			break;
		}

		log_info("Found database \"%s\" (%s)", db->datname, db->bytesPretty);

		/* copy db info into our array */
		dbArray[dbCount] = *db;

		/* create target database */
		if (!multidb_create_target_database(&dst, db->datname,
											parentSpecs->restoreOptions.dropIfExists))
		{
			log_error("Failed to create database \"%s\" on target", db->datname);
			ok = false;
			if (parentSpecs->failFast)
				break;
			else
			{
				dbCount++;
				continue;
			}
		}

		/* set up work dir and export snapshot for this database */
		dbSpecsArray[dbCount].pgPaths = parentSpecs->pgPaths;

		if (!multidb_setup_one_database(parentSpecs,
										&dbArray[dbCount],
										&dbSpecsArray[dbCount],
										&dbConnStringsArray[dbCount],
										&multiDbInfos[dbCount]))
		{
			log_error("Failed to setup database \"%s\"", db->datname);
			ok = false;
			if (parentSpecs->failFast)
				break;
		}

		dbCount++;
	}

	if (!catalog_iter_s_database_finish(&dbIter))
		ok = false;

	pgsql_finish(&dst);

	if (!ok)
		goto cleanup_snapshots;

	/*
	 * Store per-database info in parentSpecs so the global COPY workers (which
	 * are forked after this point) can access it via the COW post-fork copy.
	 */
	parentSpecs->multiDbCount = dbCount;
	parentSpecs->multiDbInfos = multiDbInfos;

	/* close instance-level catalog before forking subprocesses */
	if (!catalog_close_from_specs(parentSpecs))
	{
		ok = false;
		goto cleanup_snapshots;
	}

	/* ====== PHASE I: PRE-DATA (parallel, bounded by --table-jobs) ====== */
	log_info("Phase I: parallel schema fetch, dump, and pre-data restore "
			 "for %d databases (--table-jobs=%d)",
			 dbCount, parentSpecs->tableJobs);

	{
		fflush(stdout);
		fflush(stderr);

		pid_t prePid = fork();

		switch (prePid)
		{
			case -1:
			{
				log_error("Failed to fork pre-data supervisor: %m");
				ok = false;
				goto cleanup_snapshots;
			}

			case 0:
			{
				/* pre-data supervisor process */
				bool res = multidb_pre_data_supervisor(parentSpecs);
				exit(res ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
			}

			default:
				break;
		}

		if (!multidb_wait_child(prePid, "pre-data supervisor"))
		{
			ok = false;
			goto cleanup_snapshots;
		}
	}

	/* ====== PHASE II: GLOBAL COPY ====== */
	log_info("Phase II: global COPY for %d databases (largest tables first)",
			 dbCount);

	if (!multidb_global_copy(parentSpecs))
	{
		log_error("Global COPY phase failed");
		ok = false;
		goto cleanup_snapshots;
	}

	/* ====== PHASE III: POST-DATA (sequential per-database) ====== */
	log_info("Phase III: post-data restore and sequences for %d databases",
			 dbCount);

	for (int i = 0; i < dbCount; i++)
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			ok = false;
			break;
		}

		log_info("Post-data for database \"%s\"", dbArray[i].datname);

		if (!multidb_clone_one_database_post_data(parentSpecs,
												  &dbArray[i],
												  &dbSpecsArray[i],
												  &dbConnStringsArray[i]))
		{
			log_error("Post-data failed for database \"%s\"", dbArray[i].datname);
			ok = false;
			if (parentSpecs->failFast)
				break;
		}
	}

cleanup_snapshots:
	/* close all per-database snapshot connections held in the parent */
	for (int i = 0; i < dbCount; i++)
	{
		if (dbSpecsArray[i].consistent &&
			dbSpecsArray[i].sourceSnapshot.state == SNAPSHOT_STATE_EXPORTED)
		{
			if (!copydb_close_snapshot(&dbSpecsArray[i]))
			{
				log_warn("Failed to close snapshot for database \"%s\"",
						 dbArray[i].datname);
			}
		}

		/* free heap-allocated connection strings */
		if (dbConnStringsArray[i].source_pguri)
			free(dbConnStringsArray[i].source_pguri);
		if (dbConnStringsArray[i].target_pguri)
			free(dbConnStringsArray[i].target_pguri);
		if (dbConnStringsArray[i].safeSourcePGURI.pguri)
			free(dbConnStringsArray[i].safeSourcePGURI.pguri);
		if (dbConnStringsArray[i].safeTargetPGURI.pguri)
			free(dbConnStringsArray[i].safeTargetPGURI.pguri);
	}

	parentSpecs->multiDbInfos = NULL;
	parentSpecs->multiDbCount = 0;

cleanup_arrays:
	free(multiDbInfos);
	free(dbConnStringsArray);
	free(dbSpecsArray);
	free(dbArray);

	return ok;
}


/*
 * multidb_setup_one_database builds the per-database work directory, exports
 * a Postgres snapshot (held by the parent), and fills in info.
 */
static bool
multidb_setup_one_database(CopyDataSpec *parentSpecs,
						   SourceDatabase *db,
						   CopyDataSpec *dbSpecs,
						   ConnStrings *dbConnStrings,
						   MultiDbInfo *info)
{
	/* build per-db work directory path */
	char dbdir[MAXPGPATH] = { 0 };

	sformat(dbdir, sizeof(dbdir), "%s/db/%s",
			parentSpecs->cfPaths.topdir, db->datname);

	/* build per-db connection strings */
	if (!multidb_build_conn_strings(&parentSpecs->connStrings, db->datname,
									dbConnStrings))
	{
		log_error("Failed to build connection strings for database \"%s\"",
				  db->datname);
		return false;
	}

	/*
	 * Create the per-database work directory.  Must happen in the parent so
	 * the directory exists before forking subprocesses.
	 */
	if (!copydb_init_workdir(dbSpecs, dbdir,
							 false,     /* service */
							 NULL,      /* serviceName */
							 parentSpecs->restart,
							 parentSpecs->resume,
							 true))     /* createWorkDir */
	{
		log_error("Failed to initialise work directory \"%s\" for database \"%s\"",
				  dbdir, db->datname);
		return false;
	}

	/* wire up connection strings and snapshot fields */
	dbSpecs->connStrings = *dbConnStrings;
	dbSpecs->sourceSnapshot.pguri = dbSpecs->connStrings.source_pguri;
	dbSpecs->sourceSnapshot.safeURI = dbSpecs->connStrings.safeSourcePGURI;
	dbSpecs->sourceSnapshot.connectionType = PGSQL_CONN_SOURCE;
	dbSpecs->consistent = parentSpecs->consistent;

	/*
	 * Export a per-database snapshot in the parent.  The parent keeps this
	 * connection open throughout Phase I and Phase II (global COPY) so that
	 * COPY workers can import it via SET TRANSACTION SNAPSHOT.
	 */
	if (dbSpecs->consistent)
	{
		if (!copydb_prepare_snapshot(dbSpecs))
		{
			log_error("Failed to export snapshot for database \"%s\"",
					  db->datname);
			return false;
		}
	}

	/* populate MultiDbInfo for workers */
	strlcpy(info->datname, db->datname, sizeof(info->datname));
	strlcpy(info->snapshot, dbSpecs->sourceSnapshot.snapshot,
			sizeof(info->snapshot));
	strlcpy(info->source_pguri, dbConnStrings->source_pguri,
			sizeof(info->source_pguri));
	strlcpy(info->target_pguri, dbConnStrings->target_pguri,
			sizeof(info->target_pguri));
	strlcpy(info->topdir, dbdir, sizeof(info->topdir));
	info->isReadOnly = dbSpecs->sourceSnapshot.isReadOnly;

	return true;
}


/*
 * multidb_pre_data_supervisor is the supervisor for Phase I.
 *
 * It runs inside a dedicated subprocess (forked by clone_all_databases).
 * The supervisor creates a preDataQueue, forks tableJobs workers, forks one
 * queue-filler child, then calls waitpid(-1) to collect all children.
 *
 * Process tree:
 *   clone_all_databases
 *   └── pre-data supervisor  (this function)
 *           ├── pre-data worker 1
 *           ├── pre-data worker N  (N = --table-jobs)
 *           └── pre-data queue filler  (sends database names, then STOPs)
 */
static bool
multidb_pre_data_supervisor(CopyDataSpec *parentSpecs)
{
	(void) set_ps_title("pgcopydb: pre-data supervisor");
	log_notice("Started pre-data supervisor %d [%d]", getpid(), getppid());

	/* create the IPC queue for Phase I — workers inherit qId after fork */
	if (!queue_create(&parentSpecs->preDataQueue, "pre-data"))
	{
		log_error("Failed to create the pre-data process queue");
		return false;
	}

	/* fork tableJobs pre-data workers */
	for (int i = 0; i < parentSpecs->tableJobs; i++)
	{
		fflush(stdout);
		fflush(stderr);

		pid_t wpid = fork();

		switch (wpid)
		{
			case -1:
			{
				log_error("Failed to fork pre-data worker: %m");
				(void) copydb_fatal_exit();
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			case 0:
			{
				(void) set_ps_title("pgcopydb: pre-data worker");
				bool ok = multidb_pre_data_worker(parentSpecs);
				exit(ok ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
			}

			default:
				break;
		}
	}

	/* fork the queue filler (child of supervisor) */
	{
		fflush(stdout);
		fflush(stderr);

		pid_t qpid = fork();

		switch (qpid)
		{
			case -1:
			{
				log_error("Failed to fork pre-data queue filler: %m");
				(void) copydb_fatal_exit();
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			case 0:
			{
				(void) set_ps_title("pgcopydb: pre-data queue databases");
				bool ok = multidb_pre_data_queue_filler(parentSpecs);
				exit(ok ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
			}

			default:
				break;
		}
	}

	/* supervisor waits for all children: workers + queue filler */
	bool ok = copydb_wait_for_subprocesses(parentSpecs->failFast);

	(void) queue_unlink(&parentSpecs->preDataQueue);
	parentSpecs->preDataQueue.qId = -1;

	return ok;
}


/*
 * multidb_pre_data_queue_filler sends one DBNAME message per database to the
 * preDataQueue, then sends tableJobs STOP messages so all workers terminate.
 */
static bool
multidb_pre_data_queue_filler(CopyDataSpec *parentSpecs)
{
	log_notice("Started pre-data queue filler %d [%d]", getpid(), getppid());

	for (int i = 0; i < parentSpecs->multiDbCount; i++)
	{
		MultiDbInfo *info = &parentSpecs->multiDbInfos[i];

		QMessage mesg = { .type = QMSG_TYPE_DBNAME };
		strlcpy(mesg.data.datname, info->datname, sizeof(mesg.data.datname));

		if (!queue_send(&parentSpecs->preDataQueue, &mesg))
		{
			log_error("Failed to enqueue database \"%s\" for pre-data",
					  info->datname);
			return false;
		}

		log_info("Pre-data queue filler: enqueued database \"%s\"",
				 info->datname);
	}

	/* one STOP per worker */
	for (int i = 0; i < parentSpecs->tableJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP };

		if (!queue_send(&parentSpecs->preDataQueue, &stop))
		{
			log_error("Failed to send STOP to pre-data queue");
			return false;
		}
	}

	log_info("Pre-data queue filler: sent %d databases, %d STOP messages",
			 parentSpecs->multiDbCount, parentSpecs->tableJobs);

	return true;
}


/*
 * multidb_pre_data_worker dequeues database names from preDataQueue and runs
 * the full pre-data pipeline for each one (schema fetch, pg_dump,
 * pg_restore --pre-data).  Processes databases sequentially until it receives
 * a STOP message.
 */
static bool
multidb_pre_data_worker(CopyDataSpec *parentSpecs)
{
	pid_t pid = getpid();
	uint64_t errors = 0;
	bool stop = false;

	log_notice("Started pre-data worker %d [%d]", pid, getppid());

	while (!stop)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(&parentSpecs->preDataQueue, &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("Pre-data worker %d interrupted by signal", pid);
			break;
		}

		if (!recv_ok)
		{
			log_error("Pre-data worker %d failed to receive from queue", pid);
			break;
		}

		switch (mesg.type)
		{
			case QMSG_TYPE_STOP:
			{
				stop = true;
				log_debug("Pre-data worker %d received STOP", pid);
				break;
			}

			case QMSG_TYPE_DBNAME:
			{
				const char *datname = mesg.data.datname;

				log_info("Pre-data worker %d: processing database \"%s\"",
						 pid, datname);

				summary_reset_toplevel_timings();

				if (!multidb_pre_data_one_db(parentSpecs, datname))
				{
					log_error("Pre-data worker %d: failed for database \"%s\"",
							  pid, datname);
					++errors;

					if (parentSpecs->failFast)
						return false;
				}
				break;
			}

			default:
			{
				log_error("Pre-data worker %d: unknown message type %ld",
						  pid, mesg.type);
				break;
			}
		}
	}

	log_notice("Pre-data worker %d finished with %llu error(s)",
			   pid, (unsigned long long) errors);

	return stop && errors == 0;
}


/*
 * multidb_pre_data_one_db performs the full pre-data pipeline for one
 * database: schema fetch from source, pg_dump, pg_restore --pre-data.
 *
 * Called from within a pre-data worker process.  All resources are
 * allocated locally and released before returning so the worker can
 * proceed to the next database.
 */
static bool
multidb_pre_data_one_db(CopyDataSpec *parentSpecs, const char *datname)
{
	/* locate MultiDbInfo for this database */
	MultiDbInfo *info = NULL;

	for (int i = 0; i < parentSpecs->multiDbCount; i++)
	{
		if (strcmp(parentSpecs->multiDbInfos[i].datname, datname) == 0)
		{
			info = &parentSpecs->multiDbInfos[i];
			break;
		}
	}

	if (info == NULL)
	{
		log_error("BUG: multidb_pre_data_one_db: no MultiDbInfo for "
				  "database \"%s\"", datname);
		return false;
	}

	log_info("[SOURCE] Pre-data for database \"%s\"", datname);

	/*
	 * Build per-database connection strings.  These are strdup'd here and
	 * freed at the end of this function.
	 */
	ConnStrings cs = { 0 };

	cs.source_pguri = strdup(info->source_pguri);
	cs.target_pguri = strdup(info->target_pguri);

	if (cs.source_pguri == NULL || cs.target_pguri == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(cs.source_pguri);
		return false;
	}

	if (!parse_and_scrub_connection_string(cs.source_pguri,
										   &cs.safeSourcePGURI))
	{
		log_error("Failed to scrub source URI for database \"%s\"", datname);
		free(cs.source_pguri);
		free(cs.target_pguri);
		return false;
	}

	if (!parse_and_scrub_connection_string(cs.target_pguri,
										   &cs.safeTargetPGURI))
	{
		log_error("Failed to scrub target URI for database \"%s\"", datname);
		free(cs.source_pguri);
		free(cs.target_pguri);
		if (cs.safeSourcePGURI.pguri)
			free(cs.safeSourcePGURI.pguri);
		return false;
	}

	bool ok = true;

	/*
	 * Build a local CopyDataSpec for this database.
	 * copydb_init_workdir must be called before multidb_init_db_specs so
	 * that cfPaths is populated.
	 */
	CopyDataSpec dbSpecs = { 0 };
	dbSpecs.pgPaths = parentSpecs->pgPaths;

	if (!copydb_init_workdir(&dbSpecs, info->topdir,
							 false,   /* service */
							 NULL,    /* serviceName */
							 false,   /* restart */
							 true,    /* resume */
							 false))  /* createWorkDir (already created) */
	{
		log_error("Failed to init work dir for database \"%s\"", datname);
		ok = false;
		goto cleanup_cs;
	}

	if (!multidb_init_db_specs(&dbSpecs, parentSpecs, &cs))
	{
		log_error("Failed to init db specs for database \"%s\"", datname);
		ok = false;
		goto cleanup_cs;
	}

	/*
	 * Pre-data workers do not run index or vacuum workers — destroy the
	 * queues multidb_init_db_specs may have created.
	 */
	if (dbSpecs.vacuumQueue.qId != -1)
	{
		(void) queue_unlink(&dbSpecs.vacuumQueue);
		dbSpecs.vacuumQueue.qId = -1;
	}

	if (dbSpecs.indexQueue.qId != -1)
	{
		(void) queue_unlink(&dbSpecs.indexQueue);
		dbSpecs.indexQueue.qId = -1;
	}

	/*
	 * Propagate the exported snapshot identifier so that pg_dump can use
	 * it via --snapshot=<id>.
	 */
	strlcpy(dbSpecs.sourceSnapshot.snapshot, info->snapshot,
			sizeof(dbSpecs.sourceSnapshot.snapshot));

	/* STEP 1: fetch source database schema into source.db */
	if (!copydb_fetch_schema_and_prepare_specs(&dbSpecs))
	{
		log_error("Failed to fetch schema for database \"%s\"", datname);
		ok = false;
		goto cleanup_specs;
	}

	/* STEP 2: dump source schema (pre-data + post-data) via pg_dump */
	if (!copydb_dump_source_schema(&dbSpecs, info->snapshot))
	{
		log_error("Failed to dump schema for database \"%s\"", datname);
		ok = false;
		goto cleanup_specs;
	}

	/* STEP 3: restore the pre-data section to the target via pg_restore */
	if (!copydb_target_prepare_schema(&dbSpecs))
	{
		log_error("Failed to restore pre-data for database \"%s\"", datname);
		ok = false;
	}

cleanup_specs:
	(void) catalog_close_from_specs(&dbSpecs);

cleanup_cs:
	free(cs.source_pguri);
	free(cs.target_pguri);
	if (cs.safeSourcePGURI.pguri)
		free(cs.safeSourcePGURI.pguri);
	if (cs.safeTargetPGURI.pguri)
		free(cs.safeTargetPGURI.pguri);

	return ok;
}


/*
 * multidb_clone_one_database_post_data forks a subprocess that performs the
 * post-data phase for one database: pg_restore post-data and set sequences.
 *
 * This runs after the global COPY phase has completed for all databases.
 */
static bool
multidb_clone_one_database_post_data(CopyDataSpec *parentSpecs,
									 SourceDatabase *db,
									 CopyDataSpec *dbSpecs,
									 ConnStrings *dbConnStrings)
{
	log_info("[TARGET] Post-data for database \"%s\" into \"%s\"",
			 db->datname, dbConnStrings->safeTargetPGURI.pguri);

	fflush(stdout);
	fflush(stderr);

	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork post-data subprocess for database \"%s\": %m",
					  db->datname);
			return false;
		}

		case 0:
		{
			/* child process */
			char psTitle[MAXPGPATH] = { 0 };
			sformat(psTitle, sizeof(psTitle), "pgcopydb: post-data %s", db->datname);
			(void) set_ps_title(psTitle);

			summary_reset_toplevel_timings();

			if (!multidb_init_db_specs(dbSpecs, parentSpecs, dbConnStrings))
			{
				log_error("Failed to initialise specs for database \"%s\"",
						  db->datname);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/*
			 * Re-use the existing catalog (pre-data already populated it).
			 * copydb_fetch_schema_and_prepare_specs will detect this and
			 * return early without re-fetching from source.
			 */
			if (!copydb_fetch_schema_and_prepare_specs(dbSpecs))
			{
				log_error("Failed to open catalog for database \"%s\"",
						  db->datname);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/* STEP 10: restore the post-data section (indexes, constraints, …) */
			if (!copydb_target_finalize_schema(dbSpecs))
			{
				log_error("Failed to finalize schema for database \"%s\"",
						  db->datname);
				exit(EXIT_CODE_TARGET);
			}

			/* Set sequences to match the source values captured during schema fetch */
			if (!copydb_copy_all_sequences(dbSpecs, false))
			{
				log_error("Failed to set sequences for database \"%s\"",
						  db->datname);
				exit(EXIT_CODE_TARGET);
			}

			(void) catalog_close_from_specs(dbSpecs);
			exit(EXIT_CODE_QUIT);
		}

		default:
			break;
	}

	/* parent: wait for post-data subprocess */
	return multidb_wait_child(fpid, db->datname);
}


/*
 * multidb_global_copy runs the global COPY phase.
 *
 * Process tree:
 *   clone_all_databases
 *   └── global copy supervisor  (this function forks it, then waits)
 *           ├── global copy worker 1
 *           ├── global copy worker N  (N = --table-jobs)
 *           └── global copy queue tables  (fills queue, then exits)
 *
 * Fix A — shared catalog semaphores:
 *   One System V semaphore is created per database before forking the
 *   supervisor.  All workers inherit the semaphore IDs via COW.
 *   multidb_init_entry wires the semaphore into the DatabaseCatalog
 *   before calling catalog_init(), so catalog_create_semaphore() finds
 *   semId != 0 and skips creating a second independent semaphore.
 *   Without this, each worker creates its own semaphore, breaking
 *   write serialisation and producing SQLite BUSY errors.
 */
static bool
multidb_global_copy(CopyDataSpec *parentSpecs)
{
	if (parentSpecs->tableJobs == 0 || parentSpecs->multiDbCount == 0)
	{
		log_info("No tables to COPY (tableJobs=%d, databases=%d)",
				 parentSpecs->tableJobs, parentSpecs->multiDbCount);
		return true;
	}

	/*
	 * Fix A: create one shared semaphore per database BEFORE forking the
	 * supervisor.  Workers inherit the semId via COW; catalog_init() then
	 * skips creating its own semaphore.
	 */
	for (int i = 0; i < parentSpecs->multiDbCount; i++)
	{
		MultiDbInfo *info = &parentSpecs->multiDbInfos[i];

		if (info->catalogSemId != 0)
			continue;   /* already created (shouldn't happen, but be safe) */

		Semaphore sema = { .initValue = 1, .reentrant = true };

		if (!semaphore_create(&sema))
		{
			log_error("Failed to create catalog semaphore for database \"%s\"",
					  info->datname);

			/* best-effort cleanup of already-created semaphores */
			for (int j = 0; j < i; j++)
			{
				MultiDbInfo *prev = &parentSpecs->multiDbInfos[j];

				if (prev->catalogSemId != 0)
				{
					Semaphore s = { .semId = prev->catalogSemId };
					(void) semaphore_unlink(&s);
					prev->catalogSemId = 0;
				}
			}
			return false;
		}

		info->catalogSemId = sema.semId;

		log_debug("Created catalog semaphore %d for database \"%s\"",
				  sema.semId, info->datname);
	}

	/*
	 * Fork the global copy supervisor.  The supervisor creates the copyQueue,
	 * forks workers and the queue filler, then collects all children.
	 */
	fflush(stdout);
	fflush(stderr);

	pid_t spid = fork();

	switch (spid)
	{
		case -1:
		{
			log_error("Failed to fork global copy supervisor: %m");
			return false;
		}

		case 0:
		{
			/* ====== global copy supervisor process ====== */
			(void) set_ps_title("pgcopydb: global copy supervisor");
			log_notice("Started global copy supervisor %d [%d]",
					   getpid(), getppid());

			/*
			 * Create the copyQueue here so workers can inherit its qId
			 * after they are forked below.
			 */
			if (!queue_create(&parentSpecs->copyQueue, "global copy"))
			{
				log_error("Failed to create the global COPY queue");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/* fork tableJobs COPY workers */
			log_info("Starting %d global COPY workers", parentSpecs->tableJobs);

			for (int i = 0; i < parentSpecs->tableJobs; i++)
			{
				fflush(stdout);
				fflush(stderr);

				pid_t wpid = fork();

				switch (wpid)
				{
					case -1:
					{
						log_error("Failed to fork global COPY worker: %m");
						(void) copydb_fatal_exit();
						exit(EXIT_CODE_INTERNAL_ERROR);
					}

					case 0:
					{
						(void) set_ps_title("pgcopydb: global copy worker");
						bool ok = copydb_table_data_worker_multidb(parentSpecs);
						exit(ok ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
					}

					default:
						break;
				}
			}

			/* fork the queue filler as a child of the supervisor */
			{
				fflush(stdout);
				fflush(stderr);

				pid_t qpid = fork();

				switch (qpid)
				{
					case -1:
					{
						log_error("Failed to fork global COPY queue filler: %m");
						(void) copydb_fatal_exit();
						exit(EXIT_CODE_INTERNAL_ERROR);
					}

					case 0:
					{
						(void) set_ps_title("pgcopydb: global copy queue tables");
						bool ok =
							copydb_copy_worker_queue_tables_multidb(parentSpecs);
						exit(ok ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
					}

					default:
						break;
				}
			}

			/* supervisor waits for all children (workers + queue filler) */
			bool ok = copydb_wait_for_subprocesses(parentSpecs->failFast);

			(void) queue_unlink(&parentSpecs->copyQueue);
			parentSpecs->copyQueue.qId = -1;

			exit(ok ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
		}

		default:
			break;
	}

	/* clone_all_databases waits for the one supervisor child */
	bool ok = multidb_wait_child(spid, "global copy supervisor");

	/* destroy the shared catalog semaphores */
	for (int i = 0; i < parentSpecs->multiDbCount; i++)
	{
		MultiDbInfo *info = &parentSpecs->multiDbInfos[i];

		if (info->catalogSemId != 0)
		{
			Semaphore sema = { .semId = info->catalogSemId };
			(void) semaphore_unlink(&sema);
			info->catalogSemId = 0;
		}
	}

	return ok;
}


/* ============================================================
 * MultiDbContext helpers — called from both multi_db.c and table-data.c
 * ============================================================ */

/*
 * multidb_context_init zeroes a MultiDbContext.
 */
bool
multidb_context_init(MultiDbContext *ctx, CopyDataSpec *parentSpecs)
{
	memset(ctx, 0, sizeof(*ctx));
	ctx->parentSpecs = parentSpecs;
	return true;
}


/*
 * multidb_context_get_entry looks up the per-database entry for datname in the
 * cache.  If not found, it initialises a new entry (possibly evicting an
 * existing one using round-robin).
 *
 * Returns NULL on error.
 */
MultiDbEntry *
multidb_context_get_entry(MultiDbContext *ctx, const char *datname)
{
	/* search existing active entries */
	for (int i = 0; i < MULTIDB_ENTRY_CACHE_MAX; i++)
	{
		if (ctx->entries[i].active &&
			strcmp(ctx->entries[i].datname, datname) == 0)
		{
			return &ctx->entries[i];
		}
	}

	/* find a free slot, or evict the next-in-round-robin */
	int slot = -1;

	for (int i = 0; i < MULTIDB_ENTRY_CACHE_MAX; i++)
	{
		if (!ctx->entries[i].active)
		{
			slot = i;
			break;
		}
	}

	if (slot < 0)
	{
		/* cache full: evict round-robin */
		slot = ctx->nextEvict % MULTIDB_ENTRY_CACHE_MAX;
		ctx->nextEvict = (ctx->nextEvict + 1) % MULTIDB_ENTRY_CACHE_MAX;

		log_debug("Evicting multidb connection cache entry for \"%s\"",
				  ctx->entries[slot].datname);

		if (!multidb_context_close_entry(&ctx->entries[slot]))
		{
			/* errors have already been logged */
			return NULL;
		}
	}

	/* initialise the new entry */
	MultiDbEntry *entry = &ctx->entries[slot];

	if (!multidb_init_entry(entry, ctx->parentSpecs, datname))
	{
		/* errors have already been logged */
		return NULL;
	}

	ctx->count++;
	return entry;
}


/*
 * multidb_context_close_entry closes all resources held by a cache entry.
 */
bool
multidb_context_close_entry(MultiDbEntry *entry)
{
	if (!entry->active)
		return true;

	log_debug("Closing multidb connection cache entry for \"%s\"",
			  entry->datname);

	/* close source snapshot connection */
	if (entry->dbSpecs != NULL)
	{
		(void) copydb_close_snapshot(entry->dbSpecs);
		(void) catalog_close_from_specs(entry->dbSpecs);
		free(entry->dbSpecs->connStrings.source_pguri);
		free(entry->dbSpecs->connStrings.target_pguri);
		free(entry->dbSpecs);
		entry->dbSpecs = NULL;
	}

	/* close target connection */
	(void) pgsql_finish(&entry->dst);

	entry->active = false;
	memset(entry->datname, 0, sizeof(entry->datname));

	return true;
}


/*
 * multidb_context_close_all closes all active cache entries.
 */
bool
multidb_context_close_all(MultiDbContext *ctx)
{
	bool ok = true;

	for (int i = 0; i < MULTIDB_ENTRY_CACHE_MAX; i++)
	{
		if (ctx->entries[i].active)
		{
			if (!multidb_context_close_entry(&ctx->entries[i]))
				ok = false;
		}
	}

	return ok;
}


/*
 * multidb_init_entry initialises a MultiDbEntry for the given database.
 *
 * This involves:
 *  1. Finding the MultiDbInfo for datname in parentSpecs
 *  2. Allocating a per-database CopyDataSpec
 *  3. Opening the per-database source catalog
 *  4. Importing the snapshot on the source connection
 *  5. Connecting to the target database
 */
static bool
multidb_init_entry(MultiDbEntry *entry, CopyDataSpec *parentSpecs,
				   const char *datname)
{
	/* find the MultiDbInfo for this database */
	MultiDbInfo *info = NULL;

	for (int i = 0; i < parentSpecs->multiDbCount; i++)
	{
		if (strcmp(parentSpecs->multiDbInfos[i].datname, datname) == 0)
		{
			info = &parentSpecs->multiDbInfos[i];
			break;
		}
	}

	if (info == NULL)
	{
		log_error("BUG: multidb_init_entry: no MultiDbInfo for database \"%s\"",
				  datname);
		return false;
	}

	log_debug("Opening per-database connection for \"%s\"", datname);

	/* allocate a per-database CopyDataSpec on the heap */
	CopyDataSpec *dbSpecs = (CopyDataSpec *) calloc(1, sizeof(CopyDataSpec));

	if (dbSpecs == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* copy settings from parent */
	dbSpecs->pgPaths = parentSpecs->pgPaths;
	dbSpecs->filters = parentSpecs->filters;
	dbSpecs->extRequirements = parentSpecs->extRequirements;
	dbSpecs->section = DATA_SECTION_TABLE_DATA;  /* skip inline index routing */
	dbSpecs->restoreOptions = parentSpecs->restoreOptions;
	dbSpecs->roles = false;
	dbSpecs->skipLargeObjects = parentSpecs->skipLargeObjects;
	dbSpecs->skipExtensions = parentSpecs->skipExtensions;
	dbSpecs->skipCommentOnExtension = parentSpecs->skipCommentOnExtension;
	dbSpecs->skipCollations = parentSpecs->skipCollations;
	dbSpecs->skipVacuum = true;    /* vacuum done separately in post-data */
	dbSpecs->skipAnalyze = parentSpecs->skipAnalyze;
	dbSpecs->skipDBproperties = parentSpecs->skipDBproperties;
	dbSpecs->skipCtidSplit = parentSpecs->skipCtidSplit;
	dbSpecs->noRolesPasswords = parentSpecs->noRolesPasswords;
	dbSpecs->failFast = parentSpecs->failFast;
	dbSpecs->useCopyBinary = parentSpecs->useCopyBinary;
	dbSpecs->restart = false;
	dbSpecs->resume = parentSpecs->resume;
	dbSpecs->consistent = parentSpecs->consistent;
	dbSpecs->fetchCatalogs = parentSpecs->fetchCatalogs;
	dbSpecs->fetchFilteredOids = parentSpecs->fetchFilteredOids;
	dbSpecs->tableJobs = parentSpecs->tableJobs;
	dbSpecs->indexJobs = parentSpecs->indexJobs;
	dbSpecs->vacuumJobs = parentSpecs->vacuumJobs;
	dbSpecs->lObjectJobs = parentSpecs->lObjectJobs;
	dbSpecs->splitTablesLargerThan = parentSpecs->splitTablesLargerThan;
	dbSpecs->splitMaxParts = parentSpecs->splitMaxParts;
	dbSpecs->estimateTableSizes = parentSpecs->estimateTableSizes;
	dbSpecs->allDatabases = false;  /* per-db context, not the global flag */

	/* no queues for per-database workers — index/vacuum done in post-data */
	dbSpecs->copyQueue.qId = -1;
	dbSpecs->indexQueue.qId = -1;
	dbSpecs->vacuumQueue.qId = -1;
	dbSpecs->loQueue.qId = -1;

	/* set up per-database connection strings (heap-allocated) */
	dbSpecs->connStrings.source_pguri = strdup(info->source_pguri);
	dbSpecs->connStrings.target_pguri = strdup(info->target_pguri);

	if (dbSpecs->connStrings.source_pguri == NULL ||
		dbSpecs->connStrings.target_pguri == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(dbSpecs);
		return false;
	}

	if (!parse_and_scrub_connection_string(dbSpecs->connStrings.source_pguri,
										   &dbSpecs->connStrings.safeSourcePGURI))
	{
		log_error("Failed to scrub source URI for database \"%s\"", datname);
		free(dbSpecs->connStrings.source_pguri);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	if (!parse_and_scrub_connection_string(dbSpecs->connStrings.target_pguri,
										   &dbSpecs->connStrings.safeTargetPGURI))
	{
		log_error("Failed to scrub target URI for database \"%s\"", datname);
		free(dbSpecs->connStrings.source_pguri);
		free(dbSpecs->connStrings.target_pguri);
		if (dbSpecs->connStrings.safeSourcePGURI.pguri)
			free(dbSpecs->connStrings.safeSourcePGURI.pguri);
		free(dbSpecs);
		return false;
	}

	/* set up the per-database work directory paths */
	if (!copydb_init_workdir(dbSpecs, info->topdir,
							 false,  /* service */
							 NULL,   /* serviceName */
							 false,  /* restart */
							 true,   /* resume */
							 false)) /* createWorkDir (already created) */
	{
		log_error("Failed to init work dir for database \"%s\"", datname);
		free(dbSpecs->connStrings.source_pguri);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	/* set up catalog file paths */
	DatabaseCatalog *sourceDB = &dbSpecs->catalogs.source;
	DatabaseCatalog *filterDB = &dbSpecs->catalogs.filter;
	DatabaseCatalog *targetDB = &dbSpecs->catalogs.target;

	sourceDB->type = DATABASE_CATALOG_TYPE_SOURCE;
	filterDB->type = DATABASE_CATALOG_TYPE_FILTER;
	targetDB->type = DATABASE_CATALOG_TYPE_TARGET;

	strlcpy(sourceDB->dbfile, dbSpecs->cfPaths.sdbfile, sizeof(sourceDB->dbfile));
	strlcpy(filterDB->dbfile, dbSpecs->cfPaths.fdbfile, sizeof(filterDB->dbfile));
	strlcpy(targetDB->dbfile, dbSpecs->cfPaths.tdbfile, sizeof(targetDB->dbfile));

	/*
	 * Fix A: wire the shared catalog semaphore so catalog_create_semaphore()
	 * finds semId != 0 and reuses it rather than creating a fresh independent
	 * semaphore.  All workers for the same database thus share one lock,
	 * serialising SQLite writes and eliminating SQLITE_BUSY contention.
	 */
	if (info->catalogSemId != 0)
	{
		sourceDB->sema.semId = info->catalogSemId;
		sourceDB->sema.reentrant = true;
	}

	/* open the per-database source catalog (populated by pre-data) */
	if (!catalog_init(sourceDB))
	{
		log_error("Failed to open catalog for database \"%s\"", datname);
		free(dbSpecs->connStrings.source_pguri);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	/* set up the snapshot for the source connection */
	dbSpecs->sourceSnapshot.pguri = dbSpecs->connStrings.source_pguri;
	dbSpecs->sourceSnapshot.safeURI = dbSpecs->connStrings.safeSourcePGURI;
	dbSpecs->sourceSnapshot.connectionType = PGSQL_CONN_SOURCE;
	dbSpecs->sourceSnapshot.isReadOnly = info->isReadOnly;

	strlcpy(dbSpecs->sourceSnapshot.snapshot, info->snapshot,
			sizeof(dbSpecs->sourceSnapshot.snapshot));

	/* connect to source and import the exported snapshot */
	if (!copydb_set_snapshot(dbSpecs))
	{
		log_error("Failed to import snapshot for database \"%s\"", datname);
		catalog_close(sourceDB);
		free(dbSpecs->connStrings.source_pguri);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	/* connect to target database */
	if (!pgsql_init(&entry->dst, dbSpecs->connStrings.target_pguri,
					PGSQL_CONN_TARGET))
	{
		log_error("Failed to connect to target for database \"%s\"", datname);
		copydb_close_snapshot(dbSpecs);
		catalog_close(sourceDB);
		free(dbSpecs->connStrings.source_pguri);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	/* set GUCs on target connection */
	if (!pgsql_set_gucs(&entry->dst, dstSettings))
	{
		log_warn("Failed to set GUCs on target for database \"%s\"", datname);
		/* non-fatal: continue */
	}

	/* populate the entry */
	strlcpy(entry->datname, datname, sizeof(entry->datname));
	entry->dbSpecs = dbSpecs;
	entry->active = true;

	log_debug("Opened connections for database \"%s\"", datname);

	return true;
}


/*
 * multidb_is_system_database returns true for template0 and template1.
 */
static bool
multidb_is_system_database(const char *datname)
{
	return strcmp(datname, "template0") == 0 ||
		   strcmp(datname, "template1") == 0;
}


/*
 * multidb_target_database_exists checks whether datname already exists on the
 * target via pg_database.
 */
static bool
multidb_target_database_exists(PGSQL *dst, const char *datname, bool *exists)
{
	SingleValueResultContext ctx = { { 0 }, PGSQL_RESULT_BOOL, false };

	const char *sql =
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)";

	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { datname };

	if (!pgsql_execute_with_params(dst, sql,
								   paramCount, paramTypes, paramValues,
								   &ctx, &parseSingleValueResult))
	{
		log_error("Failed to check existence of database \"%s\" on target",
				  datname);
		return false;
	}

	if (!ctx.parsedOk)
	{
		log_error("Failed to parse result for database existence check");
		return false;
	}

	*exists = ctx.boolVal;
	return true;
}


/*
 * multidb_create_target_database creates datname on the target instance.
 */
static bool
multidb_create_target_database(PGSQL *dst, const char *datname,
								bool dropIfExists)
{
	bool exists = false;

	if (!multidb_target_database_exists(dst, datname, &exists))
		return false;

	if (exists)
	{
		if (!dropIfExists)
		{
			log_info("Database \"%s\" already exists on target, skipping CREATE",
					 datname);
			return true;
		}

		/* need a live connection to call PQescapeIdentifier */
		PGconn *conn = pgsql_open_connection(dst);

		if (conn == NULL)
			return false;

		char *quotedName = PQescapeIdentifier(conn, datname, strlen(datname));

		if (quotedName == NULL)
		{
			log_error("Failed to quote database name \"%s\": %s",
					  datname, PQerrorMessage(conn));
			pgsql_finish(dst);
			return false;
		}

		char sql[BUFSIZE] = { 0 };
		sformat(sql, sizeof(sql), "DROP DATABASE %s", quotedName);
		PQfreemem(quotedName);

		pgsql_finish(dst);

		log_info("Dropping existing database \"%s\" on target", datname);

		if (!pgsql_execute(dst, sql))
		{
			log_error("Failed to drop database \"%s\" on target", datname);
			return false;
		}
	}

	{
		PGconn *conn = pgsql_open_connection(dst);

		if (conn == NULL)
			return false;

		char *quotedName = PQescapeIdentifier(conn, datname, strlen(datname));

		if (quotedName == NULL)
		{
			log_error("Failed to quote database name \"%s\": %s",
					  datname, PQerrorMessage(conn));
			pgsql_finish(dst);
			return false;
		}

		char sql[BUFSIZE] = { 0 };
		sformat(sql, sizeof(sql),
				"CREATE DATABASE %s TEMPLATE template0", quotedName);
		PQfreemem(quotedName);

		pgsql_finish(dst);

		log_info("Creating database \"%s\" on target", datname);

		if (!pgsql_execute(dst, sql))
		{
			log_error("Failed to create database \"%s\" on target", datname);
			return false;
		}
	}

	return true;
}


/*
 * multidb_build_uri_for_database takes an existing Postgres URI and returns a
 * new one with the dbname component replaced by datname.
 */
static bool
multidb_build_uri_for_database(const char *pguri, const char *datname,
								char **result_uri)
{
	KeyVal noDefaults = { 0 };
	KeyVal noOverrides = { 0 };
	URIParams params = { 0 };

	if (!parse_pguri_info_key_vals(pguri, &noDefaults, &noOverrides,
								   &params, false))
	{
		log_error("Failed to parse URI for database \"%s\" replacement",
				  datname);
		return false;
	}

	/* replace the dbname component */
	free(params.dbname);
	params.dbname = strdup(datname);

	if (params.dbname == NULL)
	{
		log_error("Failed to strdup datname \"%s\"", datname);
		return false;
	}

	bool ok = buildPostgresURIfromPieces(&params, result_uri);

	free(params.username);
	free(params.hostname);
	free(params.port);
	free(params.dbname);

	for (int i = 0; i < params.parameters.count; i++)
	{
		free(params.parameters.keywords[i]);
		free(params.parameters.values[i]);
	}

	return ok;
}


/*
 * multidb_build_conn_strings constructs per-database ConnStrings.
 */
static bool
multidb_build_conn_strings(ConnStrings *parent, const char *datname,
						   ConnStrings *dbConnStrings)
{
	if (!multidb_build_uri_for_database(parent->source_pguri, datname,
										&dbConnStrings->source_pguri))
	{
		log_error("Failed to build source URI for database \"%s\"", datname);
		return false;
	}

	if (!multidb_build_uri_for_database(parent->target_pguri, datname,
										&dbConnStrings->target_pguri))
	{
		log_error("Failed to build target URI for database \"%s\"", datname);
		free(dbConnStrings->source_pguri);
		dbConnStrings->source_pguri = NULL;
		return false;
	}

	if (!parse_and_scrub_connection_string(dbConnStrings->source_pguri,
										   &dbConnStrings->safeSourcePGURI))
	{
		log_error("Failed to scrub source URI for database \"%s\"", datname);
		free(dbConnStrings->source_pguri);
		free(dbConnStrings->target_pguri);
		return false;
	}

	if (!parse_and_scrub_connection_string(dbConnStrings->target_pguri,
										   &dbConnStrings->safeTargetPGURI))
	{
		log_error("Failed to scrub target URI for database \"%s\"", datname);
		free(dbConnStrings->source_pguri);
		free(dbConnStrings->target_pguri);
		if (dbConnStrings->safeSourcePGURI.pguri)
			free(dbConnStrings->safeSourcePGURI.pguri);
		return false;
	}

	return true;
}


/*
 * multidb_init_db_specs initialises a per-database CopyDataSpec by copying
 * all settings from the parent and replacing the connection strings.
 */
static bool
multidb_init_db_specs(CopyDataSpec *dbSpecs,
					  CopyDataSpec *parent,
					  ConnStrings *connStrings)
{
	/* connection strings */
	dbSpecs->connStrings = *connStrings;

	dbSpecs->sourceSnapshot.pguri = dbSpecs->connStrings.source_pguri;
	dbSpecs->sourceSnapshot.safeURI = dbSpecs->connStrings.safeSourcePGURI;
	dbSpecs->sourceSnapshot.connectionType = PGSQL_CONN_SOURCE;

	/* copy all operational settings from parent */
	dbSpecs->section = parent->section;
	dbSpecs->restoreOptions = parent->restoreOptions;
	dbSpecs->roles = false;
	dbSpecs->skipLargeObjects = parent->skipLargeObjects;
	dbSpecs->skipExtensions = parent->skipExtensions;
	dbSpecs->skipCommentOnExtension = parent->skipCommentOnExtension;
	dbSpecs->skipCollations = parent->skipCollations;
	dbSpecs->skipVacuum = parent->skipVacuum;
	dbSpecs->skipAnalyze = parent->skipAnalyze;
	dbSpecs->skipDBproperties = parent->skipDBproperties;
	dbSpecs->skipCtidSplit = parent->skipCtidSplit;
	dbSpecs->noRolesPasswords = parent->noRolesPasswords;
	dbSpecs->failFast = parent->failFast;
	dbSpecs->useCopyBinary = parent->useCopyBinary;
	dbSpecs->restart = parent->restart;
	dbSpecs->resume = parent->resume;
	dbSpecs->consistent = parent->consistent;
	dbSpecs->fetchCatalogs = parent->fetchCatalogs;
	dbSpecs->fetchFilteredOids = parent->fetchFilteredOids;
	dbSpecs->tableJobs = parent->tableJobs;
	dbSpecs->indexJobs = parent->indexJobs;
	dbSpecs->vacuumJobs = parent->vacuumJobs;
	dbSpecs->lObjectJobs = parent->lObjectJobs;
	dbSpecs->splitTablesLargerThan = parent->splitTablesLargerThan;
	dbSpecs->splitMaxParts = parent->splitMaxParts;
	dbSpecs->estimateTableSizes = parent->estimateTableSizes;
	dbSpecs->extRequirements = parent->extRequirements;

	/* filters are shared — pointer copy (read-only in workers) */
	dbSpecs->filters = parent->filters;

	/* prepare dump paths under the per-db schema dir */
	if (!copydb_prepare_dump_paths(&dbSpecs->cfPaths, &dbSpecs->dumpPaths))
	{
		log_error("Failed to prepare dump paths for per-database work dir");
		return false;
	}

	/* initialise catalog types and file paths */
	DatabaseCatalog *source = &dbSpecs->catalogs.source;
	DatabaseCatalog *filter = &dbSpecs->catalogs.filter;
	DatabaseCatalog *target = &dbSpecs->catalogs.target;

	source->type = DATABASE_CATALOG_TYPE_SOURCE;
	filter->type = DATABASE_CATALOG_TYPE_FILTER;
	target->type = DATABASE_CATALOG_TYPE_TARGET;

	strlcpy(source->dbfile, dbSpecs->cfPaths.sdbfile, sizeof(source->dbfile));
	strlcpy(filter->dbfile, dbSpecs->cfPaths.fdbfile, sizeof(filter->dbfile));
	strlcpy(target->dbfile, dbSpecs->cfPaths.tdbfile, sizeof(target->dbfile));

	/* create System V message queues */
	bool shouldCreateVacuumQueue =
		(dbSpecs->section == DATA_SECTION_ALL ||
		 dbSpecs->section == DATA_SECTION_INDEXES ||
		 dbSpecs->section == DATA_SECTION_TABLE_DATA) &&
		!dbSpecs->skipVacuum;

	if (shouldCreateVacuumQueue)
	{
		if (!queue_create(&dbSpecs->vacuumQueue, "vacuum"))
		{
			log_error("Failed to create the VACUUM process queue");
			return false;
		}
	}
	else
	{
		dbSpecs->vacuumQueue.qId = -1;
	}

	if (dbSpecs->section == DATA_SECTION_ALL ||
		dbSpecs->section == DATA_SECTION_INDEXES ||
		dbSpecs->section == DATA_SECTION_CONSTRAINTS ||
		dbSpecs->section == DATA_SECTION_TABLE_DATA)
	{
		if (!queue_create(&dbSpecs->indexQueue, "create index"))
		{
			log_error("Failed to create the INDEX process queue");
			return false;
		}
	}
	else
	{
		dbSpecs->indexQueue.qId = -1;
	}

	return true;
}


/*
 * multidb_wait_child blocks until the given subprocess exits and returns
 * true iff it exited with EXIT_CODE_QUIT (0).  The label is used only in
 * log messages (it may be a database name or a supervisor description).
 */
static bool
multidb_wait_child(pid_t pid, const char *label)
{
	int status = 0;
	pid_t result = waitpid(pid, &status, 0);

	if (result < 0)
	{
		log_error("Failed to wait for subprocess %d (%s): %m", pid, label);
		return false;
	}

	if (!WIFEXITED(status))
	{
		int sig = WTERMSIG(status);
		log_error("Subprocess %d (%s) terminated by signal %d",
				  pid, label, sig);
		return false;
	}

	int returnCode = WEXITSTATUS(status);

	if (returnCode != 0)
	{
		log_error("Subprocess %d (%s) exited with status %d",
				  pid, label, returnCode);
		return false;
	}

	return true;
}
