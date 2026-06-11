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

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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
#include "summary.h"
#include "signals.h"
#include "string_utils.h"


static bool multidb_is_system_database(const char *datname);
static bool multidb_target_database_exists(PGSQL *dst, const char *datname,
										   bool *exists);
static bool multidb_create_target_database(PGSQL *dst, const char *datname,
										   bool dropIfExists);
bool multidb_build_uri_for_database(const char *pguri,
									const char *datname,
									char **result_uri);
static bool multidb_init_db_specs(CopyDataSpec *dbSpecs,
								  CopyDataSpec *parent,
								  ConnStrings *connStrings);

/*
 * Phase I: snapshot holder + parallel pre-data workers.
 *
 * The snapshot holder is a long-lived direct child of clone_all_databases.
 * It exports one REPEATABLE READ snapshot per database, writes the snapshot
 * IDs into the instance catalog, enqueues DBNAME messages, and then waits
 * until the parent signals it is done (by closing the write end of donepipe).
 *
 * Pre-data workers are forked by the supervisor (after the snapshot holder
 * has signalled "ready").  They import the held snapshot via SET TRANSACTION
 * SNAPSHOT to do consistent schema fetch + pg_dump + pg_restore --pre-data.
 */
static bool multidb_snapshot_holder(CopyDataSpec *parentSpecs,
									int readyfd, int donefd);
static bool multidb_pre_data_supervisor(CopyDataSpec *parentSpecs);
static bool multidb_pre_data_worker(CopyDataSpec *parentSpecs);
static bool multidb_pre_data_one_db(CopyDataSpec *parentSpecs,
									const char *datname);

static bool multidb_clone_one_database_post_data(CopyDataSpec *parentSpecs,
												 const char *datname);
static bool multidb_global_copy(CopyDataSpec *parentSpecs);
static bool multidb_wait_child(pid_t pid, const char *label);

/* connection cache helpers (also called from table-data.c) */
static bool multidb_init_entry(MultiDbEntry *entry,
							   CopyDataSpec *parentSpecs,
							   const char *datname);

/* lighter index/vacuum pool helpers */
static bool multidb_init_index_entry(MultiDbEntry *entry,
									 CopyDataSpec *parentSpecs,
									 const char *datname);
static bool multidb_index_context_close_entry(MultiDbEntry *entry);


/*
 * clone_all_databases clones all user databases from a source Postgres
 * instance in three phases:
 *
 *  Phase I  — per-database pre-data (parallel, bounded by --table-jobs)
 *  Phase II — global COPY with shared worker pool sorted by table size
 *  Phase III — per-database post-data (sequential subprocesses)
 *
 * Snapshot design
 * ---------------
 * A long-lived "snapshot holder" subprocess is forked as a direct child.
 * It connects to each per-database source URI, exports a REPEATABLE READ
 * snapshot, writes the snapshot ID into the instance catalog, enqueues
 * DBNAME messages for Phase I workers, and then blocks on a "done" pipe.
 *
 * After the holder signals "ready" (via readypipe), the parent re-reads the
 * instance catalog to build multiDbInfos[] with snapshot IDs, then forks
 * the Phase I supervisor.  Phase I and II workers import those snapshots
 * using SET TRANSACTION SNAPSHOT — the holder's transactions are still live.
 *
 * After Phase III the parent closes the write end of donepipe; the holder
 * gets EOF, commits all snapshot transactions, and exits.
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
	 * Initialise the instance-level catalog schema on disk.  The snapshot
	 * holder subprocess will write the database list and snapshot IDs into it.
	 */
	if (!catalog_init_from_specs(parentSpecs))
	{
		log_error("Failed to initialise the instance-level catalog");
		return false;
	}

	/* copy instance-level roles first (single connection to instance) */
	log_info("Copying instance-level roles");

	if (!pg_copy_roles(&parentSpecs->pgPaths,
					   &parentSpecs->connStrings,
					   parentSpecs->dumpPaths.rolesFilename,
					   parentSpecs->noRolesPasswords))
	{
		log_error("Failed to copy roles from source instance");
		(void) catalog_close_from_specs(parentSpecs);
		return false;
	}

	/*
	 * Close the catalog before forking: the snapshot holder subprocess will
	 * be the sole writer; we re-open read-only after it signals "ready".
	 */
	if (!catalog_close_from_specs(parentSpecs))
	{
		log_error("Failed to close instance catalog before forking");
		return false;
	}

	/*
	 * Create the Phase I queue before forking the snapshot holder so that the
	 * holder inherits the qId via COW and can send DBNAME messages directly.
	 */
	if (!queue_create(&parentSpecs->preDataQueue, "pre-data"))
	{
		log_error("Failed to create the pre-data process queue");
		return false;
	}

	/*
	 * Two pipes coordinate the snapshot holder with the parent.
	 *
	 *  readypipe  — holder writes one byte after all snapshots are exported
	 *               and all snapshot IDs are in the catalog.  Parent blocks
	 *               here until the holder is ready.
	 *
	 *  donepipe   — parent holds the write end open throughout Phase I, II,
	 *               and III.  Closing it sends EOF to the holder, which then
	 *               commits all snapshot transactions and exits.
	 *
	 * FD_CLOEXEC prevents the pipe descriptors from being inherited by
	 * exec'd children (pg_dump, pg_restore).
	 */
	int readypipe[2] = { -1, -1 };
	int donepipe[2] = { -1, -1 };

	if (pipe(readypipe) < 0 || pipe(donepipe) < 0)
	{
		log_error("Failed to create synchronisation pipes: %m");
		(void) queue_unlink(&parentSpecs->preDataQueue);
		return false;
	}

	for (int i = 0; i < 2; i++)
	{
		(void) fcntl(readypipe[i], F_SETFD, FD_CLOEXEC);
		(void) fcntl(donepipe[i], F_SETFD, FD_CLOEXEC);
	}

	fflush(stdout);
	fflush(stderr);

	pid_t holderPid = fork();

	switch (holderPid)
	{
		case -1:
		{
			log_error("Failed to fork snapshot holder: %m");
			close(readypipe[0]);
			close(readypipe[1]);
			close(donepipe[0]);
			close(donepipe[1]);
			(void) queue_unlink(&parentSpecs->preDataQueue);
			return false;
		}

		case 0:
		{
			/* ====== snapshot holder subprocess ====== */
			close(readypipe[0]);  /* not reading from ready pipe */
			close(donepipe[1]);   /* not writing to done pipe */
			(void) set_ps_title("pgcopydb: snapshot holder");

			bool res = multidb_snapshot_holder(parentSpecs,
											   readypipe[1],
											   donepipe[0]);
			exit(res ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
		}

		default:
		{
			break;
		}
	}

	/* parent: close pipe ends not needed here */
	close(readypipe[1]);
	close(donepipe[0]);

	/*
	 * Block until the snapshot holder has exported all per-database snapshots,
	 * written their IDs to the instance catalog, and enqueued the DBNAME
	 * messages.  Only then can we safely re-read the catalog.
	 */
	{
		char ready = 0;
		ssize_t n = read(readypipe[0], &ready, 1);

		close(readypipe[0]);

		if (n <= 0)
		{
			log_error("Snapshot holder failed to signal ready (read=%zd): %m", n);
			(void) multidb_wait_child(holderPid, "snapshot holder");
			close(donepipe[1]);
			(void) queue_unlink(&parentSpecs->preDataQueue);
			return false;
		}

		log_debug("Received ready signal from snapshot holder");
	}

	/*
	 * Re-open the instance catalog — the snapshot holder has populated it with
	 * the database list and per-database snapshot IDs.  Build multiDbInfos[].
	 */
	bool ok = true;
	MultiDbInfo *multiDbInfos = NULL;
	int dbCount = 0;

	if (!catalog_init_from_specs(parentSpecs))
	{
		log_error("Failed to re-open instance catalog after snapshot holder ready");
		ok = false;
		goto signal_done;
	}

	{
		DatabaseCatalog *instanceCatalog = &(parentSpecs->catalogs.source);
		CatalogCounts counts = { 0 };

		if (!catalog_count_objects(instanceCatalog, &counts))
		{
			log_error("Failed to count databases in instance catalog");
			(void) catalog_close_from_specs(parentSpecs);
			ok = false;
			goto signal_done;
		}

		int maxDbs = (int) counts.databases;

		if (maxDbs == 0)
		{
			log_info("No databases found on the source instance");
			(void) catalog_close_from_specs(parentSpecs);
			goto signal_done;
		}

		multiDbInfos = (MultiDbInfo *) calloc(maxDbs, sizeof(MultiDbInfo));

		if (multiDbInfos == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			(void) catalog_close_from_specs(parentSpecs);
			ok = false;
			goto signal_done;
		}

		SourceDatabaseIterator dbIter = {
			.catalog = instanceCatalog,
			.dat = NULL
		};

		if (!catalog_iter_s_database_init(&dbIter))
		{
			(void) catalog_close_from_specs(parentSpecs);
			ok = false;
			goto signal_done;
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
			{
				break;        /* end of results */
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

			log_info("Found database \"%s\" (%s) snapshot \"%s\"",
					 db->datname, db->bytesPretty, db->snapshot);

			MultiDbInfo *info = &multiDbInfos[dbCount++];

			strlcpy(info->datname, db->datname, sizeof(info->datname));
			strlcpy(info->snapshot, db->snapshot, sizeof(info->snapshot));

			sformat(info->topdir, sizeof(info->topdir), "%s/db/%s",
					parentSpecs->cfPaths.topdir, db->datname);

			char *srcuri = NULL;
			char *tgturi = NULL;

			if (!multidb_build_uri_for_database(
					parentSpecs->connStrings.source_pguri,
					db->datname, &srcuri) ||
				!multidb_build_uri_for_database(
					parentSpecs->connStrings.target_pguri,
					db->datname, &tgturi))
			{
				log_error("Failed to build URIs for database \"%s\"", db->datname);
				free(srcuri);
				free(tgturi);
				ok = false;
				break;
			}

			strlcpy(info->source_pguri, srcuri, sizeof(info->source_pguri));
			strlcpy(info->target_pguri, tgturi, sizeof(info->target_pguri));
			free(srcuri);
			free(tgturi);
		}

		if (!catalog_iter_s_database_finish(&dbIter))
		{
			ok = false;
		}

		(void) catalog_close_from_specs(parentSpecs);
	}

	if (!ok || dbCount == 0)
	{
		if (ok && dbCount == 0)
		{
			log_warn("No user databases found after filtering");
		}
		goto signal_done;
	}

	/*
	 * Publish the per-database info so that the Phase I and II supervisors
	 * (forked below) can access it via the COW post-fork copy.
	 */
	parentSpecs->multiDbCount = dbCount;
	parentSpecs->multiDbInfos = multiDbInfos;

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
				goto signal_done;
			}

			case 0:
			{
				bool res = multidb_pre_data_supervisor(parentSpecs);
				exit(res ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
			}

			default:
			{
				break;
			}
		}

		if (!multidb_wait_child(prePid, "pre-data supervisor"))
		{
			ok = false;
			goto signal_done;
		}

		/*
		 * The pre-data supervisor already called queue_unlink on
		 * preDataQueue before it exited.  Mark it as unlinked in the
		 * parent's system_res_array so that the atexit/signal cleanup
		 * handler does not attempt a second IPC_RMID and log a
		 * spurious "Invalid argument" error.
		 */
		(void) copydb_unlink_sysv_queue(&system_res_array,
										&parentSpecs->preDataQueue);
		parentSpecs->preDataQueue.qId = -1;
	}

	/* ====== PHASE II: GLOBAL COPY ====== */
	log_info("Phase II: global COPY for %d databases (largest tables first)",
			 dbCount);

	/*
	 * Open the instance catalog to record Phase II wall-clock timing.
	 * The catalog was closed after reading multiDbInfos; re-open it now.
	 */
	if (!catalog_init_from_specs(parentSpecs))
	{
		log_error("Failed to open instance catalog for Phase II timing");
		ok = false;
		goto signal_done;
	}

	(void) summary_start_timing(&parentSpecs->catalogs.source,
								TIMING_SECTION_TOTAL_DATA);

	if (!multidb_global_copy(parentSpecs))
	{
		log_error("Global COPY phase failed");
		(void) summary_stop_timing(&parentSpecs->catalogs.source,
								   TIMING_SECTION_TOTAL_DATA);
		(void) catalog_close_from_specs(parentSpecs);
		ok = false;
		goto signal_done;
	}

	(void) summary_stop_timing(&parentSpecs->catalogs.source,
							   TIMING_SECTION_TOTAL_DATA);

	(void) catalog_close_from_specs(parentSpecs);

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

		log_info("Post-data for database \"%s\"", multiDbInfos[i].datname);

		if (!multidb_clone_one_database_post_data(parentSpecs,
												  multiDbInfos[i].datname))
		{
			log_error("Post-data failed for database \"%s\"",
					  multiDbInfos[i].datname);
			ok = false;
			if (parentSpecs->failFast)
			{
				break;
			}
		}
	}

signal_done:

	/*
	 * Close the write end of donepipe, signalling the snapshot holder to commit
	 * all its REPEATABLE READ transactions and exit.
	 */
	close(donepipe[1]);

	if (!multidb_wait_child(holderPid, "snapshot holder"))
	{
		ok = false;
	}

	parentSpecs->multiDbInfos = NULL;
	parentSpecs->multiDbCount = 0;

	free(multiDbInfos);

	return ok;
}


/*
 * multidb_snapshot_holder is a long-lived subprocess (direct child of
 * clone_all_databases) that:
 *
 *  1. Opens the instance catalog and calls schema_list_databases().
 *  2. For each user database, exports a per-database REPEATABLE READ snapshot
 *     and writes the snapshot ID back to the instance catalog.
 *  3. Sends one QMSG_TYPE_DBNAME message per database to parentSpecs->preDataQueue,
 *     followed by tableJobs QMSG_TYPE_STOP messages.
 *  4. Signals the parent by writing one byte to readyfd, then closes readyfd.
 *  5. Blocks on donefd (EOF from parent) while holding all snapshot transactions
 *     open so that Phase I and II workers can import them.
 *  6. On EOF: commits all snapshot transactions and exits.
 *
 * Process tree context:
 *   clone_all_databases
 *   ├── snapshot holder  ← this function (long-lived)
 *   ├── pre-data supervisor
 *   └── global copy supervisor
 */
static bool
multidb_snapshot_holder(CopyDataSpec *parentSpecs, int readyfd, int donefd)
{
	log_notice("Started snapshot holder %d [%d]", getpid(), getppid());

	/* open the instance catalog for writing */
	if (!catalog_init_from_specs(parentSpecs))
	{
		log_error("Snapshot holder: failed to open instance catalog");
		return false;
	}

	DatabaseCatalog *instanceCatalog = &(parentSpecs->catalogs.source);

	/* connect to source instance and list databases */
	PGSQL src = { 0 };

	if (!pgsql_init(&src, parentSpecs->connStrings.source_pguri,
					PGSQL_CONN_SOURCE))
	{
		log_error("Snapshot holder: failed to connect to source instance");
		(void) catalog_close_from_specs(parentSpecs);
		return false;
	}

	if (!schema_list_databases(&src, instanceCatalog))
	{
		log_error("Snapshot holder: failed to list databases");
		pgsql_finish(&src);
		(void) catalog_close_from_specs(parentSpecs);
		return false;
	}

	pgsql_finish(&src);

	/* count non-system databases */
	CatalogCounts counts = { 0 };

	if (!catalog_count_objects(instanceCatalog, &counts))
	{
		log_error("Snapshot holder: failed to count databases");
		(void) catalog_close_from_specs(parentSpecs);
		return false;
	}

	int maxDbs = (int) counts.databases;

	/*
	 * Allocate per-database snapshot structs and source URI strings.
	 * The snapshots stay open until the parent signals "done".
	 */
	TransactionSnapshot *snapshots =
		(TransactionSnapshot *) calloc(maxDbs, sizeof(TransactionSnapshot));
	char **sourceUris =
		(char **) calloc(maxDbs, sizeof(char *));

	if (snapshots == NULL || sourceUris == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(snapshots);
		free(sourceUris);
		(void) catalog_close_from_specs(parentSpecs);
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
		ok = false;
		goto cleanup_snapshots;
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
		{
			break;
		}

		if (multidb_is_system_database(db->datname))
		{
			log_debug("Snapshot holder: skipping system database \"%s\"",
					  db->datname);
			continue;
		}

		if (dbCount >= maxDbs)
		{
			log_error("Snapshot holder: more databases than expected (%d)",
					  maxDbs);
			ok = false;
			break;
		}

		/* build per-database source URI */
		if (!multidb_build_uri_for_database(
				parentSpecs->connStrings.source_pguri,
				db->datname, &sourceUris[dbCount]))
		{
			log_error("Snapshot holder: failed to build source URI "
					  "for database \"%s\"", db->datname);
			ok = false;
			break;
		}

		/* export a REPEATABLE READ snapshot for this database */
		TransactionSnapshot *snap = &snapshots[dbCount];

		snap->pguri = sourceUris[dbCount];
		snap->connectionType = PGSQL_CONN_SOURCE;

		if (!copydb_export_snapshot(snap))
		{
			log_error("Snapshot holder: failed to export snapshot "
					  "for database \"%s\"", db->datname);
			ok = false;
			break;
		}

		log_info("Snapshot holder: database \"%s\" snapshot \"%s\"",
				 db->datname, snap->snapshot);

		/* persist snapshot ID in the instance catalog */
		if (!catalog_update_s_database_snapshot(instanceCatalog,
												db->datname,
												snap->snapshot))
		{
			log_error("Snapshot holder: failed to record snapshot "
					  "for database \"%s\"", db->datname);
			ok = false;
			break;
		}

		/* send DBNAME message to Phase I queue */
		QMessage mesg = { .type = QMSG_TYPE_DBNAME };
		strlcpy(mesg.data.datname, db->datname, sizeof(mesg.data.datname));

		if (!queue_send(&parentSpecs->preDataQueue, &mesg))
		{
			log_error("Snapshot holder: failed to enqueue database \"%s\"",
					  db->datname);
			ok = false;
			break;
		}

		dbCount++;
	}

	if (!catalog_iter_s_database_finish(&dbIter))
	{
		ok = false;
	}

	(void) catalog_close_from_specs(parentSpecs);

	if (!ok)
	{
		goto signal_ready;
	}

	/* send one STOP message per worker */
	for (int i = 0; i < parentSpecs->tableJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP };

		if (!queue_send(&parentSpecs->preDataQueue, &stop))
		{
			log_error("Snapshot holder: failed to send STOP to pre-data queue");
			ok = false;
			break;
		}
	}

	log_info("Snapshot holder: ready — %d databases, %d STOP messages",
			 dbCount, parentSpecs->tableJobs);

signal_ready:

	/*
	 * Signal the parent that the catalog is fully written and all DBNAME/STOP
	 * messages are in the queue.  The parent will re-open the catalog, build
	 * multiDbInfos[], and fork the Phase I supervisor.
	 *
	 * We signal ready even on failure so the parent does not block forever;
	 * the parent detects the failure via the snapshot holder's exit status.
	 */
	{
		char rdy = 'R';

		if (write(readyfd, &rdy, 1) != 1)
		{
			log_error("Failed to signal snapshot-holder ready to parent: %m");
		}
		close(readyfd);
	}

	if (!ok)
	{
		goto cleanup_snapshots;
	}

	/*
	 * Wait for the parent to close the write end of donepipe (after Phase III).
	 * While we block here, Phase I and II workers use SET TRANSACTION SNAPSHOT
	 * to import our held snapshots for consistent reads.
	 */
	{
		char buf = 0;

		/* blocks until the parent closes the write end (EOF) */
		while (read(donefd, &buf, 1) > 0)
		{ }
		close(donefd);
	}

	log_notice("Snapshot holder: received done signal, closing snapshots");

cleanup_snapshots:

	/* commit all held snapshot transactions */
	for (int i = 0; i < dbCount; i++)
	{
		if (snapshots[i].state == SNAPSHOT_STATE_EXPORTED)
		{
			if (!pgsql_commit(&snapshots[i].pgsql))
			{
				log_warn("Snapshot holder: failed to commit snapshot "
						 "for database %d", i);
			}

			pgsql_finish(&snapshots[i].pgsql);
		}

		free(sourceUris[i]);
	}

	free(snapshots);
	free(sourceUris);

	log_notice("Snapshot holder %d exiting", getpid());

	return ok;
}


/*
 * multidb_pre_data_supervisor is the supervisor for Phase I.
 *
 * By the time this runs the snapshot holder has already filled preDataQueue
 * with DBNAME and STOP messages, so the supervisor only needs to fork workers
 * and wait for them.
 *
 * Process tree:
 *   clone_all_databases
 *   └── pre-data supervisor  (this function)
 *           ├── pre-data worker 1
 *           └── pre-data worker N  (N = --table-jobs)
 */
static bool
multidb_pre_data_supervisor(CopyDataSpec *parentSpecs)
{
	(void) set_ps_title("pgcopydb: pre-data supervisor");
	log_notice("Started pre-data supervisor %d [%d]", getpid(), getppid());

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
			{
				break;
			}
		}
	}

	/* supervisor waits for all workers to drain the queue */
	bool ok = copydb_wait_for_subprocesses(parentSpecs->failFast);

	(void) queue_unlink(&parentSpecs->preDataQueue);
	parentSpecs->preDataQueue.qId = -1;

	return ok;
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
					{
						return false;
					}
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
 * database:
 *   1. CREATE DATABASE on the target (if not already present)
 *   2. Import snapshot exported by the snapshot holder
 *   3. Fetch source database schema into source.db
 *   4. pg_dump --snapshot=<id> (pre-data + post-data sections)
 *   5. Close the worker's snapshot connection (holder still holds the real one)
 *   6. pg_restore --pre-data to the target
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

	/* STEP 0: CREATE DATABASE on the target instance */
	{
		PGSQL dst = { 0 };

		if (!pgsql_init(&dst, parentSpecs->connStrings.target_pguri,
						PGSQL_CONN_TARGET))
		{
			log_error("Failed to connect to target instance for "
					  "database \"%s\"", datname);
			return false;
		}

		bool created = multidb_create_target_database(
			&dst,
			datname,
			parentSpecs->restoreOptions.dropIfExists);

		pgsql_finish(&dst);

		if (!created)
		{
			log_error("Failed to create database \"%s\" on target", datname);
			return false;
		}
	}

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
		{
			free(cs.safeSourcePGURI.pguri);
		}
		return false;
	}

	bool ok = true;

	/*
	 * Build a local CopyDataSpec for this database.
	 * copydb_init_workdir must be called before multidb_init_db_specs so
	 * that cfPaths is populated.
	 *
	 * The per-database work directory is created here (createWorkDir=true)
	 * on a first run.  On --resume, the directory already exists.
	 */
	CopyDataSpec dbSpecs = { 0 };
	dbSpecs.pgPaths = parentSpecs->pgPaths;

	bool createWorkDir = !parentSpecs->resume;

	if (!copydb_init_workdir(&dbSpecs, info->topdir,
							 false,          /* service */
							 NULL,           /* serviceName */
							 parentSpecs->restart,
							 parentSpecs->resume,
							 createWorkDir))
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

	/* set database name for enriched log messages */
	strlcpy(dbSpecs.datname, datname, sizeof(dbSpecs.datname));

	/*
	 * Set the snapshot identifier so that copydb_prepare_snapshot (called
	 * inside copydb_fetch_schema_and_prepare_specs) will call
	 * copydb_set_snapshot to import the snapshot held by the snapshot holder.
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

	/*
	 * Close the worker's SET TRANSACTION SNAPSHOT connection.  The snapshot
	 * holder's exporting connection stays open, so the snapshot remains valid
	 * for Phase II workers.
	 */
	(void) copydb_close_snapshot(&dbSpecs);

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
	{
		free(cs.safeSourcePGURI.pguri);
	}
	if (cs.safeTargetPGURI.pguri)
	{
		free(cs.safeTargetPGURI.pguri);
	}

	return ok;
}


/*
 * multidb_clone_one_database_post_data forks a subprocess that performs the
 * post-data phase for one database: pg_restore post-data and set sequences.
 *
 * This runs after the global COPY phase has completed for all databases.
 * The function is self-contained: it derives all specs from MultiDbInfo.
 */
static bool
multidb_clone_one_database_post_data(CopyDataSpec *parentSpecs,
									 const char *datname)
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
		log_error("BUG: multidb_clone_one_database_post_data: "
				  "no MultiDbInfo for database \"%s\"", datname);
		return false;
	}

	log_info("[TARGET] Post-data for database \"%s\"", datname);

	fflush(stdout);
	fflush(stderr);

	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork post-data subprocess for "
					  "database \"%s\": %m", datname);
			return false;
		}

		case 0:
		{
			/* child process */
			char psTitle[MAXPGPATH] = { 0 };
			sformat(psTitle, sizeof(psTitle), "pgcopydb: post-data %s", datname);
			(void) set_ps_title(psTitle);

			summary_reset_toplevel_timings();

			/* build per-db connection strings */
			ConnStrings cs = { 0 };

			cs.source_pguri = strdup(info->source_pguri);
			cs.target_pguri = strdup(info->target_pguri);

			if (cs.source_pguri == NULL || cs.target_pguri == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			if (!parse_and_scrub_connection_string(cs.source_pguri,
												   &cs.safeSourcePGURI) ||
				!parse_and_scrub_connection_string(cs.target_pguri,
												   &cs.safeTargetPGURI))
			{
				log_error("Failed to scrub URIs for database \"%s\"", datname);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			CopyDataSpec dbSpecs = { 0 };
			dbSpecs.pgPaths = parentSpecs->pgPaths;

			if (!copydb_init_workdir(&dbSpecs, info->topdir,
									 false,  /* service */
									 NULL,   /* serviceName */
									 false,  /* restart */
									 true,   /* resume */
									 false)) /* createWorkDir (already created) */
			{
				log_error("Failed to init work dir for database \"%s\"",
						  datname);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			if (!multidb_init_db_specs(&dbSpecs, parentSpecs, &cs))
			{
				log_error("Failed to initialise specs for database \"%s\"",
						  datname);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/*
			 * Index and vacuum operations are done by the global supervisors
			 * in Phase II.  Destroy any per-db queues that multidb_init_db_specs
			 * may have created to avoid leaking System V resources.
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

			strlcpy(dbSpecs.datname, datname, sizeof(dbSpecs.datname));

			/*
			 * The catalog was created in Phase I with a specific snapshot ID.
			 * The consistency check in catalog_init_from_specs compares
			 * dbSpecs.sourceSnapshot.snapshot against the stored value, so we
			 * must populate it here.  The post-data phase does not open a new
			 * snapshot transaction — it just needs the ID to match so the
			 * catalog opens successfully.
			 */
			strlcpy(dbSpecs.sourceSnapshot.snapshot,
					info->snapshot,
					sizeof(dbSpecs.sourceSnapshot.snapshot));

			/*
			 * Re-use the existing catalog (pre-data already populated it).
			 * copydb_fetch_schema_and_prepare_specs will detect this and
			 * return early without re-fetching from source.
			 */
			if (!copydb_fetch_schema_and_prepare_specs(&dbSpecs))
			{
				log_error("Failed to open catalog for database \"%s\"",
						  datname);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/*
			 * Indexes and vacuum were already handled by the global supervisors
			 * during the COPY phase (Phase II).  Post-data here only needs to
			 * restore the remaining objects that pg_restore --post-data covers
			 * but pgcopydb's index workers do not: triggers, rules, comments,
			 * etc.  copydb_target_finalize_schema filters out already-built
			 * indexes via copydb_objectid_has_been_processed_already so that
			 * pg_restore does not try to recreate them.
			 */

			/* restore remaining post-data (triggers, rules, …) */
			if (!copydb_target_finalize_schema(&dbSpecs))
			{
				log_error("Failed to finalize schema for database \"%s\"",
						  datname);
				exit(EXIT_CODE_TARGET);
			}

			/* set sequences to match source values captured during schema fetch */
			if (!copydb_copy_all_sequences(&dbSpecs, false))
			{
				log_error("Failed to set sequences for database \"%s\"",
						  datname);
				exit(EXIT_CODE_TARGET);
			}

			(void) catalog_close_from_specs(&dbSpecs);
			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			break;
		}
	}

	/* parent: wait for post-data subprocess */
	return multidb_wait_child(fpid, datname);
}


/*
 * multidb_global_copy runs the global COPY + index + vacuum phase.
 *
 * Process tree (mirrors singledb copydb_process_table_data):
 *   multidb_global_copy (parent)
 *   ├── global vacuum supervisor  (async; reads vacuumQueue)
 *   ├── global index supervisor   (async; reads indexQueue; sends vacuum STOP)
 *   └── global copy supervisor    (forks copy workers + queue filler; waits)
 *
 * Copy workers fill indexQueue and vacuumQueue as they finish each table,
 * exactly as in the singledb path.  Index/vacuum workers use a per-db
 * connection pool (catalog + target, no snapshot) to route to the right db.
 *
 * Fix A — shared catalog semaphores:
 *   One System V semaphore is created per database before forking.  All
 *   workers inherit the semaphore IDs via COW.  multidb_init_entry (copy
 *   workers) and multidb_init_index_entry (index/vacuum workers) wire the
 *   semaphore into DatabaseCatalog so catalog_create_semaphore() reuses it.
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

	/* Create global index and vacuum queues before any forking. */
	if (!queue_create(&parentSpecs->indexQueue, "global index"))
	{
		log_error("Failed to create the global INDEX process queue");
		return false;
	}

	if (!parentSpecs->skipVacuum)
	{
		if (!queue_create(&parentSpecs->vacuumQueue, "global vacuum"))
		{
			log_error("Failed to create the global VACUUM process queue");
			(void) queue_unlink(&parentSpecs->indexQueue);
			parentSpecs->indexQueue.qId = -1;
			return false;
		}
	}

	/*
	 * Create ONE semaphore set with one slot per database.  All workers for
	 * database[i] share slot i, serialising concurrent SQLite writes without
	 * consuming O(N) system_res_array slots — the entire set costs one entry.
	 * Workers inherit the semId via COW; catalog_init() skips creation when
	 * semId is already set.
	 */
	{
		Semaphore catalogSemaSet = { 0 };

		if (!semaphore_create_set(&catalogSemaSet, parentSpecs->multiDbCount))
		{
			log_error("Failed to create catalog semaphore set for %d databases",
					  parentSpecs->multiDbCount);
			(void) queue_unlink(&parentSpecs->indexQueue);
			parentSpecs->indexQueue.qId = -1;

			if (!parentSpecs->skipVacuum)
			{
				(void) queue_unlink(&parentSpecs->vacuumQueue);
				parentSpecs->vacuumQueue.qId = -1;
			}

			return false;
		}

		for (int i = 0; i < parentSpecs->multiDbCount; i++)
		{
			parentSpecs->multiDbInfos[i].catalogSemId = catalogSemaSet.semId;
			parentSpecs->multiDbInfos[i].catalogSemIndex = i;

			log_debug("Assigned catalog semaphore set %d[%d] to database \"%s\"",
					  catalogSemaSet.semId, i,
					  parentSpecs->multiDbInfos[i].datname);
		}
	}

	/*
	 * Pre-seed the COPY_DATA timing row for each database so that workers'
	 * summary_increment_timing calls have a valid start_time_epoch.
	 */
	for (int i = 0; i < parentSpecs->multiDbCount; i++)
	{
		MultiDbInfo *info = &parentSpecs->multiDbInfos[i];

		DatabaseCatalog dbCat = {
			.type = DATABASE_CATALOG_TYPE_SOURCE,
			.sema.semId = info->catalogSemId,
			.sema.semIndex = info->catalogSemIndex,
			.sema.reentrant = true,
		};
		sformat(dbCat.dbfile, sizeof(dbCat.dbfile),
				"%s/schema/source.db", info->topdir);

		if (catalog_init(&dbCat))
		{
			(void) summary_start_timing(&dbCat, TIMING_SECTION_COPY_DATA);
			(void) catalog_close(&dbCat);
		}
		else
		{
			log_warn("Failed to open catalog for \"%s\": "
					 "COPY_DATA start timing not recorded", info->datname);
		}
	}

	/*
	 * Close the global catalog before forking so that no child inherits an
	 * open SQLite handle (fork-safety).  The caller re-opens it after we
	 * return via catalog_open_from_specs.
	 */
	if (!catalog_close_from_specs(parentSpecs))
	{
		log_error("Failed to close global catalog before forking");
		return false;
	}

	/*
	 * Fork vacuum supervisor first (reads vacuumQueue; waits for workers).
	 * The index supervisor sends the STOP messages after all indexes are done.
	 */
	pid_t vacPid = -1;

	if (!parentSpecs->skipVacuum)
	{
		if (!vacuum_start_supervisor(parentSpecs, &vacPid))
		{
			log_error("Failed to start global vacuum supervisor");
			return false;
		}
	}

	/*
	 * Fork index supervisor (reads indexQueue; after done, sends vacuum STOP).
	 */
	pid_t idxPid = -1;

	if (!copydb_start_index_supervisor(parentSpecs, &idxPid))
	{
		log_error("Failed to start global index supervisor");
		return false;
	}

	/*
	 * Fork the global copy supervisor.  The supervisor creates the copyQueue,
	 * forks copy workers and the queue filler, then waits for them.
	 * Copy workers fill indexQueue/vacuumQueue after each table copy.
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

			if (!queue_create(&parentSpecs->copyQueue, "global copy"))
			{
				log_error("Failed to create the global COPY queue");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

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
					{
						break;
					}
				}
			}

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
					{
						break;
					}
				}
			}

			bool ok = copydb_wait_for_subprocesses(parentSpecs->failFast);

			/*
			 * All COPY workers finished; send STOP to index workers so they
			 * can drain the index queue and terminate.  Mirror what
			 * copydb_copy_supervisor() does in table-data.c.
			 */
			if (!copydb_index_workers_send_stop(parentSpecs))
			{
				log_error("Failed to send STOP messages to index workers");
				ok = false;
			}

			(void) queue_unlink(&parentSpecs->copyQueue);
			parentSpecs->copyQueue.qId = -1;

			exit(ok ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
		}

		default:
		{
			break;
		}
	}

	/*
	 * Wait for all three supervisor children using targeted waitpid() calls.
	 *
	 * We MUST NOT use copydb_wait_for_subprocesses() here because it calls
	 * waitpid(-1, ...) which would accidentally reap the snapshot holder — a
	 * sibling child process forked before multidb_global_copy() was called.
	 * The snapshot holder blocks on donepipe EOF and would never exit until
	 * clone_all_databases() closes the pipe AFTER this function returns,
	 * causing a deadlock.
	 */
	pid_t supervisor_pids[3] = { spid, idxPid, vacPid };
	bool ok = true;
	int remaining = (vacPid > 0) ? 3 : 2;

	while (remaining > 0)
	{
		pg_usleep(100 * 1000); /* 100 ms */

		for (int i = 0; i < 3; i++)
		{
			if (supervisor_pids[i] <= 0)
			{
				continue;
			}

			int status = 0;
			pid_t reaped = waitpid(supervisor_pids[i], &status, WNOHANG);

			if (reaped == supervisor_pids[i])
			{
				int returnCode = WEXITSTATUS(status);

				if (WIFSIGNALED(status))
				{
					log_error("Global supervisor %d killed by signal %s",
							  reaped,
							  signal_to_string(WTERMSIG(status)));
					ok = false;
				}
				else if (returnCode != 0)
				{
					log_error("Global supervisor %d exited with code %d",
							  reaped, returnCode);
					ok = false;
				}

				supervisor_pids[i] = -1;
				--remaining;
			}
			else if (reaped < 0 && errno != EINTR)
			{
				log_error("waitpid(%d) failed: %m", supervisor_pids[i]);
				ok = false;
				supervisor_pids[i] = -1;
				--remaining;
			}
		}
	}

	/*
	 * Reopen the global catalog so the caller can write summary_stop_timing.
	 */
	if (!catalog_open_from_specs(parentSpecs))
	{
		log_error("Failed to reopen global catalog after copy/index/vacuum phase");
		ok = false;
	}

	/*
	 * Finalize per-database timing sections.  Each per-db catalog has
	 * accumulated summary_increment_timing calls from copy/index/vacuum
	 * workers; calling summary_stop_timing here sets done_time_epoch and
	 * pretty-prints the duration so the consolidated summary displays
	 * non-zero values.
	 *
	 * Must happen BEFORE destroying semaphores so catalog_init can reuse
	 * the existing semId.
	 */
	for (int i = 0; i < parentSpecs->multiDbCount; i++)
	{
		MultiDbInfo *info = &parentSpecs->multiDbInfos[i];

		if (info->catalogSemId == 0)
		{
			continue;
		}

		DatabaseCatalog dbCat = {
			.type = DATABASE_CATALOG_TYPE_SOURCE,
			.sema.semId = info->catalogSemId,
			.sema.semIndex = info->catalogSemIndex,
			.sema.reentrant = true,
		};
		sformat(dbCat.dbfile, sizeof(dbCat.dbfile),
				"%s/schema/source.db", info->topdir);

		if (!catalog_init(&dbCat))
		{
			log_warn("Failed to open per-db catalog for \"%s\": "
					 "timings not finalized", info->datname);
			continue;
		}

		(void) summary_stop_timing(&dbCat, TIMING_SECTION_COPY_DATA);
		(void) summary_stop_timing(&dbCat, TIMING_SECTION_CREATE_INDEX);
		(void) summary_stop_timing(&dbCat, TIMING_SECTION_ALTER_TABLE);

		if (!parentSpecs->skipVacuum)
		{
			(void) summary_stop_timing(&dbCat, TIMING_SECTION_VACUUM);
		}

		(void) catalog_close(&dbCat);
	}

	/*
	 * Destroy the catalog semaphore set.  One IPC_RMID removes all N slots.
	 * All multiDbInfos share the same semId; unlink it once via [0].
	 */
	if (parentSpecs->multiDbCount > 0 &&
		parentSpecs->multiDbInfos[0].catalogSemId != 0)
	{
		Semaphore setHandle = {
			.semId = parentSpecs->multiDbInfos[0].catalogSemId
		};
		(void) semaphore_unlink(&setHandle);

		for (int i = 0; i < parentSpecs->multiDbCount; i++)
		{
			parentSpecs->multiDbInfos[i].catalogSemId = 0;
		}
	}

	/* Clean up index/vacuum queues (copy queue cleaned up by copy supervisor). */
	if (parentSpecs->indexQueue.qId != -1)
	{
		(void) queue_unlink(&parentSpecs->indexQueue);
		parentSpecs->indexQueue.qId = -1;
	}

	if (parentSpecs->vacuumQueue.qId != -1)
	{
		(void) queue_unlink(&parentSpecs->vacuumQueue);
		parentSpecs->vacuumQueue.qId = -1;
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
	{
		return true;
	}

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
			{
				ok = false;
			}
		}
	}

	return ok;
}


/*
 * multidb_init_index_entry is a lighter version of multidb_init_entry for use
 * by index and vacuum workers.  It opens the per-db catalog and target
 * connection but does NOT open a source connection or import a snapshot,
 * because index/vacuum workers only need catalog lookups and target writes.
 */
static bool
multidb_init_index_entry(MultiDbEntry *entry, CopyDataSpec *parentSpecs,
						 const char *datname)
{
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
		log_error("BUG: multidb_init_index_entry: no MultiDbInfo for \"%s\"",
				  datname);
		return false;
	}

	CopyDataSpec *dbSpecs = (CopyDataSpec *) calloc(1, sizeof(CopyDataSpec));

	if (dbSpecs == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	dbSpecs->pgPaths = parentSpecs->pgPaths;
	dbSpecs->filters = parentSpecs->filters;
	dbSpecs->section = parentSpecs->section;
	dbSpecs->restoreOptions = parentSpecs->restoreOptions;
	dbSpecs->skipVacuum = parentSpecs->skipVacuum;
	dbSpecs->skipAnalyze = parentSpecs->skipAnalyze;
	dbSpecs->skipLargeObjects = parentSpecs->skipLargeObjects;
	dbSpecs->failFast = parentSpecs->failFast;
	dbSpecs->resume = parentSpecs->resume;
	dbSpecs->allDatabases = false;

	/* queues inherited from parent — index/vacuum workers enqueue into them */
	dbSpecs->indexQueue = parentSpecs->indexQueue;
	dbSpecs->vacuumQueue = parentSpecs->vacuumQueue;
	dbSpecs->copyQueue.qId = -1;
	dbSpecs->loQueue.qId = -1;

	dbSpecs->connStrings.target_pguri = strdup(info->target_pguri);

	if (dbSpecs->connStrings.target_pguri == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(dbSpecs);
		return false;
	}

	if (!parse_and_scrub_connection_string(dbSpecs->connStrings.target_pguri,
										   &dbSpecs->connStrings.safeTargetPGURI))
	{
		log_error("Failed to scrub target URI for database \"%s\"", datname);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	if (!copydb_init_workdir(dbSpecs, info->topdir,
							 false, NULL, false, true, false))
	{
		log_error("Failed to init work dir for database \"%s\"", datname);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	DatabaseCatalog *sourceDB = &dbSpecs->catalogs.source;
	DatabaseCatalog *targetDB = &dbSpecs->catalogs.target;

	sourceDB->type = DATABASE_CATALOG_TYPE_SOURCE;
	strlcpy(sourceDB->dbfile, dbSpecs->cfPaths.sdbfile, sizeof(sourceDB->dbfile));

	/*
	 * Reuse the parent-created shared semaphore set slot for sourceDB so that
	 * all index workers for the same database share one lock (no SQLITE_BUSY).
	 */
	if (info->catalogSemId != 0)
	{
		sourceDB->sema.semId = info->catalogSemId;
		sourceDB->sema.semIndex = info->catalogSemIndex;
		sourceDB->sema.reentrant = true;
	}

	if (!catalog_init(sourceDB))
	{
		log_error("Failed to open catalog for database \"%s\"", datname);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	/*
	 * Open the target catalog read-only for constraint resume checks
	 * (catalog_s_table_count_indexes, catalog_lookup_s_index_by_name).
	 * No semaphore: SQLite WAL guarantees snapshot-consistent reads and
	 * never produces SQLITE_BUSY for readers.  Separate from sourceDB's
	 * semaphore slot to avoid deadlocking with catalog_iter_s_index_table
	 * which holds that slot for the entire iteration.
	 */
	targetDB->type = DATABASE_CATALOG_TYPE_TARGET;
	strlcpy(targetDB->dbfile, dbSpecs->cfPaths.tdbfile, sizeof(targetDB->dbfile));

	if (!catalog_open_readonly(targetDB))
	{
		log_error("Failed to open target catalog for database \"%s\"", datname);
		catalog_close(sourceDB);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	if (!pgsql_init(&entry->dst, dbSpecs->connStrings.target_pguri,
					PGSQL_CONN_TARGET))
	{
		log_error("Failed to connect to target for database \"%s\"", datname);
		catalog_close(targetDB);
		catalog_close(sourceDB);
		free(dbSpecs->connStrings.target_pguri);
		free(dbSpecs);
		return false;
	}

	if (!pgsql_set_gucs(&entry->dst, dstSettings))
	{
		log_warn("Failed to set GUCs on target for database \"%s\"", datname);
	}

	strlcpy(entry->datname, datname, sizeof(entry->datname));
	strlcpy(dbSpecs->datname, datname, sizeof(dbSpecs->datname));
	entry->dbSpecs = dbSpecs;
	entry->active = true;

	log_debug("Opened index/vacuum connections for database \"%s\"", datname);

	return true;
}


/*
 * multidb_index_context_close_entry closes an index/vacuum pool entry: catalog
 * and target connection only (no snapshot to close).
 */
static bool
multidb_index_context_close_entry(MultiDbEntry *entry)
{
	if (!entry->active)
	{
		return true;
	}

	log_debug("Closing index/vacuum connection cache entry for \"%s\"",
			  entry->datname);

	if (entry->dbSpecs != NULL)
	{
		(void) catalog_close(&entry->dbSpecs->catalogs.target);
		(void) catalog_close(&entry->dbSpecs->catalogs.source);
		free(entry->dbSpecs->connStrings.target_pguri);
		free(entry->dbSpecs);
		entry->dbSpecs = NULL;
	}

	(void) pgsql_finish(&entry->dst);

	entry->active = false;
	memset(entry->datname, 0, sizeof(entry->datname));

	return true;
}


/*
 * multidb_index_context_close_all closes all active entries in an
 * index/vacuum pool.
 */
bool
multidb_index_context_close_all(MultiDbContext *ctx)
{
	bool ok = true;

	for (int i = 0; i < MULTIDB_ENTRY_CACHE_MAX; i++)
	{
		if (ctx->entries[i].active)
		{
			if (!multidb_index_context_close_entry(&ctx->entries[i]))
			{
				ok = false;
			}
		}
	}

	return ok;
}


/*
 * multidb_index_context_get_entry returns the pool entry for datname, opening
 * a new one (with catalog + target connection only) if not cached.
 */
MultiDbEntry *
multidb_index_context_get_entry(MultiDbContext *ctx, const char *datname)
{
	for (int i = 0; i < MULTIDB_ENTRY_CACHE_MAX; i++)
	{
		if (ctx->entries[i].active &&
			strcmp(ctx->entries[i].datname, datname) == 0)
		{
			return &ctx->entries[i];
		}
	}

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
		slot = ctx->nextEvict % MULTIDB_ENTRY_CACHE_MAX;
		ctx->nextEvict = (ctx->nextEvict + 1) % MULTIDB_ENTRY_CACHE_MAX;

		log_debug("Evicting index/vacuum cache entry for \"%s\"",
				  ctx->entries[slot].datname);

		if (!multidb_index_context_close_entry(&ctx->entries[slot]))
		{
			return NULL;
		}
	}

	MultiDbEntry *entry = &ctx->entries[slot];

	if (!multidb_init_index_entry(entry, ctx->parentSpecs, datname))
	{
		return NULL;
	}

	ctx->count++;
	return entry;
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
	dbSpecs->section = parentSpecs->section;
	dbSpecs->restoreOptions = parentSpecs->restoreOptions;
	dbSpecs->roles = false;
	dbSpecs->skipLargeObjects = parentSpecs->skipLargeObjects;
	dbSpecs->skipExtensions = parentSpecs->skipExtensions;
	dbSpecs->skipCommentOnExtension = parentSpecs->skipCommentOnExtension;
	dbSpecs->skipCollations = parentSpecs->skipCollations;
	dbSpecs->skipVacuum = parentSpecs->skipVacuum;
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

	/*
	 * Wire the global index and vacuum queues so that copy workers fill them
	 * as they finish each table, exactly as in the singledb code path.
	 * copyQueue is not used by per-db workers; loQueue is handled separately.
	 */
	dbSpecs->copyQueue.qId = -1;
	dbSpecs->indexQueue = parentSpecs->indexQueue;
	dbSpecs->vacuumQueue = parentSpecs->vacuumQueue;
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
		{
			free(dbSpecs->connStrings.safeSourcePGURI.pguri);
		}
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
	 * Wire the shared catalog semaphore set slot so catalog_create_semaphore()
	 * finds semId != 0 and reuses it rather than creating a fresh semaphore.
	 * All workers for the same database thus share one lock, serialising
	 * SQLite writes and guaranteeing no SQLITE_BUSY contention.
	 */
	if (info->catalogSemId != 0)
	{
		sourceDB->sema.semId = info->catalogSemId;
		sourceDB->sema.semIndex = info->catalogSemIndex;
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
	strlcpy(dbSpecs->datname, datname, sizeof(dbSpecs->datname));
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
	{
		return false;
	}

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
		{
			return false;
		}

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
		{
			return false;
		}

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
bool
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
