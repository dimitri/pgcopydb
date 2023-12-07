/*
 * src/bin/pgcopydb/sequences.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

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


static bool copydb_prepare_sequence_specs_hook(void *ctx, SourceSequence *seq);
static bool copydb_copy_all_sequences_hook(void *ctx, SourceSequence *seq);

typedef struct PrepareSequenceContext
{
	PGSQL *pgsql;
	DatabaseCatalog *sourceDB;
} PrepareSequenceContext;


/*
 * sequence_prepare_specs fetches the list of sequences at pgsql connection,
 * using the filtering already prepared in the connection (as temp tables).
 * Then the function loops over the sequences to fetch their current values.
 */
bool
copydb_prepare_sequence_specs(CopyDataSpec *specs, PGSQL *pgsql)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!schema_list_sequences(pgsql, &(specs->filters), sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count objects in our catalogs");
		return false;
	}

	log_info("Fetching information for %lld sequences",
			 (long long) count.sequences);

	PrepareSequenceContext context = {
		.pgsql = pgsql,
		.sourceDB = sourceDB
	};

	if (!catalog_iter_s_seq(sourceDB,
							&context,
							&copydb_prepare_sequence_specs_hook))
	{
		log_error("Failed to prepare our internal sequence catalogs, "
				  "see above for details");
		return false;
	}

	if (!catalog_register_section(sourceDB, DATA_SECTION_SET_SEQUENCES))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_prepare_sequence_specs_hook is an iterator callback function.
 */
static bool
copydb_prepare_sequence_specs_hook(void *ctx, SourceSequence *seq)
{
	PrepareSequenceContext *context = (PrepareSequenceContext *) ctx;
	PGSQL *pgsql = context->pgsql;

	/*
	 * In case of "permission denied" for SELECT on the sequence object, we
	 * would then have a broken transaction and all the rest of the loop
	 * would get the following:
	 *
	 * ERROR: current transaction is aborted, commands ignored
	 * until end of transaction block
	 *
	 * To avoid that, for each sequence we first see if we're granted the
	 * SELECT privilege.
	 */
	bool granted = false;

	if (!pgsql_has_sequence_privilege(pgsql, seq->qname, "select", &granted))
	{
		/* errors have been logged */
		return false;
	}

	if (!granted)
	{
		log_error("Failed to SELECT values for sequence %s: "
				  "permission denied",
				  seq->qname);
		return false;
	}

	if (!schema_get_sequence_value(pgsql, seq))
	{
		/* just skip this one */
		log_warn("Failed to get sequence values for %s", seq->qname);
		return false;
	}

	if (!catalog_update_sequence_values(context->sourceDB, seq))
	{
		log_error("Failed to update sequences values for %s "
				  "in our internal catalogs",
				  seq->qname);
		return false;
	}

	return true;
}


/*
 * copydb_start_seq_process create a single sub-process that connects to the
 * target database to issue the setval() calls to reset sequences.
 */
bool
copydb_start_seq_process(CopyDataSpec *specs)
{
	log_info("STEP 9: reset sequences values");

	/*
	 * Flush stdio channels just before fork, to avoid double-output
	 * problems.
	 */
	fflush(stdout);
	fflush(stderr);

	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a worker process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			(void) set_ps_title("pgcopydb: copy sequences");

			if (!copydb_copy_all_sequences(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			break;
		}
	}

	return true;
}


/*
 * copydb_copy_all_sequences fetches the list of sequences from the source
 * database and then for each of them runs a SELECT last_value, is_called FROM
 * the sequence on the source database and then calls SELECT setval(); on the
 * target database with the same values.
 */
bool
copydb_copy_all_sequences(CopyDataSpec *specs)
{
	log_notice("Now starting setval process %d [%d]", getpid(), getppid());

	if (specs->dirState.sequenceCopyIsDone)
	{
		log_info("Skipping sequences, already done on a previous run");
		return true;
	}

	if (specs->section != DATA_SECTION_SET_SEQUENCES &&
		specs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping sequences in section %d", specs->section);
		return true;
	}

	log_info("Reset sequences values on the target database");

	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to open internal catalogs in CREATE INDEX worker, "
				  "see above for details");
		return false;
	}

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

	if (!catalog_iter_s_seq(sourceDB, &dst, &copydb_copy_all_sequences_hook))
	{
		log_error("Failed to copy sequences values from our internal catalogs, "
				  "see above for details");
		(void) pgsql_finish(&dst);
		return false;
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/* and write that we successfully finished copying all sequences */
	if (!write_file("", 0, specs->cfPaths.done.sequences))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.sequences);
	}

	return true;
}


/*
 * copydb_copy_all_sequences_hook is an iterator callback function.
 */
static bool
copydb_copy_all_sequences_hook(void *ctx, SourceSequence *seq)
{
	PGSQL *dst = (PGSQL *) ctx;

	if (!schema_set_sequence_value(dst, seq))
	{
		log_error("Failed to set sequence values for %s", seq->qname);
		return false;
	}

	return true;
}
