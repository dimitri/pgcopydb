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
copydb_prepare_sequence_specs(CopyDataSpec *specs, PGSQL *pgsql, bool reset)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	TopLevelTiming timing = {
		.label = CopyDataSectionToString(DATA_SECTION_SET_SEQUENCES)
	};

	/*
	 * At sequence RESET time, we already have a list of sequences in our
	 * catalogs, so just skip listing sequences and use the cache we have
	 * on-disk.
	 */
	instr_time startTime;

	if (reset)
	{
		INSTR_TIME_SET_CURRENT(startTime);
	}
	else
	{
		(void) catalog_start_timing(&timing);

		if (!schema_list_sequences(pgsql, &(specs->filters), sourceDB))
		{
			/* errors have already been logged */
			return false;
		}
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

	if (reset)
	{
		instr_time duration;

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, startTime);

		uint64_t durationMs = INSTR_TIME_GET_MILLISEC(duration);

		if (!summary_increment_timing(sourceDB,
									  TIMING_SECTION_SET_SEQUENCES,
									  0, /* count didn't change */
									  0, /* bytesTransmitted */
									  durationMs))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		(void) catalog_stop_timing(&timing);

		/*
		 * Only register the section has done the first time (reset is false).
		 */
		if (!catalog_register_section(sourceDB, &timing))
		{
			/* errors have already been logged */
			return false;
		}
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

			bool reset = false;

			if (!copydb_copy_all_sequences(specs, reset))
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


typedef struct CopySeqContext
{
	PGSQL *dst;
	uint64_t count;
} CopySeqContext;


/*
 * copydb_copy_all_sequences fetches the list of sequences from the source
 * database and then for each of them runs a SELECT last_value, is_called FROM
 * the sequence on the source database and then calls SELECT setval(); on the
 * target database with the same values.
 */
bool
copydb_copy_all_sequences(CopyDataSpec *specs, bool reset)
{
	PGSQL src = { 0 };
	instr_time startTime;

	log_notice("Now starting setval process %d [%d]", getpid(), getppid());

	if (reset)
	{
		if (!pgsql_init(&src, specs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else if (specs->runState.sequenceCopyIsDone)
	{
		log_info("Skipping sequences, already done on a previous run");
		return true;
	}
	else if (specs->section != DATA_SECTION_SET_SEQUENCES &&
			 specs->section != DATA_SECTION_ALL)
	{
		log_debug("Skipping sequences in section %d", specs->section);
		return true;
	}

	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (!catalog_open(sourceDB))
	{
		log_error("Failed to open internal catalogs in sequence reset worker, "
				  "see above for details");
		return false;
	}

	log_info("%s sequences values on the target database",
			 reset ? "Reset" : "Set");

	if (reset)
	{
		INSTR_TIME_SET_CURRENT(startTime);
	}
	else
	{
		if (!summary_start_timing(sourceDB, TIMING_SECTION_SET_SEQUENCES))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * At sequence RESET time, we need to fetch the current values of the
	 * sequences on the source database all over again.
	 */
	if (reset)
	{
		if (!copydb_prepare_sequence_specs(specs, &src, reset))
		{
			/* errors have already been logged */
			return false;
		}
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

	CopySeqContext context = { .dst = &dst, .count = 0 };

	if (!catalog_iter_s_seq(sourceDB, &context, &copydb_copy_all_sequences_hook))
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

	if (reset)
	{
		instr_time duration;

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, startTime);

		uint64_t durationMs = INSTR_TIME_GET_MILLISEC(duration);

		if (!summary_increment_timing(sourceDB,
									  TIMING_SECTION_SET_SEQUENCES,
									  0, /* count didn't change */
									  0, /* bytesTransmitted */
									  durationMs))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		if (!summary_stop_timing(sourceDB, TIMING_SECTION_SET_SEQUENCES))
		{
			/* errors have already been logged */
			return false;
		}

		if (!summary_set_timing_count(sourceDB,
									  TIMING_SECTION_SET_SEQUENCES,
									  context.count))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!catalog_close(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_copy_all_sequences_hook is an iterator callback function.
 */
static bool
copydb_copy_all_sequences_hook(void *ctx, SourceSequence *seq)
{
	CopySeqContext *context = (CopySeqContext *) ctx;

	if (!schema_set_sequence_value(context->dst, seq))
	{
		log_error("Failed to set sequence values for %s", seq->qname);
		return false;
	}

	++(context->count);

	return true;
}
