/*
 * src/bin/pgcopydb/sequences.c
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
 * sequence_prepare_specs fetches the list of sequences at pgsql connection,
 * using the filtering already prepared in the connection (as temp tables).
 * Then the function loops over the sequences to fetch their current values.
 */
bool
copydb_prepare_sequence_specs(CopyDataSpec *specs, PGSQL *pgsql)
{
	SourceSequenceArray *sequenceArray = &(specs->catalog.sequenceArray);

	if (!schema_list_sequences(pgsql, &(specs->filters), sequenceArray))
	{
		/* errors have already been logged */
		return false;
	}

	SourceSequence *sourceSeqHashByOid = NULL;

	log_info("Fetching information for %d sequences", sequenceArray->count);

	int errors = 0;

	for (int seqIndex = 0; seqIndex < sequenceArray->count; seqIndex++)
	{
		SourceSequence *seq = &(sequenceArray->array[seqIndex]);

		/* add the current sequence to the sequence Hash-by-OID */
		HASH_ADD(hh, sourceSeqHashByOid, oid, sizeof(uint32_t), seq);

		sformat(seq->qname, sizeof(seq->qname), "%s.%s",
				seq->nspname,
				seq->relname);

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
			++errors;
			break;
		}

		if (!granted)
		{
			log_error("Failed to SELECT values for sequence %s: "
					  "permission denied",
					  seq->qname);
			++errors;
			continue;
		}

		if (!schema_get_sequence_value(pgsql, seq))
		{
			/* just skip this one */
			log_warn("Failed to get sequence values for %s", seq->qname);
			++errors;
			continue;
		}
	}

	/* now attach the final hash table head to the specs */
	specs->catalog.sourceSeqHashByOid = sourceSeqHashByOid;

	return errors == 0;
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

	int errors = 0;

	SourceSequenceArray *sequenceArray = &(specs->catalog.sequenceArray);

	for (int seqIndex = 0; seqIndex < sequenceArray->count; seqIndex++)
	{
		SourceSequence *seq = &(sequenceArray->array[seqIndex]);

		char qname[BUFSIZE] = { 0 };

		sformat(qname, sizeof(qname), "%s.%s",
				seq->nspname,
				seq->relname);

		if (!pgsql_savepoint(&dst, "sequences"))
		{
			/* errors have already been logged */
			return false;
		}

		if (!schema_set_sequence_value(&dst, seq))
		{
			log_error("Failed to set sequence values for %s", qname);

			if (specs->failFast)
			{
				(void) pgsql_commit(&dst);
				return false;
			}

			if (!pgsql_rollback_to_savepoint(&dst, "sequences"))
			{
				/* errors have already been logged */
				return false;
			}

			++errors;
			continue;
		}

		if (!pgsql_release_savepoint(&dst, "sequences"))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		++errors;
	}

	/* and write that we successfully finished copying all sequences */
	if (!write_file("", 0, specs->cfPaths.done.sequences))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.sequences);
	}

	if (errors > 0)
	{
		log_warn("Failed to set values for %d sequences, "
				 "see above for details",
				 errors);
		return false;
	}

	return true;
}
