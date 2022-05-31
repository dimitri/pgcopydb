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
	SourceSequenceArray *sequenceArray = &(specs->sequenceArray);

	if (!schema_list_sequences(pgsql, &(specs->filters), sequenceArray))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Fetching information for %d sequences", sequenceArray->count);

	int errors = 0;

	for (int seqIndex = 0; seqIndex < sequenceArray->count; seqIndex++)
	{
		SourceSequence *seq = &(sequenceArray->array[seqIndex]);

		char qname[BUFSIZE] = { 0 };

		sformat(qname, sizeof(qname), "\"%s\".\"%s\"",
				seq->nspname,
				seq->relname);

		if (!schema_get_sequence_value(pgsql, seq))
		{
			/* just skip this one */
			log_warn("Failed to get sequence values for %s", qname);
			++errors;
			continue;
		}
	}

	return errors == 0;
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

	if (!pgsql_init(&dst, specs->target_pguri, PGSQL_CONN_TARGET))
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

	SourceSequenceArray *sequenceArray = &(specs->sequenceArray);

	for (int seqIndex = 0; seqIndex < sequenceArray->count; seqIndex++)
	{
		SourceSequence *seq = &(sequenceArray->array[seqIndex]);

		char qname[BUFSIZE] = { 0 };

		sformat(qname, sizeof(qname), "\"%s\".\"%s\"",
				seq->nspname,
				seq->relname);

		if (!schema_set_sequence_value(&dst, seq))
		{
			/* just skip this one */
			log_warn("Failed to set sequence values for %s", qname);
			++errors;
			continue;
		}
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		++errors;
	}

	/* and write that we successfully finished copying all tables */
	if (!write_file("", 0, specs->cfPaths.done.sequences))
	{
		log_warn("Failed to write the tracking file \%s\"",
				 specs->cfPaths.done.sequences);
	}

	return errors == 0;
}
