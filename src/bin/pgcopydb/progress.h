/*
 * src/bin/pgcopydb/progress.h
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#ifndef PROGRESS_H
#define PROGRESS_H

#include "parson.h"

#include "schema.h"
#include "summary.h"


typedef struct CopyTableSummaryArray
{
	int count;
	CopyTableSummary *array;         /* malloc'ed area */
} CopyTableSummaryArray;


typedef struct CopyIndexSummaryArray
{
	int count;
	CopyIndexSummary *array;         /* malloc'ed area */
} CopyIndexSummaryArray;


/* register progress being made, see `pgcopydb list progress` */
typedef struct CopyProgress
{
	int tableCount;
	int tableDoneCount;
	SourceTableArray tableInProgress;
	CopyTableSummaryArray tableSummaryArray;

	int indexCount;
	int indexDoneCount;
	SourceIndexArray indexInProgress;
	CopyIndexSummaryArray indexSummaryArray;
} CopyProgress;


bool copydb_prepare_schema_json_file(CopyDataSpec *copySpecs);
bool copydb_parse_schema_json_file(CopyDataSpec *copySpecs);
bool copydb_update_progress(CopyDataSpec *copySpecs, CopyProgress *progress);

bool copydb_progress_as_json(CopyDataSpec *copySpecs,
							 CopyProgress *progress,
							 JSON_Value *js);

#endif  /* PROGRESS_H */
