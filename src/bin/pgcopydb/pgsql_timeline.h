/*
 * src/bin/pgcopydb/pgsql_timeline.h
 *	 API for sending SQL commands about timelines to a PostgreSQL server
 */
#ifndef PGSQL_TIMELINE_H
#define PGSQL_TIMELINE_H

#include "pgsql.h"
#include "schema.h"

typedef struct ParseTimelineHistoryContext
{
	uint32_t currentTimeline;
	DatabaseCatalog *catalog;
} ParseTimelineHistoryContext;


typedef struct TimelineHistoryIterator
{
	const char *filename;
	FileLinesIterator *fileIterator;
	TimelineHistoryEntry *entry;
	uint64_t prevend;
	uint32_t currentTimeline;
} TimelineHistoryIterator;

/* iterate over a file one line at a time */
typedef bool (TimelineHistoryFun)(void *context, TimelineHistoryEntry *entry);

/* pgsql_timeline.c */
bool pgsql_identify_system(PGSQL *pgsql, IdentifySystem *system,
						   DatabaseCatalog *catalog, char *cdcPathDir);
bool timeline_iter_history(char *filename,
						   ParseTimelineHistoryContext *context,
						   TimelineHistoryFun *callback);

bool timeline_iter_history_init(TimelineHistoryIterator *iter);
bool timeline_iter_history_next(TimelineHistoryIterator *iter);
bool timeline_iter_history_finish(TimelineHistoryIterator *iter);

bool timeline_history_add_hook(void *context, TimelineHistoryEntry *entry);

/* pgsql.c */
bool pgsql_start_replication(LogicalStreamClient *client, DatabaseCatalog *catalog,
							 char *cdcPathDir);

#endif /* PGSQL_TIMELINE_H */
