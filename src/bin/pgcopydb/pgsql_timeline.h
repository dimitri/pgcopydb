/*
 * src/bin/pgcopydb/pgsql_timeline.h
 *	 API for sending SQL commands about timelines to a PostgreSQL server
 */
#ifndef PGSQL_TIMELINE_H
#define PGSQL_TIMELINE_H

#include "pgsql.h"
#include "schema.h"

/* pgsql_timeline.c */
bool pgsql_identify_system(PGSQL *pgsql, IdentifySystem *system,
						   char *cdcPathDir);
bool parse_timeline_history_file(char *filename,
								 DatabaseCatalog *catalog,
								 uint32_t currentTimeline);

#endif /* PGSQL_TIMELINE_H */
