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
						   DatabaseCatalog *catalog, char *cdcPathDir);
bool parseTimelineHistory(const char *content, IdentifySystem *system,
						  DatabaseCatalog *catalog);

/* pgsql.c */
bool pgsql_start_replication(LogicalStreamClient *client, DatabaseCatalog *catalog,
							 char *cdcPathDir);

#endif /* PGSQL_TIMELINE_H */
