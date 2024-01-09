/*
 * src/bin/pgcopydb/lsn_tracking.h
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#ifndef LSN_TRACKING_H
#define LSN_TRACKING_H

#include "catalog.h"
#include "copydb.h"
#include "ld_stream.h"


bool lsn_tracking_read(StreamApplyContext *context);
bool lsn_tracking_write(DatabaseCatalog *catalog, LSNTracking *lsnTrackingList);
bool lsn_tracking_delete_all(DatabaseCatalog *catalog);
bool lsn_tracking_add(DatabaseCatalog *catalog, LSNTracking *current);

/*
 * Catalog Iterator API for lsn_tracking
 */
typedef bool (LSNTrackingIterFun)(void *context, LSNTracking *lsn_tracking);

bool lsn_tracking_iter(DatabaseCatalog *catalog,
					   void *context,
					   LSNTrackingIterFun *callback);

typedef struct LSNTrackingIterator
{
	DatabaseCatalog *catalog;
	LSNTracking *lsnTracking;
	SQLiteQuery query;
} LSNTrackingIterator;

bool lsn_tracking_iter_init(LSNTrackingIterator *iter);
bool lsn_tracking_iter_next(LSNTrackingIterator *iter);
bool lsn_tracking_iter_finish(LSNTrackingIterator *iter);

bool lsn_tracking_fetch(SQLiteQuery *query);

#endif /* LSN_TRACKING_H */
