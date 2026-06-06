/*
 * src/bin/pgcopydb/follow_coordinator.h
 *
 * Optional TCP coordinator for the follow process.
 *
 * When PGCOPYDB_HOST / PGCOPYDB_PORT are set, a running "pgcopydb follow"
 * listens on that TCP endpoint for CLI commands (SET_ENDPOS, QUERY_STATUS,
 * QUERY_SENTINEL).  Pipeline lifecycle coordination uses pipes and
 * pipeline_state instead; the Unix socket path is gone.
 */

#ifndef FOLLOW_COORDINATOR_H
#define FOLLOW_COORDINATOR_H

#include <stdbool.h>
#include <time.h>

#include "ld_ipc.h"
#include "ld_stream.h"

typedef struct {
	IPCConn  tcp_listen;

	uint64_t sentinel_startpos;
	uint64_t sentinel_endpos;
	uint64_t sentinel_write_lsn;
	uint64_t sentinel_flush_lsn;
	uint64_t sentinel_replay_lsn;

	time_t   last_sentinel_update;
	uint64_t last_update_lsn;
	int      update_interval_sec;
} FollowCoordinator;

bool follow_coordinator_init(FollowCoordinator *coord,
                             const char *host, int port);
void follow_coordinator_shutdown(FollowCoordinator *coord);
bool follow_coordinator_handle_messages(FollowCoordinator *coord,
                                        StreamSpecs *specs);
bool follow_coordinator_update_sentinel(FollowCoordinator *coord,
                                        StreamSpecs *specs);

#endif /* FOLLOW_COORDINATOR_H */
