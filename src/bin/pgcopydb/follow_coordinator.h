/*
 * src/bin/pgcopydb/follow_coordinator.h
 *
 * Follow process coordinator for CDC pipeline
 */

#ifndef FOLLOW_COORDINATOR_H
#define FOLLOW_COORDINATOR_H

#include <stdbool.h>
#include <time.h>

#include "ld_ipc.h"
#include "ld_stream.h"

typedef struct {
	/* Network listeners */
	IPCConn ipc_listen;           /* Unix socket from receive process */
	IPCConn tcp_listen;           /* TCP socket for CLI commands */

	/* Coordinator state (cached from sentinel) */
	uint64_t sentinel_startpos;
	uint64_t sentinel_endpos;
	uint64_t sentinel_write_lsn;
	uint64_t sentinel_transform_lsn;
	uint64_t sentinel_flush_lsn;
	uint64_t sentinel_replay_lsn;

	/* Throttling for IPC feedback */
	time_t last_sentinel_update;
	uint64_t last_update_lsn;
	int update_interval_sec;      /* Min interval between sentinel updates */
} FollowCoordinator;

/* Initialize coordinator (opens sockets) */
bool follow_coordinator_init(FollowCoordinator *coord, const char *work_dir,
                             const char *host, int port);

/* Shutdown coordinator */
void follow_coordinator_shutdown(FollowCoordinator *coord);

/* Handle incoming IPC and TCP messages */
bool follow_coordinator_handle_messages(FollowCoordinator *coord,
                                        StreamSpecs *specs);

/* Update sentinel atomically */
bool follow_coordinator_update_sentinel(FollowCoordinator *coord,
                                        StreamSpecs *specs);

#endif /* FOLLOW_COORDINATOR_H */
