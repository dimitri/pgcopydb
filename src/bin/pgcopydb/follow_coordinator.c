/*
 * src/bin/pgcopydb/follow_coordinator.c
 *
 * Optional TCP coordinator for the follow process.
 *
 * Listens on PGCOPYDB_HOST:PGCOPYDB_PORT (default 5442) for CLI commands.
 * Pipeline lifecycle coordination (receive→transform→apply sequencing) is
 * handled by pipes and pipeline_state; only the TCP CLI path remains here.
 */

#include <string.h>
#include <time.h>

#include "copydb.h"
#include "file_utils.h"
#include "log.h"
#include "follow_coordinator.h"


bool
follow_coordinator_init(FollowCoordinator *coord,
						const char *host, int port)
{
	memset(coord, 0, sizeof(FollowCoordinator));

	coord->update_interval_sec = 5;
	coord->last_sentinel_update = time(NULL);

	if (host == NULL || port <= 0)
	{
		/* TCP coordinator not requested */
		coord->tcp_listen.fd = -1;
		return true;
	}

	if (!ld_ipc_tcp_listen(&coord->tcp_listen, host, port))
	{
		log_error("Failed to listen on TCP coordinator socket %s:%d", host, port);
		return false;
	}

	log_info("Follow coordinator listening on %s:%d", host, port);
	return true;
}


void
follow_coordinator_shutdown(FollowCoordinator *coord)
{
	ld_ipc_close(&coord->tcp_listen);
}


bool
follow_coordinator_handle_messages(FollowCoordinator *coord, StreamSpecs *specs)
{
	if (coord->tcp_listen.fd < 0)
	{
		return true;   /* TCP coordinator not active */
	}
	IPCConn peer = { 0 };
	IPCMessage msg = { 0 };
	IPCMessage response = { 0 };

	/* Non-blocking accept (100 ms timeout) */
	if (!ld_ipc_tcp_accept(&coord->tcp_listen, &peer, 100))
	{
		return true;
	}

	if (!ld_ipc_recv_message(&peer, &msg, 1000))
	{
		ld_ipc_close(&peer);
		return true;
	}

	IPC_INIT_MESSAGE(response, IPC_MSG_ACK_CONFIRMED);

	/*
	 * The coordinator runs inside the follow supervisor, which holds an open
	 * sourceDB catalog sharing the IPC_PRIVATE write semaphore with the
	 * receive/apply children.  All sentinel reads/writes go straight to SQLite
	 * here, serialized by that shared semaphore — the CLI client never touches
	 * the catalog files.
	 */
	DatabaseCatalog *sourceDB = specs->sourceDB;

	switch (msg.type)
	{
		case IPC_MSG_SET_ENDPOS:
		{
			IPCPayloadSetEndpos *cmd = (IPCPayloadSetEndpos *) msg.payload;

			log_info("Coordinator: CLI set endpos to %X/%X (%s)",
					 (unsigned) (cmd->endpos_lsn >> 32),
					 (unsigned) (cmd->endpos_lsn & 0xFFFFFFFFU),
					 cmd->reason);

			if (!sentinel_update_endpos(sourceDB, cmd->endpos_lsn))
			{
				response.type = IPC_MSG_ERROR;
				const char *err = "Failed to update sentinel endpos";
				response.payload_len = strlen(err);
				memcpy(response.payload, err, response.payload_len); /* IGNORE-BANNED */
			}
			break;
		}

		case IPC_MSG_SET_STARTPOS:
		{
			IPCPayloadSetStartpos *cmd = (IPCPayloadSetStartpos *) msg.payload;

			log_info("Coordinator: CLI set startpos to %X/%X (%s)",
					 (unsigned) (cmd->startpos_lsn >> 32),
					 (unsigned) (cmd->startpos_lsn & 0xFFFFFFFFU),
					 cmd->reason);

			if (!sentinel_update_startpos(sourceDB, cmd->startpos_lsn))
			{
				response.type = IPC_MSG_ERROR;
				const char *err = "Failed to update sentinel startpos";
				response.payload_len = strlen(err);
				memcpy(response.payload, err, response.payload_len); /* IGNORE-BANNED */
			}
			break;
		}

		case IPC_MSG_SET_APPLY:
		{
			IPCPayloadSetApply *cmd = (IPCPayloadSetApply *) msg.payload;

			log_info("Coordinator: CLI set apply mode to %s",
					 cmd->apply ? "enabled" : "disabled");

			if (!sentinel_update_apply(sourceDB, cmd->apply != 0))
			{
				response.type = IPC_MSG_ERROR;
				const char *err = "Failed to update sentinel apply flag";
				response.payload_len = strlen(err);
				memcpy(response.payload, err, response.payload_len); /* IGNORE-BANNED */
			}
			break;
		}

		case IPC_MSG_QUERY_SENTINEL:
		{
			CopyDBSentinel s = { 0 };

			if (!sentinel_get(sourceDB, &s))
			{
				response.type = IPC_MSG_ERROR;
				const char *err = "Failed to read sentinel";
				response.payload_len = strlen(err);
				memcpy(response.payload, err, response.payload_len); /* IGNORE-BANNED */
			}
			else
			{
				response.type = IPC_MSG_SENTINEL_REPLY;
				response.payload_len = sizeof(CopyDBSentinel);
				memcpy(response.payload, &s, sizeof(CopyDBSentinel)); /* IGNORE-BANNED */
			}
			break;
		}

		case IPC_MSG_QUERY_STATUS:
		{
			CopyDBSentinel s = { 0 };
			(void) sentinel_get(sourceDB, &s);

			response.type = IPC_MSG_STATUS_REPLY;
			IPCPayloadStatusReply *st = (IPCPayloadStatusReply *) response.payload;
			st->startpos = s.startpos;
			st->endpos = s.endpos;
			st->write_lsn = s.write_lsn;
			st->flush_lsn = s.flush_lsn;
			st->replay_lsn = s.replay_lsn;
			st->state = 1;
			response.payload_len = sizeof(IPCPayloadStatusReply);
			break;
		}

		case IPC_MSG_PING:
		{
			response.type = IPC_MSG_PONG;
			break;
		}

		default:
		{
			response.type = IPC_MSG_ERROR;
			const char *err = "Unknown command";
			response.payload_len = strlen(err);
			memcpy(response.payload, err, response.payload_len); /* IGNORE-BANNED */
			break;
		}
	}

	ld_ipc_send_message(&peer, &response);
	ld_ipc_close(&peer);
	return true;
}


bool
follow_coordinator_update_sentinel(FollowCoordinator *coord, StreamSpecs *specs)
{
	time_t now = time(NULL);

	bool endpos_changed = (specs->sentinel.endpos != coord->sentinel_endpos);

	uint64_t progress = coord->sentinel_write_lsn - coord->last_update_lsn;
	const uint64_t THRESH_1MB = 1 * 1024 * 1024;

	if (!endpos_changed &&
		progress < THRESH_1MB &&
		(now - coord->last_sentinel_update) < coord->update_interval_sec)
	{
		return true;
	}

	if (endpos_changed)
	{
		if (!sentinel_update_endpos(specs->sourceDB, coord->sentinel_endpos))
		{
			log_error("Failed to update sentinel endpos");
			return false;
		}
		log_info("Coordinator: sentinel endpos → %X/%X",
				 (unsigned) (coord->sentinel_endpos >> 32),
				 (unsigned) (coord->sentinel_endpos & 0xFFFFFFFFU));
	}

	if (coord->sentinel_write_lsn != specs->sentinel.write_lsn)
	{
		if (!sentinel_update_write_flush_lsn(specs->sourceDB,
											 coord->sentinel_write_lsn,
											 coord->sentinel_flush_lsn))
		{
			log_error("Failed to update sentinel write/flush LSNs");
			return false;
		}
	}

	coord->last_sentinel_update = now;
	coord->last_update_lsn = coord->sentinel_write_lsn;
	return true;
}
