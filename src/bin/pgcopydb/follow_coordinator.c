/*
 * src/bin/pgcopydb/follow_coordinator.c
 *
 * Follow process coordinator implementation
 */

#include <string.h>
#include <time.h>

#include "copydb.h"
#include "file_utils.h"
#include "log.h"
#include "follow_coordinator.h"

bool
follow_coordinator_init(FollowCoordinator *coord, const char *work_dir,
                       const char *host, int port)
{
	char socket_path[512];

	memset(coord, 0, sizeof(FollowCoordinator));

	/* Initialize throttling: update every 5 seconds at most */
	coord->update_interval_sec = 5;
	coord->last_sentinel_update = time(NULL);

	/* Start listening on Unix socket from receive process */
	sformat(socket_path, sizeof(socket_path), "%s/follow-receive.sock", work_dir);

	if (!ld_ipc_unix_listen(&coord->ipc_listen, socket_path)) {
		log_error("Failed to listen on IPC socket");
		return false;
	}

	/* Start listening on TCP socket for CLI commands */
	if (!ld_ipc_tcp_listen(&coord->tcp_listen, host, port)) {
		log_error("Failed to listen on TCP socket");
		ld_ipc_close(&coord->ipc_listen);
		return false;
	}

	log_info("Coordinator initialized: IPC=%s TCP=%s:%d", socket_path, host, port);
	return true;
}

void
follow_coordinator_shutdown(FollowCoordinator *coord)
{
	ld_ipc_close(&coord->ipc_listen);
	ld_ipc_close(&coord->tcp_listen);
}

static bool
follow_coordinator_handle_ipc(FollowCoordinator *coord, StreamSpecs *specs)
{
	IPCConn peer = {0};
	IPCMessage msg = {0};

	/* Try to accept a new connection (non-blocking) */
	if (!ld_ipc_unix_accept(&coord->ipc_listen, &peer, 100)) {
		return true;  /* No connection, continue */
	}

	/* Try to receive a message */
	if (!ld_ipc_recv_message(&peer, &msg, 500)) {
		ld_ipc_close(&peer);
		return true;
	}

	/* Handle the message */
	switch (msg.type) {
	case IPC_MSG_DATA_AVAILABLE: {
		IPCPayloadDataAvailable *payload = (IPCPayloadDataAvailable *)msg.payload;
		coord->sentinel_write_lsn = payload->written_lsn;
		log_debug("Coordinator: receive reported new data at %X/%X",
				 (unsigned)(payload->written_lsn >> 32),
				 (unsigned)(payload->written_lsn & 0xFFFFFFFFU));
		break;
	}

	case IPC_MSG_ENDPOS_REACHED: {
		IPCPayloadEndposReached *payload = (IPCPayloadEndposReached *)msg.payload;
		coord->sentinel_endpos = payload->endpos_lsn;
		log_info("Coordinator: receive reported ENDPOS at %X/%X",
				(unsigned)(payload->endpos_lsn >> 32),
				(unsigned)(payload->endpos_lsn & 0xFFFFFFFFU));
		break;
	}

	case IPC_MSG_PING: {
		/* Send ACK */
		IPCMessage ack = {0};
		IPC_INIT_MESSAGE(ack, IPC_MSG_ACK);
		ld_ipc_send_message(&peer, &ack);
		break;
	}

	default:
		log_debug("Coordinator: ignored IPC message type %d", msg.type);
	}

	ld_ipc_close(&peer);
	return true;
}

static bool
follow_coordinator_handle_tcp(FollowCoordinator *coord, StreamSpecs *specs)
{
	IPCConn peer = {0};
	IPCMessage msg = {0};
	IPCMessage response = {0};

	/* Try to accept a new connection (non-blocking) */
	if (!ld_ipc_tcp_accept(&coord->tcp_listen, &peer, 100)) {
		return true;  /* No connection, continue */
	}

	/* Receive command message */
	if (!ld_ipc_recv_message(&peer, &msg, 1000)) {
		ld_ipc_close(&peer);
		return true;
	}

	/* Initialize response as ACK by default */
	IPC_INIT_MESSAGE(response, IPC_MSG_ACK);

	/* Handle command */
	switch (msg.type) {
	case IPC_MSG_SET_ENDPOS: {
		IPCPayloadSetEndpos *cmd = (IPCPayloadSetEndpos *)msg.payload;

		log_info("Coordinator: CLI set endpos to %X/%X (%s)",
				(unsigned)(cmd->endpos_lsn >> 32),
				(unsigned)(cmd->endpos_lsn & 0xFFFFFFFFU),
				cmd->reason);

		coord->sentinel_endpos = cmd->endpos_lsn;

		/* Update sentinel immediately for endpos changes */
		if (!follow_coordinator_update_sentinel(coord, specs)) {
			response.type = IPC_MSG_ERROR;
			const char *error = "Failed to update sentinel";
			response.payload_len = strlen(error);
			memcpy(response.payload, error, response.payload_len);
		}
		break;
	}

	case IPC_MSG_QUERY_SENTINEL: {
		/* Return current sentinel state */
		response.type = IPC_MSG_SENTINEL_REPLY;
		response.payload_len = sizeof(CopyDBSentinel);
		memcpy(response.payload, &specs->sentinel, sizeof(CopyDBSentinel));
		break;
	}

	case IPC_MSG_QUERY_STATUS: {
		/* Return status reply */
		response.type = IPC_MSG_STATUS_REPLY;
		IPCPayloadStatusReply *status = (IPCPayloadStatusReply *)response.payload;
		status->startpos = coord->sentinel_startpos;
		status->endpos = coord->sentinel_endpos;
		status->write_lsn = coord->sentinel_write_lsn;
		status->transform_lsn = coord->sentinel_transform_lsn;
		status->flush_lsn = coord->sentinel_flush_lsn;
		status->replay_lsn = coord->sentinel_replay_lsn;
		status->state = 1;  /* streaming */
		response.payload_len = sizeof(IPCPayloadStatusReply);
		break;
	}

	case IPC_MSG_PING: {
		/* Already ACK */
		break;
	}

	default:
		response.type = IPC_MSG_ERROR;
		const char *error = "Unknown command type";
		response.payload_len = strlen(error);
		memcpy(response.payload, error, response.payload_len);
	}

	/* Send response */
	ld_ipc_send_message(&peer, &response);
	ld_ipc_close(&peer);

	return true;
}

bool
follow_coordinator_handle_messages(FollowCoordinator *coord, StreamSpecs *specs)
{
	/* Handle IPC from receive process */
	if (!follow_coordinator_handle_ipc(coord, specs)) {
		return false;
	}

	/* Handle TCP from CLI */
	if (!follow_coordinator_handle_tcp(coord, specs)) {
		return false;
	}

	return true;
}

bool
follow_coordinator_update_sentinel(FollowCoordinator *coord, StreamSpecs *specs)
{
	time_t now = time(NULL);

	/*
	 * Throttle sentinel updates: only write if:
	 * 1. Enough time has passed since last update, OR
	 * 2. endpos was just set (urgent update), OR
	 * 3. Significant progress made (>= 1MB of data)
	 */
	uint64_t progress = coord->sentinel_write_lsn - coord->last_update_lsn;
	const uint64_t PROGRESS_THRESHOLD = 1 * 1024 * 1024;  /* 1MB */

	bool endpos_changed = (specs->sentinel.endpos != coord->sentinel_endpos);

	if (now - coord->last_sentinel_update < coord->update_interval_sec &&
		progress < PROGRESS_THRESHOLD &&
		!endpos_changed) {
		/* Skip this update */
		return true;
	}

	/* Update endpos if it changed (this takes priority) */
	if (endpos_changed) {
		if (!sentinel_update_endpos(specs->sourceDB, coord->sentinel_endpos)) {
			log_error("Failed to update sentinel endpos");
			return false;
		}
		log_info("Coordinator: updated sentinel endpos to %X/%X",
				 (unsigned)(coord->sentinel_endpos >> 32),
				 (unsigned)(coord->sentinel_endpos & 0xFFFFFFFFU));
	}

	/* Update write_lsn if it changed */
	if (coord->sentinel_write_lsn != specs->sentinel.write_lsn) {
		if (!sentinel_update_write_flush_lsn(specs->sourceDB,
											  coord->sentinel_write_lsn,
											  coord->sentinel_flush_lsn)) {
			log_error("Failed to update sentinel LSNs");
			return false;
		}
	}

	coord->last_sentinel_update = now;
	coord->last_update_lsn = coord->sentinel_write_lsn;

	log_debug("Coordinator: updated sentinel at %X/%X",
			 (unsigned)(coord->sentinel_write_lsn >> 32),
			 (unsigned)(coord->sentinel_write_lsn & 0xFFFFFFFFU));

	return true;
}
