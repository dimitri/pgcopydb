/*
 * src/bin/pgcopydb/ld_ipc.h
 *
 * TCP service protocol for optional follow-coordinator mode.
 *
 * The pipeline processes (receive / transform / apply) coordinate via:
 *  - POSIX pipes (pipe_rt, pipe_ta) for lifecycle signals  ← primary
 *  - pipeline_state table in sourceDB for restart recovery  ← primary
 *
 * This header defines the TCP message protocol used by the optional
 * follow coordinator (follow_coordinator.c), which lets external CLI
 * commands (e.g. "pgcopydb stream sentinel set endpos") talk to a running
 * follow process over TCP when PGCOPYDB_HOST / PGCOPYDB_PORT are set.
 *
 * The Unix domain socket path (receive ↔ coordinator) was removed; the
 * pipe-based lifecycle signal replaces it entirely.
 */

#ifndef LD_IPC_H
#define LD_IPC_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#define IPC_PROTOCOL_VERSION 1
#define IPC_MAX_PAYLOAD_SIZE 1024

/*
 * Message types used by the optional TCP coordinator.
 * Only the minimal set actually referenced by follow_coordinator.c and
 * cli_sentinel.c is kept here.
 */
typedef enum
{
	IPC_MSG_PING = 0,             /* liveness check */
	IPC_MSG_PONG = 1,             /* liveness reply */

	IPC_MSG_SET_ENDPOS = 3,       /* CLI → coordinator: set sentinel.endpos */
	IPC_MSG_QUERY_SENTINEL = 4,   /* CLI → coordinator: read sentinel */
	IPC_MSG_SENTINEL_REPLY = 5,   /* coordinator → CLI: sentinel data */
	IPC_MSG_QUERY_STATUS = 6,     /* CLI → coordinator: process status */
	IPC_MSG_STATUS_REPLY = 7,     /* coordinator → CLI: status data */
	IPC_MSG_SET_STARTPOS = 8,     /* CLI → coordinator: set sentinel.startpos */
	IPC_MSG_SET_APPLY = 9,        /* CLI → coordinator: set sentinel.apply flag */

	IPC_MSG_CLEANUP = 10,         /* CLI → coordinator: cleanup old CDC files */
	IPC_MSG_CLEANUP_REPLY = 11,   /* coordinator → CLI: cleanup result */

	IPC_MSG_ACK_CONFIRMED = 18,   /* coordinator → CLI: request accepted */
	IPC_MSG_ERROR = 99,           /* generic error */
} IPCMessageType;

/*
 * Wire format: [version:1][type:1][payload_len:2][payload:N]
 */
typedef struct
{
	uint8_t version;
	uint8_t type;
	uint16_t payload_len;
	uint8_t payload[IPC_MAX_PAYLOAD_SIZE];
} IPCMessage;

/* Payload for IPC_MSG_SET_ENDPOS */
typedef struct
{
	uint64_t endpos_lsn;
	char reason[256];
} IPCPayloadSetEndpos;

/* Payload for IPC_MSG_SET_STARTPOS */
typedef struct
{
	uint64_t startpos_lsn;
	char reason[256];
} IPCPayloadSetStartpos;

/* Payload for IPC_MSG_SET_APPLY (apply=1 enables apply mode, 0 = prefetch) */
typedef struct
{
	uint8_t apply;
} IPCPayloadSetApply;

/* Payload for IPC_MSG_STATUS_REPLY */
typedef struct
{
	uint64_t startpos;
	uint64_t endpos;
	uint64_t write_lsn;
	uint64_t flush_lsn;
	uint64_t replay_lsn;
	uint32_t state;         /* 0=init 1=streaming 2=done 3=error */
} IPCPayloadStatusReply;

/* Payload for IPC_MSG_CLEANUP */
typedef struct
{
	uint8_t dry_run;
} IPCPayloadCleanup;

/* Payload for IPC_MSG_CLEANUP_REPLY */
typedef struct
{
	uint64_t files_deleted;
	uint64_t bytes_freed;
	uint64_t safe_lsn;       /* replay_lsn used as the cleanup boundary */
	uint8_t dry_run;
} IPCPayloadCleanupReply;

/* Payload for IPC_MSG_ERROR */
typedef struct
{
	char error_message[256];
} IPCPayloadError;

/*
 * TCP connection context.
 */
typedef struct
{
	int fd;
	char path[512];            /* host:port string for logging */
	time_t last_activity;
	int read_timeout_ms;
} IPCConn;

/* TCP socket operations (used by follow_coordinator and ld_service) */
bool ld_ipc_tcp_listen(IPCConn *conn, const char *host, int port);
bool ld_ipc_tcp_accept(IPCConn *listen, IPCConn *peer, int timeout_ms);
bool ld_ipc_tcp_connect(IPCConn *conn, const char *host, int port);

bool ld_ipc_send_message(IPCConn *conn, const IPCMessage *msg);
bool ld_ipc_recv_message(IPCConn *conn, IPCMessage *msg, int timeout_ms);

void ld_ipc_close(IPCConn *conn);
bool ld_ipc_is_alive(IPCConn *conn);

#define IPC_INIT_MESSAGE(msg, msg_type) \
	do { \
		memset(&(msg), 0, sizeof(IPCMessage)); \
		(msg).version = IPC_PROTOCOL_VERSION; \
		(msg).type = (msg_type); \
	} while (0)

#endif /* LD_IPC_H */
