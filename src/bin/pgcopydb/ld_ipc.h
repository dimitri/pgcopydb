/*
 * src/bin/pgcopydb/ld_ipc.h
 *
 * Inter-Process Communication (IPC) Protocol for CDC Pipeline Coordination
 *
 * The follow process acts as a coordinator for receive/transform/apply processes.
 * Communication happens via:
 * - Unix domain sockets (receive ↔ follow)
 * - TCP sockets (CLI/inject.sh ↔ follow)
 */

#ifndef LD_IPC_H
#define LD_IPC_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

/*
 * IPC Protocol Version
 */
#define IPC_PROTOCOL_VERSION 1
#define IPC_MAX_PAYLOAD_SIZE 1024

/*
 * IPC Message Type Enum
 */
typedef enum {
	/* Control messages */
	IPC_MSG_PING = 0,            /* Liveness check request */
	IPC_MSG_ACK = 1,              /* Acknowledgment */

	/* Receive → Coordinator progress reporting */
	IPC_MSG_RECEIVE_INIT = 2,     /* Receive process initialized */
	IPC_MSG_DATA_AVAILABLE = 3,   /* New data written to output table */
	IPC_MSG_ENDPOS_REACHED = 4,   /* ENDPOS marker inserted */
	IPC_MSG_RECEIVE_DONE = 5,     /* Receive process finishing cleanly */
	IPC_MSG_RECEIVE_ERROR = 6,    /* Receive encountered fatal error */

	/* CLI/Coordinator commands */
	IPC_MSG_SET_ENDPOS = 11,      /* "Set endpos to LSN X/Y" */
	IPC_MSG_QUERY_STATUS = 12,    /* "What's the current state?" */
	IPC_MSG_QUERY_SENTINEL = 13,  /* "Get full sentinel data" */
	IPC_MSG_PAUSE_RECEIVE = 14,   /* "Pause streaming" */
	IPC_MSG_RESUME_RECEIVE = 15,  /* "Resume streaming" */

	/* Status responses */
	IPC_MSG_STATUS_REPLY = 20,    /* Status response payload */
	IPC_MSG_SENTINEL_REPLY = 21,  /* Sentinel data response */
	IPC_MSG_ERROR = 99,           /* Generic error message */
} IPCMessageType;

/*
 * IPC Message Header + Payload
 *
 * All integers are in network byte order (big-endian)
 *
 * [version:1][type:1][payload_len:2][payload:N]
 * Total: 4 bytes header + variable payload
 */
typedef struct {
	uint8_t  version;
	uint8_t  type;
	uint16_t payload_len;
	uint8_t  payload[IPC_MAX_PAYLOAD_SIZE];
} IPCMessage;

/*
 * Payload structures for specific message types
 */

/* IPC_MSG_DATA_AVAILABLE */
typedef struct {
	uint64_t written_lsn;         /* Latest LSN written to output table */
	uint64_t timestamp;           /* Unix timestamp */
} IPCPayloadDataAvailable;

/* IPC_MSG_ENDPOS_REACHED */
typedef struct {
	uint64_t endpos_lsn;          /* The endpos LSN that was set */
	uint64_t timestamp;           /* When ENDPOS was inserted */
} IPCPayloadEndposReached;

/* IPC_MSG_SET_ENDPOS */
typedef struct {
	uint64_t endpos_lsn;
	char reason[256];             /* Audit trail: who/why set this */
} IPCPayloadSetEndpos;

/* IPC_MSG_STATUS_REPLY */
typedef struct {
	uint64_t startpos;
	uint64_t endpos;
	uint64_t write_lsn;
	uint64_t transform_lsn;
	uint64_t flush_lsn;
	uint64_t replay_lsn;
	uint32_t state;               /* 0=init, 1=streaming, 2=done, 3=error */
} IPCPayloadStatusReply;

/* IPC_MSG_ERROR */
typedef struct {
	char error_message[256];
} IPCPayloadError;

/*
 * IPC Connection Context
 */
typedef struct {
	int fd;                       /* Socket file descriptor */
	char path[512];               /* Socket path (Unix) or address (TCP) */
	time_t last_activity;         /* For timeout detection */
	int read_timeout_ms;          /* Read timeout in milliseconds */
} IPCConn;

/*
 * Public API functions
 */

/* Unix domain socket operations */
bool ld_ipc_unix_listen(IPCConn *conn, const char *socket_path);
bool ld_ipc_unix_accept(IPCConn *listen_conn, IPCConn *peer_conn, int timeout_ms);
bool ld_ipc_unix_connect(IPCConn *conn, const char *socket_path);

/* TCP socket operations */
bool ld_ipc_tcp_listen(IPCConn *conn, const char *host, int port);
bool ld_ipc_tcp_accept(IPCConn *listen_conn, IPCConn *peer_conn, int timeout_ms);
bool ld_ipc_tcp_connect(IPCConn *conn, const char *host, int port);

/* Send/receive messages */
bool ld_ipc_send_message(IPCConn *conn, const IPCMessage *msg);
bool ld_ipc_recv_message(IPCConn *conn, IPCMessage *msg, int timeout_ms);

/* Connection management */
void ld_ipc_close(IPCConn *conn);
bool ld_ipc_is_alive(IPCConn *conn);

/* Helper macros */
#define IPC_INIT_MESSAGE(msg, msg_type) \
	do { \
		memset(&(msg), 0, sizeof(IPCMessage)); \
		(msg).version = IPC_PROTOCOL_VERSION; \
		(msg).type = (msg_type); \
	} while (0)

#endif /* LD_IPC_H */
