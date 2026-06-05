/*
 * src/bin/pgcopydb/ld_ipc.c
 *
 * Inter-Process Communication implementation for CDC pipeline coordination
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "log.h"
#include "ld_ipc.h"

/* Unix domain socket operations */

bool
ld_ipc_unix_listen(IPCConn *conn, const char *socket_path)
{
	struct sockaddr_un addr = {0};
	int fd = -1;

	if (strlen(socket_path) >= sizeof(addr.sun_path) - 1) {
		log_error("Socket path too long: %s", socket_path);
		return false;
	}

	/* Create Unix domain socket */
	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		log_error("Failed to create Unix socket: %m");
		return false;
	}

	/* Remove stale socket file if exists */
	unlink(socket_path);

	/* Bind socket */
	addr.sun_family = AF_UNIX;
	strlcpy(addr.sun_path, socket_path, sizeof(addr.sun_path));

	if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		log_error("Failed to bind Unix socket at %s: %m", socket_path);
		close(fd);
		return false;
	}

	/* Listen for connections */
	if (listen(fd, 1) < 0) {
		log_error("Failed to listen on Unix socket: %m");
		close(fd);
		return false;
	}

	conn->fd = fd;
	strlcpy(conn->path, socket_path, sizeof(conn->path));
	conn->last_activity = time(NULL);

	log_debug("Listening on Unix socket: %s", socket_path);
	return true;
}

bool
ld_ipc_unix_accept(IPCConn *listen_conn, IPCConn *peer_conn, int timeout_ms)
{
	struct pollfd pfd = {0};
	int ret;

	if (listen_conn->fd < 0) {
		return false;
	}

	pfd.fd = listen_conn->fd;
	pfd.events = POLLIN;

	ret = poll(&pfd, 1, timeout_ms);
	if (ret < 0) {
		log_error("Poll failed on Unix socket: %m");
		return false;
	}

	if (ret == 0) {
		/* Timeout, no connection */
		return false;
	}

	int peer_fd = accept(listen_conn->fd, NULL, NULL);
	if (peer_fd < 0) {
		log_error("Failed to accept connection: %m");
		return false;
	}

	/* Set non-blocking for recv operations */
	int flags = fcntl(peer_fd, F_GETFL, 0);
	fcntl(peer_fd, F_SETFL, flags | O_NONBLOCK);

	peer_conn->fd = peer_fd;
	peer_conn->last_activity = time(NULL);

	return true;
}

bool
ld_ipc_unix_connect(IPCConn *conn, const char *socket_path)
{
	struct sockaddr_un addr = {0};
	int fd = -1;
	int retry = 5;

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		log_error("Failed to create Unix socket: %m");
		return false;
	}

	addr.sun_family = AF_UNIX;
	strlcpy(addr.sun_path, socket_path, sizeof(addr.sun_path));

	/* Retry connection for a few seconds (coordinator might not be ready) */
	while (retry > 0) {
		if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
			conn->fd = fd;
			strlcpy(conn->path, socket_path, sizeof(conn->path));
			conn->last_activity = time(NULL);
			log_debug("Connected to Unix socket: %s", socket_path);
			return true;
		}

		if (retry > 1) {
			log_debug("Failed to connect to %s, retrying...", socket_path);
			sleep(1);
		}
		retry--;
	}

	log_warn("Could not connect to Unix socket %s after retries", socket_path);
	close(fd);
	return false;
}

/* TCP socket operations */

bool
ld_ipc_tcp_listen(IPCConn *conn, const char *host, int port)
{
	struct sockaddr_in addr = {0};
	int fd = -1;
	int opt = 1;

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		log_error("Failed to create TCP socket: %m");
		return false;
	}

	/* Allow reuse of port */
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
		log_error("Failed to set SO_REUSEADDR: %m");
		close(fd);
		return false;
	}

	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);

	if (inet_aton(host, &addr.sin_addr) == 0) {
		log_error("Invalid IP address: %s", host);
		close(fd);
		return false;
	}

	if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		log_error("Failed to bind TCP socket at %s:%d: %m", host, port);
		close(fd);
		return false;
	}

	if (listen(fd, 5) < 0) {
		log_error("Failed to listen on TCP socket: %m");
		close(fd);
		return false;
	}

	conn->fd = fd;
	snprintf(conn->path, sizeof(conn->path), "%s:%d", host, port);
	conn->last_activity = time(NULL);

	log_info("Listening on TCP socket: %s:%d", host, port);
	return true;
}

bool
ld_ipc_tcp_accept(IPCConn *listen_conn, IPCConn *peer_conn, int timeout_ms)
{
	struct pollfd pfd = {0};
	struct sockaddr_in peer_addr = {0};
	socklen_t addr_len = sizeof(peer_addr);
	int ret;

	if (listen_conn->fd < 0) {
		return false;
	}

	pfd.fd = listen_conn->fd;
	pfd.events = POLLIN;

	ret = poll(&pfd, 1, timeout_ms);
	if (ret < 0) {
		log_error("Poll failed on TCP socket: %m");
		return false;
	}

	if (ret == 0) {
		/* Timeout, no connection */
		return false;
	}

	int peer_fd = accept(listen_conn->fd, (struct sockaddr *)&peer_addr, &addr_len);
	if (peer_fd < 0) {
		log_error("Failed to accept TCP connection: %m");
		return false;
	}

	/* Set non-blocking */
	int flags = fcntl(peer_fd, F_GETFL, 0);
	fcntl(peer_fd, F_SETFL, flags | O_NONBLOCK);

	peer_conn->fd = peer_fd;
	inet_ntop(AF_INET, &peer_addr.sin_addr, peer_conn->path, sizeof(peer_conn->path));
	peer_conn->last_activity = time(NULL);

	log_debug("Accepted TCP connection from %s", peer_conn->path);
	return true;
}

bool
ld_ipc_tcp_connect(IPCConn *conn, const char *host, int port)
{
	struct sockaddr_in addr = {0};
	int fd = -1;

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		log_error("Failed to create TCP socket: %m");
		return false;
	}

	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);

	if (inet_aton(host, &addr.sin_addr) == 0) {
		log_error("Invalid IP address: %s", host);
		close(fd);
		return false;
	}

	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		log_error("Failed to connect to %s:%d: %m", host, port);
		close(fd);
		return false;
	}

	conn->fd = fd;
	snprintf(conn->path, sizeof(conn->path), "%s:%d", host, port);
	conn->last_activity = time(NULL);

	log_debug("Connected to TCP socket: %s:%d", host, port);
	return true;
}

/* Send/receive message operations */

static bool
ld_ipc_send_all(int fd, const void *data, size_t len)
{
	size_t sent = 0;

	while (sent < len) {
		ssize_t ret = send(fd, (const char *)data + sent, len - sent, 0);
		if (ret < 0) {
			log_error("Failed to send IPC message: %m");
			return false;
		}
		sent += ret;
	}

	return true;
}

static bool
ld_ipc_recv_all(int fd, void *data, size_t len, int timeout_ms)
{
	struct pollfd pfd = {0};
	size_t recv_bytes = 0;
	int ret;

	pfd.fd = fd;
	pfd.events = POLLIN;

	while (recv_bytes < len) {
		ret = poll(&pfd, 1, timeout_ms);
		if (ret < 0) {
			log_error("Poll failed: %m");
			return false;
		}

		if (ret == 0) {
			/* Timeout */
			log_debug("IPC recv timeout after %zu/%zu bytes", recv_bytes, len);
			return false;
		}

		ssize_t n = recv(fd, (char *)data + recv_bytes, len - recv_bytes, 0);
		if (n < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
			log_error("Failed to recv IPC message: %m");
			return false;
		}

		if (n == 0) {
			log_debug("IPC connection closed");
			return false;
		}

		recv_bytes += n;
	}

	return true;
}

bool
ld_ipc_send_message(IPCConn *conn, const IPCMessage *msg)
{
	/* Send 4-byte header first */
	uint8_t header[4];
	header[0] = msg->version;
	header[1] = msg->type;
	header[2] = (msg->payload_len >> 8) & 0xFF;
	header[3] = msg->payload_len & 0xFF;

	if (!ld_ipc_send_all(conn->fd, header, 4)) {
		return false;
	}

	/* Send payload if any */
	if (msg->payload_len > 0) {
		if (!ld_ipc_send_all(conn->fd, msg->payload, msg->payload_len)) {
			return false;
		}
	}

	conn->last_activity = time(NULL);
	log_debug("Sent IPC message type %d (payload %d bytes)", msg->type, msg->payload_len);
	return true;
}

bool
ld_ipc_recv_message(IPCConn *conn, IPCMessage *msg, int timeout_ms)
{
	uint8_t header[4];

	memset(msg, 0, sizeof(IPCMessage));

	/* Receive 4-byte header */
	if (!ld_ipc_recv_all(conn->fd, header, 4, timeout_ms)) {
		return false;
	}

	msg->version = header[0];
	msg->type = header[1];
	msg->payload_len = ((uint16_t)header[2] << 8) | header[3];

	if (msg->payload_len > IPC_MAX_PAYLOAD_SIZE) {
		log_error("IPC payload too large: %d bytes", msg->payload_len);
		return false;
	}

	/* Receive payload if any */
	if (msg->payload_len > 0) {
		if (!ld_ipc_recv_all(conn->fd, msg->payload, msg->payload_len, timeout_ms)) {
			return false;
		}
	}

	conn->last_activity = time(NULL);
	log_debug("Received IPC message type %d (payload %d bytes)", msg->type, msg->payload_len);
	return true;
}

/* Connection management */

void
ld_ipc_close(IPCConn *conn)
{
	if (conn->fd >= 0) {
		close(conn->fd);
		conn->fd = -1;
	}
}

bool
ld_ipc_is_alive(IPCConn *conn)
{
	if (conn->fd < 0) {
		return false;
	}

	time_t now = time(NULL);
	if (now - conn->last_activity > 30) {
		/* No activity in 30 seconds, consider dead */
		return false;
	}

	return true;
}
