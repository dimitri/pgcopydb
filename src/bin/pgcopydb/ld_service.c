/*
 * src/bin/pgcopydb/ld_service.c
 *
 * Client implementation for pgcopydb follow coordinator service
 */

#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "file_utils.h"
#include "string_utils.h"
#include "ld_service.h"

ServiceEndpoint
ld_service_get_endpoint(void)
{
	ServiceEndpoint endpoint = {0};
	const char *host = getenv("PGCOPYDB_HOST");
	const char *port_str = getenv("PGCOPYDB_PORT");

	if (host == NULL && port_str == NULL) {
		/* Service not configured */
		endpoint.enabled = false;
		return endpoint;
	}

	/* Use provided host or default to localhost */
	if (host != NULL) {
		sformat(endpoint.host, sizeof(endpoint.host), "%s", host);
	} else {
		sformat(endpoint.host, sizeof(endpoint.host), "%s", "localhost");
	}

	/* Use provided port or default to 5442 */
	endpoint.port = 5442;
	if (port_str != NULL) {
		endpoint.port = atoi(port_str);
		if (endpoint.port <= 0 || endpoint.port > 65535) {
			log_error("Invalid PGCOPYDB_PORT value: %s", port_str);
			endpoint.enabled = false;
			return endpoint;
		}
	}

	endpoint.enabled = true;
	log_debug("Service endpoint: %s:%d", endpoint.host, endpoint.port);
	return endpoint;
}

ServiceEndpoint
ld_service_endpoint(const char *host, int port)
{
	ServiceEndpoint endpoint = {0};

	if (host == NULL || host[0] == '\0') {
		/* no --host given: caller should use the direct SQLite path */
		endpoint.enabled = false;
		return endpoint;
	}

	sformat(endpoint.host, sizeof(endpoint.host), "%s", host);
	endpoint.port = (port > 0 && port <= 65535) ? port : 5442;
	endpoint.enabled = true;

	log_debug("Service endpoint (explicit): %s:%d", endpoint.host, endpoint.port);
	return endpoint;
}

bool
ld_service_send_command(ServiceEndpoint endpoint,
                       IPCMessage *request,
                       IPCMessage *response)
{
	IPCConn conn = {0};

	if (!endpoint.enabled) {
		log_error("Service endpoint not configured");
		return false;
	}

	/* Connect to service */
	if (!ld_ipc_tcp_connect(&conn, endpoint.host, endpoint.port)) {
		log_error("Failed to connect to pgcopydb service at %s:%d",
				 endpoint.host, endpoint.port);
		return false;
	}

	/* Send request */
	if (!ld_ipc_send_message(&conn, request)) {
		log_error("Failed to send command to service");
		ld_ipc_close(&conn);
		return false;
	}

	/* Receive response */
	if (!ld_ipc_recv_message(&conn, response, 5000)) {
		log_error("No response from service");
		ld_ipc_close(&conn);
		return false;
	}

	ld_ipc_close(&conn);

	/* Check for error response */
	if (response->type == IPC_MSG_ERROR) {
		const char *error = (const char *)response->payload;
		log_error("Service error: %s", error);
		return false;
	}

	return true;
}
