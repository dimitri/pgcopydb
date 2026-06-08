/*
 * src/bin/pgcopydb/ld_service.h
 *
 * Client for connecting to pgcopydb follow coordinator service
 */

#ifndef LD_SERVICE_H
#define LD_SERVICE_H

#include <stdbool.h>

#include "ld_ipc.h"

/* Service endpoint configuration */
typedef struct
{
	char host[256];
	int port;
	bool enabled;
} ServiceEndpoint;

/*
 * Build a service endpoint from an explicit host/port (the --host/--port CLI
 * options).  Enabled when host is non-empty; port defaults to 5442 when 0.
 */
ServiceEndpoint ld_service_endpoint(const char *host, int port);

/* Connect to service and send command, get response */
bool ld_service_send_command(ServiceEndpoint endpoint,
							 IPCMessage *request,
							 IPCMessage *response);

#endif /* LD_SERVICE_H */
