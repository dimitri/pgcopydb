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
typedef struct {
	char host[256];
	int port;
	bool enabled;
} ServiceEndpoint;

/* Get service endpoint from environment variables */
ServiceEndpoint ld_service_get_endpoint(void);

/* Connect to service and send command, get response */
bool ld_service_send_command(ServiceEndpoint endpoint,
                             IPCMessage *request,
                             IPCMessage *response);

#endif /* LD_SERVICE_H */
