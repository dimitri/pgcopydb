# TCP Coordinator Design Document

## Overview

The TCP Coordinator is a network-based service running within the `pgcopydb follow` process that enables remote clients (CLI tools, scripts, monitoring systems) to manage the CDC pipeline without requiring shared filesystem volumes. It provides a clean separation between the coordinator process (which manages sentinel state and subprocess synchronization) and client processes (which issue commands).

**Key Benefits:**
- Eliminates docker volume coupling for state sharing
- Enables remote pipeline management across machines
- Supports multiple concurrent clients
- Clean separation of concerns
- Foundation for monitoring and observability

## Architecture

### High-Level Design

```
┌────────────────────────────────────────────────────────────┐
│                   pgcopydb follow                          │
│  (Main process - runs three subprocesses)                  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ FollowCoordinator                                    │  │
│  │  ├─ TCP Listener (0.0.0.0:PGCOPYDB_PORT)           │  │
│  │  ├─ IPC Listener (Unix socket, optional future)     │  │
│  │  ├─ Sentinel state cache                            │  │
│  │  └─ Message handler thread/non-blocking I/O         │  │
│  └──────────────────────────────────────────────────────┘  │
│           ▲                      ▲                          │
│           │ Messages             │                          │
│           │                      │                          │
│  ┌────────┴──────┐      ┌────────┴──────┐      ┌────────┐  │
│  │  Receive      │      │  Transform    │      │ Apply  │  │
│  │  (stream in)  │      │  (convert)    │      │ (out)  │  │
│  └───────────────┘      └───────────────┘      └────────┘  │
│                                                             │
└────────────────────────────────────────────────────────────┘
         ▲                                      ▲
         │ TCP/IP                              │ TCP/IP
         │                                      │
    ┌────┴─────┐                         ┌──────┴────┐
    │   CLI    │                         │ Monitoring│
    │ Commands │                         │  System   │
    └──────────┘                         └───────────┘
```

### Coordinator Responsibilities

1. **Socket Management**
   - Create and listen on TCP socket(s)
   - Accept client connections
   - Handle socket lifecycle (connect, read, write, close)
   - Non-blocking operations with timeout handling

2. **Message Handling**
   - Parse incoming IPC messages from clients
   - Validate message format and semantics
   - Route to appropriate handler
   - Send responses back to clients

3. **State Management**
   - Cache sentinel state (startpos, endpos, LSNs, apply flag)
   - Sync with SQLite sentinel table on demand
   - Provide atomic read/write operations

4. **Command Processing**
   - SET_APPLY: Enable/disable apply process
   - SET_ENDPOS: Set or update end position
   - QUERY_STATUS: Return pipeline status
   - Future: PAUSE, RESUME, SNAPSHOT, etc.

## Components

### 1. FollowCoordinator Structure

```c
typedef struct {
    /* Network listeners */
    IPCConn tcp_listen;              /* TCP socket listener */
    
    /* Coordinator state (cached from sentinel) */
    uint64_t sentinel_startpos;
    uint64_t sentinel_endpos;
    uint64_t sentinel_write_lsn;
    uint64_t sentinel_transform_lsn;
    uint64_t sentinel_flush_lsn;
    uint64_t sentinel_replay_lsn;
    bool     sentinel_apply;          /* Apply process enabled */
    
    /* Connected clients (circular buffer or list) */
    FollowCoordinatorClient *clients;
    int max_clients;
    int num_clients;
    
    /* Throttling for sentinel updates */
    time_t last_sentinel_update;
    uint64_t last_update_lsn;
    int update_interval_sec;
    
    /* Configuration */
    char listen_host[256];            /* IP to listen on */
    int listen_port;                  /* TCP port number */
    bool enabled;                     /* Coordinator active */
} FollowCoordinator;
```

### 2. Client Connection Structure

```c
typedef struct {
    int fd;                           /* Socket file descriptor */
    char remote_addr[64];             /* Client IP:port for logging */
    time_t connected_at;              /* Connection timestamp */
    
    /* Input buffer for reading messages */
    uint8_t recv_buffer[IPC_MAX_PAYLOAD_SIZE + 4];
    size_t recv_bytes;
    
    /* Output buffer for sending responses */
    uint8_t send_buffer[IPC_MAX_PAYLOAD_SIZE + 4];
    size_t send_bytes;
    bool send_pending;
    
    /* State tracking */
    enum {
        CLIENT_HANDSHAKE,
        CLIENT_READY,
        CLIENT_READING,
        CLIENT_WRITING
    } state;
} FollowCoordinatorClient;
```

### 3. Message Handler Architecture

```c
typedef bool (*CoordinatorMessageHandler)(
    FollowCoordinator *coord,
    StreamSpecs *specs,
    IPCMessage *request,
    IPCMessage *response
);

typedef struct {
    uint8_t message_type;
    CoordinatorMessageHandler handler;
    const char *name;
} CoordinatorCommandHandler;
```

Handlers:
- `handle_set_apply()` - Enable/disable apply process
- `handle_set_endpos()` - Update endpos value
- `handle_query_status()` - Return current pipeline status
- `handle_query_sentinel()` - Return full sentinel state

## Integration Points

### 1. Follow Process Initialization (`follow.c:followDB`)

**Current behavior (to change):**
- Starts receive, transform, apply subprocesses
- Waits for all to complete

**New behavior:**
- Before starting subprocesses:
  ```c
  if (!follow_coordinator_init(&coordinator, 
                               specs->paths.workdir,
                               listen_host,
                               listen_port)) {
      log_warn("Failed to start TCP coordinator, continuing without it");
      coord_enabled = false;
  }
  ```
- After starting subprocesses:
  ```c
  while (!all_done) {
      // Check subprocesses for signals
      // NEW: Handle coordinator messages (non-blocking)
      if (coord_enabled) {
          if (!follow_coordinator_handle_messages(&coordinator, specs)) {
              log_error("Coordinator error, but continuing...");
          }
      }
  }
  ```
- Before returning:
  ```c
  if (coord_enabled) {
      follow_coordinator_shutdown(&coordinator);
  }
  ```

### 2. Sentinel Updates

When sentinel state changes (via coordinator command), update the SQLite sentinel table:

```c
bool follow_coordinator_update_sentinel(FollowCoordinator *coord,
                                        StreamSpecs *specs)
{
    // Throttle updates (similar to existing logic)
    if (!time_for_update(coord)) {
        return true;
    }
    
    // Write to SQLite atomically
    if (!sentinel_update_endpos(specs->sourceDB, 
                                coord->sentinel_endpos)) {
        log_error("Failed to update sentinel");
        return false;
    }
    
    // Update cached state
    coord->last_sentinel_update = time(NULL);
    
    return true;
}
```

### 3. CLI Command Integration (`cli_sentinel.c`)

When `PGCOPYDB_HOST` or `PGCOPYDB_PORT` environment variables are set, CLI commands route through TCP:

```c
// In cli_sentinel_set_endpos()

ServiceEndpoint service = ld_service_get_endpoint();
if (service.enabled) {
    // Send via TCP to coordinator
    IPCMessage request = {0};
    IPC_INIT_MESSAGE(request, IPC_MSG_SET_ENDPOS);
    IPCPayloadSetEndpos *cmd = (IPCPayloadSetEndpos *)request.payload;
    cmd->endpos_lsn = endpos;
    snprintf(cmd->reason, sizeof(cmd->reason), 
             "CLI: pgcopydb sentinel set endpos");
    request.payload_len = sizeof(IPCPayloadSetEndpos);
    
    IPCMessage response = {0};
    if (!ld_service_send_command(service, &request, &response)) {
        log_error("Failed to update coordinator");
        return false;
    }
    
    if (response.type == IPC_MSG_ERROR) {
        log_error("Coordinator error: %s", response.payload);
        return false;
    }
} else {
    // Direct SQLite update (fallback)
    if (!sentinel_update_endpos(sourceDB, endpos)) {
        return false;
    }
}
```

## Listener Configuration

### Default Behavior (PGCOPYDB_HOST not set)

- TCP Listener: `0.0.0.0:5442` (all interfaces)
- Reason: Coordinator is an internal service, not exposed to untrusted networks
- Security: In production, firewall rules control access (only allow from known IPs)

### Explicit Configuration (PGCOPYDB_HOST set)

- TCP Listener: `${PGCOPYDB_HOST}:${PGCOPYDB_PORT:-5442}`
- Example: `PGCOPYDB_HOST=172.20.0.2 PGCOPYDB_PORT=5443`

### Initialization Logic

```c
bool follow_coordinator_init(FollowCoordinator *coord,
                             const char *workdir,
                             const char *host,
                             int port)
{
    memset(coord, 0, sizeof(FollowCoordinator));
    
    // Determine listen address
    const char *listen_host = host ? host : "0.0.0.0";
    int listen_port = port > 0 ? port : 5442;
    
    sformat(coord->listen_host, sizeof(coord->listen_host), 
            "%s", listen_host);
    coord->listen_port = listen_port;
    
    // Create TCP listener
    if (!ld_ipc_tcp_listen(&coord->tcp_listen, 
                           listen_host, listen_port)) {
        log_error("Failed to bind TCP listener at %s:%d",
                  listen_host, listen_port);
        return false;
    }
    
    // Initialize client array
    coord->max_clients = MAX_COORDINATOR_CLIENTS;  // e.g., 10
    coord->clients = calloc(coord->max_clients, 
                            sizeof(FollowCoordinatorClient));
    if (!coord->clients) {
        log_error("Memory allocation failed");
        return false;
    }
    
    // Initialize throttling
    coord->update_interval_sec = 5;
    coord->last_sentinel_update = time(NULL);
    
    coord->enabled = true;
    
    log_info("TCP Coordinator listening at %s:%d", 
             listen_host, listen_port);
    
    return true;
}
```

## Message Flow

### Example: `pgcopydb sentinel set endpos`

```
┌─────────────────────────────────────────────────────────┐
│ User runs: pgcopydb sentinel set endpos --current       │
└─────────────────────────────────────────────────────────┘
           │
           ├─ Check PGCOPYDB_HOST env var
           │   └─ If set, use TCP path:
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│ ld_service_send_command()                               │
│  ├─ ld_ipc_tcp_connect(coordinator_host:coordinator_port)
│  ├─ ld_ipc_send_message(IPC_MSG_SET_ENDPOS)
│  ├─ ld_ipc_recv_message() [wait for response]
│  └─ ld_ipc_close()
└─────────────────────────────────────────────────────────┘
           │ TCP packet
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│ follow_coordinator_handle_messages()                     │
│  ├─ ld_ipc_tcp_accept() [accept connection]
│  ├─ ld_ipc_recv_message(IPC_MSG_SET_ENDPOS)
│  └─ Call handle_set_endpos()
└─────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│ handle_set_endpos()                                     │
│  ├─ Validate endpos value
│  ├─ Check if apply is enabled
│  ├─ Call sentinel_update_endpos(sourceDB, endpos)
│  ├─ Update coordinator->sentinel_endpos
│  └─ Send IPC_MSG_ACK response
└─────────────────────────────────────────────────────────┘
           │ TCP packet
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│ CLI receives ACK, returns success                        │
└─────────────────────────────────────────────────────────┘
```

## Non-Blocking Event Loop

The coordinator operates in a non-blocking event loop integrated into the main follow process:

```c
bool follow_coordinator_handle_messages(FollowCoordinator *coord,
                                        StreamSpecs *specs)
{
    if (!coord->enabled) return true;
    
    /* Accept new connections (non-blocking) */
    if (coord->num_clients < coord->max_clients) {
        FollowCoordinatorClient *client = 
            &coord->clients[coord->num_clients];
        
        if (ld_ipc_tcp_accept(&coord->tcp_listen, 
                              &client->conn, 0)) {  // 0 = non-blocking
            client->fd = client->conn.fd;
            coord->num_clients++;
            log_debug("Coordinator: new client connection");
        }
    }
    
    /* Handle messages from all connected clients */
    for (int i = 0; i < coord->num_clients; ++i) {
        FollowCoordinatorClient *client = &coord->clients[i];
        
        if (client->fd < 0) continue;
        
        /* Try to read message (non-blocking) */
        IPCMessage request = {0};
        if (ld_ipc_recv_message(&client->conn, &request, 0)) {
            IPCMessage response = {0};
            
            /* Route to handler */
            if (!handle_coordinator_message(coord, specs, 
                                            &request, &response)) {
                response.type = IPC_MSG_ERROR;
                snprintf(response.payload, 
                         IPC_MAX_PAYLOAD_SIZE,
                         "Handler failed");
            }
            
            /* Send response (may queue if buffer full) */
            ld_ipc_send_message(&client->conn, &response);
        }
        
        /* Detect disconnections and clean up */
        if (!ld_ipc_is_alive(&client->conn)) {
            ld_ipc_close(&client->conn);
            client->fd = -1;
            log_debug("Coordinator: client disconnected");
        }
    }
    
    /* Periodically sync sentinel state from database */
    if (!follow_coordinator_sync_sentinel(coord, specs)) {
        log_warn("Failed to sync sentinel state");
    }
    
    return true;
}
```

## Error Handling

### Client Errors

- **Invalid message format:** Send `IPC_MSG_ERROR` with reason
- **Handler failure:** Send `IPC_MSG_ERROR` with handler-specific message
- **Connection timeout:** Close connection after 30 seconds inactivity
- **Buffer overflow:** Log error, close connection, continue

### Coordinator Errors

- **Sentinel update fails:** Log, reply with error, continue (don't crash)
- **Socket error on accept:** Log and continue (socket still functional)
- **Socket error on send/recv:** Close client connection, continue

### Principle

> **Coordinator failures should not crash the main pipeline.** The pipeline can operate without the coordinator; it just won't accept remote commands.

## Testing Strategy

### Unit Tests

```c
// test/unit/test_coordinator.c

void test_coordinator_init_with_env()
{
    setenv("PGCOPYDB_HOST", "127.0.0.1", 1);
    setenv("PGCOPYDB_PORT", "6442", 1);
    
    FollowCoordinator coord = {0};
    assert(follow_coordinator_init(&coord, "/tmp", 
                                   "127.0.0.1", 6442));
    assert(strcmp(coord.listen_host, "127.0.0.1") == 0);
    assert(coord.listen_port == 6442);
    
    follow_coordinator_shutdown(&coord);
}

void test_coordinator_init_defaults()
{
    FollowCoordinator coord = {0};
    assert(follow_coordinator_init(&coord, "/tmp", NULL, 0));
    assert(strcmp(coord.listen_host, "0.0.0.0") == 0);
    assert(coord.listen_port == 5442);
    
    follow_coordinator_shutdown(&coord);
}

void test_handle_set_endpos()
{
    // Create test coordinator and specs
    // Send SET_ENDPOS message
    // Verify sentinel updated
    // Verify ACK response sent
}
```

### Integration Tests

```bash
# Test with docker-compose
docker-compose up &

# CLI sends command via TCP
PGCOPYDB_HOST=localhost PGCOPYDB_PORT=5442 \
  pgcopydb sentinel set endpos 0/24088A8

# Verify sentinel updated
psql -c "SELECT endpos FROM pgcopydb.sentinel"
```

### Load Tests

- Multiple concurrent clients
- Rapid-fire commands
- Large payload messages
- Client connection/disconnection cycles

## Deployment Considerations

### Docker Compose

```yaml
services:
  pgcopydb:
    environment:
      - PGCOPYDB_HOST=0.0.0.0  # Listen on all interfaces
      - PGCOPYDB_PORT=5442
    ports:
      - "5442:5442"  # Expose TCP port
    # NO volume mounts needed!
```

### Kubernetes

```yaml
apiVersion: v1
kind: Service
metadata:
  name: pgcopydb-coordinator
spec:
  selector:
    app: pgcopydb
  ports:
  - protocol: TCP
    port: 5442
    targetPort: 5442
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pgcopydb
spec:
  template:
    spec:
      containers:
      - name: pgcopydb
        env:
        - name: PGCOPYDB_HOST
          value: "0.0.0.0"
        - name: PGCOPYDB_PORT
          value: "5442"
        ports:
        - containerPort: 5442
```

### Security Notes

1. **Coordinator always listens on `0.0.0.0:5442` by default** - This is safe because:
   - It's an internal service (assumes trusted container network)
   - Access control via firewall/network policy
   - Future: Add authentication/TLS support

2. **Client authentication** (Future enhancement):
   - API key in environment variable
   - TLS mutual authentication
   - JWT tokens

## Future Enhancements

### Phase 2
- [ ] Unix socket support for local clients (IPC_LISTEN in addition to TCP)
- [ ] Client authentication via API key
- [ ] TLS encryption for remote connections
- [ ] Client subscription to coordinator events (e.g., endpos changed)

### Phase 3
- [ ] Coordinator metrics export (prometheus-compatible)
- [ ] Status dashboard (HTTP API)
- [ ] Command audit logging
- [ ] High-availability coordinator (multi-instance with failover)

### Phase 4
- [ ] Coordinator-to-coordinator replication (multi-zone CDC)
- [ ] Advanced rate limiting and backpressure
- [ ] Client-side caching of sentinel state

## Summary of Files

| File | Purpose | Lines |
|------|---------|-------|
| `src/bin/pgcopydb/follow_coordinator.h` | Coordinator types and API | 50 |
| `src/bin/pgcopydb/follow_coordinator.c` | Coordinator implementation | 400 |
| `src/bin/pgcopydb/follow.c` | Integration into main loop | +50 changes |
| `src/bin/pgcopydb/ld_service.c` | CLI client support | existing |
| `src/bin/pgcopydb/cli_sentinel.c` | CLI command routing | +30 changes |
| `tests/unit/test_coordinator.c` | Unit tests | 200 |
| `tests/integration/cdc-coordinator.sh` | Integration tests | 150 |

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-05  
**Status:** Design Phase (Ready for Implementation)
