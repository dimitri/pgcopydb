# CDC Coordinator Architecture

## Overview

This document describes the Inter-Process Communication (IPC) and coordinator architecture for the SQLite-based Change Data Capture (CDC) pipeline in pgcopydb.

## Current Implementation (Phase 1: Fixed)

### Problem Solved

**The ENDPOS Detection Bug:**
When `endpos` was set to a point in the middle of a transaction, the transform process would hang indefinitely instead of exiting. Investigation revealed:

1. Receive process correctly inserted an ENDPOS message at exactly the endpos LSN
2. Transform process queried the output table using `WHERE lsn > transform_lsn` (strictly greater-than)
3. When ENDPOS was at exactly the boundary, the > comparison would skip it
4. Transform would loop forever, polling with no timeout

**The Fix:**
Changed `ld_store_lookup_output_after_lsn()` SQL query to use `>=` instead of `>` for non-BEGIN rows:

```c
/* Before (buggy): */
"where lsn > $2"

/* After (fixed): */
"where lsn >= $2 and action != 'B'"
```

File: `src/bin/pgcopydb/ld_store.c` line ~475

### Files Modified

- **ld_store.c** (critical fix): Changed ENDPOS detection query condition
- **ld_stream.c**: Added include for ld_ipc.h (foundation for future)
- **cli_sentinel.c**: Added service endpoint detection (foundation for future)

## Future Implementation (Phase 2-3: IPC Foundation)

### Architecture

The following foundation is in place for future enhancement:

```
┌─────────────────────────────────┐
│  pgcopydb follow (coordinator)  │
│  ├─ Unix socket listener        │
│  ├─ TCP socket listener (5442)  │
│  └─ Sentinel update manager     │
└──────┬──────────────────────────┘
       │ Unix socket
       ├─────────────────┐
       │                 │
▼      ▼                 ▼
Receive Transform    Apply
```

### IPC Components

**New Files (Not yet integrated but ready):**

1. **ld_ipc.h / ld_ipc.c**
   - Binary protocol for Unix and TCP sockets
   - Message types (PING, ACK, ENDPOS_REACHED, SET_ENDPOS, etc.)
   - Non-blocking send/recv with timeout support
   - Platform-independent socket operations

2. **follow_coordinator.h / follow_coordinator.c**
   - Coordinator context and event loop
   - Message handler for IPC and TCP connections
   - Sentinel update throttling (5-second min interval or on endpos change)
   - Progress reporting from receive/transform/apply

3. **ld_service.h / ld_service.c**
   - Service endpoint configuration (PGCOPYDB_HOST, PGCOPYDB_PORT env vars)
   - CLI client for remote sentinel updates
   - Default port: 5442

### Protocol Overview

**Message Format:**
```
[version:1 byte][type:1 byte][length:2 bytes][payload:N bytes]
```

**Key Message Types:**
- `IPC_MSG_SET_ENDPOS`: CLI → Coordinator (set endpos value)
- `IPC_MSG_ENDPOS_REACHED`: Receive → Coordinator (ENDPOS inserted)
- `IPC_MSG_QUERY_STATUS`: CLI → Coordinator (get pipeline status)
- `IPC_MSG_ACK`: Coordinator → Sender (confirm receipt)

## Environment Variables

### PGCOPYDB_HOST (Optional)
- Default: `localhost`
- Set to enable service endpoint for remote sentinel commands
- Example: `PGCOPYDB_HOST=coordinator-node pgcopydb sentinel set endpos`

### PGCOPYDB_PORT (Optional)
- Default: `5442`
- Coordinator listens on this port for CLI commands
- Must be open between injection node and coordinator node

## Testing

### Regression Test: CDC ENDPOS Mid-Transaction

Test: `make tests/cdc-endpos-mid-txn`

This test verifies that transform process correctly detects ENDPOS when:
- Endpos is set to a point in the middle of a transaction
- Receive process completes and writes ENDPOS marker
- Transform should exit cleanly instead of timing out

Expected behavior:
- Test completes in <30 seconds
- Transform process exits with "endpos reached" message
- All data properly applied to target database

## Integration Steps (For Future)

When integrating the coordinator architecture:

1. **Modify follow.c:**
   - Call `follow_coordinator_init()` before starting subprocesses
   - Add coordinator event handling in main loop
   - Call `follow_coordinator_shutdown()` on exit

2. **Modify ld_stream.c:**
   - Add IPC connect in receive process
   - Report progress via `IPC_MSG_DATA_AVAILABLE`
   - Report completion via `IPC_MSG_ENDPOS_REACHED`

3. **Modify ld_transform.c / ld_apply.c:**
   - Add IPC reporting for progress tracking
   - (Optional) Add liveness checks via PING/ACK

4. **Update Docker Compose:**
   - Expose port 5442 for coordinator service
   - Set PGCOPYDB_HOST/PORT environment variables

5. **Update inject.sh:**
   - No changes needed! Existing `pgcopydb sentinel set endpos` commands automatically use service endpoint when `PGCOPYDB_HOST` is set.

## Performance Considerations

### Throttling
The coordinator throttles sentinel updates to avoid excessive writes:
- Minimum 5 seconds between updates
- OR when endpos is set (urgent)
- OR when >1MB of data has been processed

This prevents I/O saturation while maintaining reasonable progress visibility.

### Socket Buffers
- Unix sockets: local machine, small buffer (default OS)
- TCP sockets: may have network latency, configurable (SO_SNDBUF, SO_RCVBUF)

All IPC operations are non-blocking with timeouts to prevent hangs.

## Debugging

Enable debug logging:
```bash
PGCOPYDB_LOG_LEVEL=debug pgcopydb follow --listen 0.0.0.0:5442
```

Log messages include:
- Socket bind/listen events
- Connection accepts
- Message send/receive operations
- Sentinel update operations

## References

- Core fix: `src/bin/pgcopydb/ld_store.c` line 475
- IPC protocol: `src/bin/pgcopydb/ld_ipc.h`
- Test: `tests/cdc-endpos-mid-txn/`
