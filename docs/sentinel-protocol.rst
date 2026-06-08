.. _sentinel_protocol:

Sentinel Control — Design Considerations
========================================

This document describes how the ``pgcopydb stream sentinel`` commands
communicate with a running ``pgcopydb clone --follow`` / ``pgcopydb follow``
process, and why pgcopydb offers an optional TCP transport for that control
channel in addition to the default SQLite catalog.

It also documents a second, internal IPC channel of the follow pipeline: the
small lifecycle signal that the ``receive`` and ``apply`` workers use to
coordinate a clean shutdown (see :ref:`pipe_protocol`).

---

The sentinel and how it is read/written
---------------------------------------

The *sentinel* is a single row stored in the source SQLite catalog
(``schema/source.db``). It carries the user-controllable streaming knobs —
``startpos``, ``endpos``, the ``apply`` flag — and the pipeline progress LSNs
(``write_lsn``, ``flush_lsn``, ``replay_lsn``). A running follow process polls
the sentinel to learn when to enable apply and where to stop; an operator (or a
test harness) updates it with ``pgcopydb stream sentinel set …``.

Two transports are available:

- **SQLite (default).** The ``stream sentinel`` command opens the source
  catalog directly and reads/writes the sentinel row. This requires the command
  to run where the catalog files live (same host/container, or a shared
  filesystem / docker volume).
- **TCP (opt-in, ``--host``/``--port``).** The command connects to the follow
  process' coordinator over TCP; the follow side performs the SQLite update on
  its behalf. The client never opens the catalog files.

---

Why a TCP transport: the SQLite write-locking constraint
--------------------------------------------------------

pgcopydb serialises concurrent SQLite writers with a System V semaphore created
via ``semget(IPC_PRIVATE, …)`` (see ``lock_utils.c``). ``IPC_PRIVATE`` produces
a semaphore that is identified only by the returned ``semId`` and is therefore
shareable **only by processes that inherit it across ``fork()``**. An
independently launched process — for example a separately invoked
``pgcopydb stream sentinel set endpos`` — calls ``semget(IPC_PRIVATE, …)`` again
and gets a *different* semaphore.

Consequences:

- The ``follow`` supervisor and its forked ``receive`` / ``apply`` children all
  share the same write semaphore and coordinate cleanly.
- An independently invoked ``stream sentinel`` CLI does **not** share that
  semaphore. With the SQLite transport it relies solely on SQLite's own WAL
  file locking, which requires the catalog to be on a shared filesystem and is
  prone to ``SQLITE_BUSY`` under contention. Sharing the catalog across
  containers (a named docker volume) is the usual workaround.

The TCP transport removes that requirement: the CLI sends a request over the
network and the **follow process** applies the sentinel change using the same
shared semaphore as the rest of the pipeline. No catalog files are shared.

---

Where the coordinator runs: in the follow supervisor
----------------------------------------------------

The coordinator must live inside the follow process group to participate in the
``IPC_PRIVATE`` semaphore. It runs **in-process, inside the follow supervisor**
(the process that already holds the source catalog open for
``follow_reached_endpos`` and that forks/monitors ``receive`` and ``apply``):

- it reuses the supervisor's already-open ``sourceDB`` handle and its
  ``semId`` — adding **no** new SQLite connection or lock participant;
- it serves requests with short, non-blocking timeouts (a 100 ms ``accept``
  folded into the supervisor's monitoring loop), so the endpos / child-exit
  detection stays responsive.

A dedicated coordinator subprocess would also work (it would inherit the
``semId`` across ``fork``) but would open a second SQLite connection on the same
file for no real benefit; in-process is preferred.

The coordinator is **optional**: it is started only when a listen endpoint is
configured, via ``--host`` / ``--port`` on ``clone --follow`` / ``follow`` /
``stream replay``, or via the ``PGCOPYDB_HOST`` / ``PGCOPYDB_PORT`` environment
variables (a convenience for ``docker-compose``).

---

Wire protocol
-------------

A minimal request/response protocol (``ld_ipc.h``). Each message is
``[version:1][type:1][payload_len:2][payload:N]``.

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Message
     - Direction
     - Payload
   * - ``PING`` / ``PONG``
     - both
     - — (liveness check; used by clients to wait for the coordinator)
   * - ``SET_STARTPOS``
     - CLI → coord
     - ``{ uint64 startpos_lsn; char reason[256]; }``
   * - ``SET_ENDPOS``
     - CLI → coord
     - ``{ uint64 endpos_lsn; char reason[256]; }``
   * - ``SET_APPLY``
     - CLI → coord
     - ``{ uint8 apply; }`` (1 = apply, 0 = prefetch)
   * - ``QUERY_SENTINEL``
     - CLI → coord
     - — ; reply ``SENTINEL_REPLY`` carries the full ``CopyDBSentinel``
   * - ``ACK_CONFIRMED`` / ``ERROR``
     - coord → CLI
     - request accepted, or an error string

The coordinator answers every request by reading/writing SQLite
(``sentinel_get`` / ``sentinel_update_startpos`` / ``…_endpos`` /
``…_apply``) — it does **not** trust an in-memory copy, because the live
``write/flush/replay_lsn`` values are maintained by the ``receive`` / ``apply``
children and are stale in the supervisor.

---

CLI client behaviour
--------------------

``pgcopydb stream sentinel get`` and ``set startpos|endpos|apply|prefetch``
choose the transport explicitly:

- with ``--host`` (and optional ``--port``, default ``5442``): connect to the
  coordinator over TCP; the catalog is **not** opened. If the coordinator is
  unreachable the command fails (no silent fallback).
- without ``--host``: open the source SQLite catalog directly (the default).

``pgcopydb stream sentinel setup`` always uses SQLite — it bootstraps the
sentinel table itself.

Hostnames are resolved with ``getaddrinfo``, so ``--host`` accepts a
docker-compose service name (e.g. ``--host test``) as well as a numeric address.

---

Testing without a shared volume
-------------------------------

The ``follow-*`` tests exercise this: the ``test`` service runs
``clone --follow`` / ``follow`` with ``PGCOPYDB_HOST=0.0.0.0`` so the
coordinator listens, and a separate ``inject`` service drives the sentinel with
``--host test --port 5442``. Because the two containers no longer share the
SQLite catalog volume, a passing test demonstrates the TCP path end-to-end (the
CLI cannot fall back to SQLite). ``docker compose run --use-aliases`` is used so
the one-off ``run`` container is reachable on the compose network as ``test``.

.. _pipe_protocol:

The receive→apply lifecycle pipe
--------------------------------

Separate from the sentinel control channel described above, the ``follow``
pipeline uses one more, much smaller IPC channel between its two worker
processes. It is worth describing here because, like the sentinel transport, it
is a deliberate design choice rather than an incidental detail.

In the SQLite CDC model the change data itself never travels through this
channel. The ``receive`` worker records decoded changes into the *output* store
and the ``apply`` worker reads from there, transforms the rows inline, and
writes them to the target; concurrent access to those stores is serialised by
the shared write semaphore (the same one discussed above). What the two workers
still need is a way for ``apply`` to learn, with minimal latency, that
``receive`` has reached the end position and will produce no further changes.

That single fact is delivered over a one-way pipe from ``receive`` to ``apply``.
The pipe carries exactly one message for its whole lifetime: the final LSN that
``receive`` stopped at — in effect, *"I am done, at position X"*. ``apply`` waits
on the pipe while it drains the store, so it wakes immediately when the signal
arrives instead of discovering completion by polling.

This follows the pattern PostgreSQL uses for postmaster-death detection — the
"death watch" pipe behind ``PostmasterIsAlive()``. The upstream process holds
the write end open for its entire run and closes it on exit, while the
downstream process watches the read end for readiness:

- a readable pipe **with data** is the normal *"done at LSN X"* hand-off;
- a closed pipe **with no data** (end-of-file) means the upstream went away
  unexpectedly.

pgcopydb layers the final-LSN payload on top of that bare death-watch so that
``apply`` can also drain cleanly up to the right transaction boundary, rather
than merely learning *that* the upstream is gone.

The pipe is purely a latency optimisation, and it exists only when ``receive``
and ``apply`` run together under the same follow supervisor. When ``apply`` (or
``stream catchup``) runs on its own, there is no live pipe; it instead consults
the durable pipeline-progress record that ``receive`` leaves behind in the
source catalog to decide when the upstream has finished. Unexpected upstream
death is, in the live case, ultimately caught by the supervisor monitoring its
children, with the pipe end-of-file serving as a belt-and-suspenders fallback.
