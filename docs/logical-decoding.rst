.. _logical_decoding_internals:

Logical Decoding — Design Considerations
=========================================

This document records the detailed understanding of PostgreSQL logical decoding
LSN semantics, the replication protocol, and how pgcopydb exploits that
understanding to track progress and support user-defined ``endpos`` values
safely.

---

PostgreSQL WAL LSN semantics
-----------------------------

WAL record layout
~~~~~~~~~~~~~~~~~

PostgreSQL WAL is a packed byte stream.  Every record has two canonical
positions:

- **ReadRecPtr** — byte offset of the record's first byte, the "LSN" of that
  record.
- **EndRecPtr** — byte offset of the first byte immediately after the record
  (= ``ReadRecPtr`` of the next record, assuming MAXALIGN padding).

Because records are packed with no gaps (beyond alignment), consecutive WAL
records share a byte boundary::

    Record N starts at ReadRecPtr_N.
    Record N ends   at EndRecPtr_N  = ReadRecPtr_{N+1}.

Reorderbuffer fields for a logical transaction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``ReorderBufferTXN`` struct (``reorderbuffer.h``) tracks three LSN values
per decoded transaction:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Field
     - Meaning
   * - ``first_lsn``
     - ``ReadRecPtr`` of the first WAL record belonging to this XID (the BEGIN
       record)
   * - ``final_lsn``
     - ``ReadRecPtr`` of the COMMIT record ("beginning of commit record")
   * - ``end_lsn``
     - ``EndRecPtr`` of the COMMIT record = ``ReadRecPtr`` of the NEXT WAL
       record

The relationship ``end_lsn = final_lsn + sizeof(COMMIT_record)`` always holds.
``end_lsn`` is one byte past the COMMIT, which equals the start of whatever WAL
record follows.

---

LSN values on the replication protocol wire
--------------------------------------------

When the logical decoding plugin callbacks fire, ``ctx->write_location`` is set
differently for BEGIN and COMMIT:

.. code-block:: c

    /* logical.c: begin_cb_wrapper */
    ctx->write_location = txn->first_lsn;   /* start of BEGIN record */

    /* logical.c: commit_cb_wrapper */
    ctx->write_location = txn->end_lsn;     /* byte past end of COMMIT */

``WalSndPrepareWrite`` (``walsender.c``) embeds ``ctx->write_location`` as the
``dataStart`` field of the XLogData protocol message.  The replication client
reads ``dataStart`` as ``cur_record_lsn``.

pgcopydb does **not** pass ``include-lsn=true`` to wal2json, so wal2json does
not embed an ``"lsn"`` field in its JSON output.  pgcopydb uses the raw
protocol ``dataStart`` value as ``metadata->lsn`` for every message.

The result for consecutive transactions N and N+1::

    COMMIT_N  delivered at dataStart = txn_N->end_lsn
    BEGIN_{N+1} delivered at dataStart = txn_{N+1}->first_lsn

Because ``txn_N->end_lsn = EndRecPtr(COMMIT_N) = ReadRecPtr(next record) =
txn_{N+1}->first_lsn``::

    COMMIT_N.lsn  ==  BEGIN_{N+1}.lsn          ← LSN COLLISION, always

This is not an edge case — it is the guaranteed layout whenever two
transactions are consecutive in WAL.

---

The LSN collision and how pgcopydb handles it
----------------------------------------------

The ``ld_store_lookup_output_after_lsn`` cursor uses a union query that handles
the collision explicitly:

.. code-block:: sql

    SELECT … FROM (
      SELECT … FROM output WHERE lsn >= $1 AND action = 'B'
      UNION ALL
      SELECT … FROM output WHERE lsn >  $2 AND action IN ('K','X')
    )
    ORDER BY lsn, CASE WHEN action='B' THEN 0 ELSE 1 END, id
    LIMIT 1

The ``>=`` (not ``>``) for BEGIN rows is intentional::

    After processing COMMIT_N:  transform_lsn = end_lsn_N = first_lsn_{N+1}

      BEGIN_N    has lsn = first_lsn_N  < end_lsn_N  → excluded by >= ✓
      BEGIN_{N+1} has lsn = first_lsn_{N+1} = end_lsn_N → included by >= ✓

KEEPALIVE rows use strict ``>`` because they can share an LSN with the
preceding COMMIT without being the next transaction's BEGIN.

---

``transform_lsn`` — the safe restart point
-------------------------------------------

``sentinel.transform_lsn`` is set to ``COMMIT.end_lsn`` (= ``txn->end_lsn``)
at the end of every committed transaction.  This value is reported to
PostgreSQL as ``flush_lsn`` via:

.. code-block:: c

    /* ld_stream.c: stream_sync_sentinel */
    if (sentinel.transform_lsn != InvalidXLogRecPtr)
        context->tracking->flushed_lsn = sentinel.transform_lsn;

PostgreSQL advances ``confirmed_flush_lsn = flushed_lsn`` via
``LogicalConfirmReceivedLocation`` (which never decreases).  On reconnect, WAL
streaming restarts from ``max(requested_start, confirmed_flush)``.

With ``confirmed_flush = end_lsn_N = first_lsn_{N+1}``:

- Streaming resumes exactly at the first byte of the next unprocessed WAL
  record — the BEGIN of transaction N+1.
- The ``>=`` cursor finds ``BEGIN_{N+1}`` (lsn = ``first_lsn_{N+1}`` =
  ``end_lsn_N``). ✓
- No data is lost.  No data is replayed twice.

The fundamental invariant:

.. admonition:: transform_lsn invariant

   ``transform_lsn`` must only advance to ``COMMIT.end_lsn`` — the byte
   immediately after a completed COMMIT record.  It must never advance to a
   position inside an uncommitted transaction (past a BEGIN but before the
   corresponding COMMIT), because doing so would make the logical replication
   slot skip that transaction on reconnect.

---

LSN+XID watermark
------------------

pgcopydb tracks two complementary watermarks:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Watermark
     - Field
     - Advances at
   * - ``transform_lsn``
     - ``sentinel.transform_lsn``
     - COMMIT/ROLLBACK/KEEPALIVE (commit boundary)
   * - ``last_xid``
     - ``pipeline_state.last_xid``
     - Same events

``(transform_lsn, last_xid)`` together form a complete restart descriptor: "the
last fully processed transaction was XID X, whose COMMIT ended at LSN L."
``last_xid`` is stored for debugging and cross-process assertions; only
``transform_lsn`` drives the PostgreSQL slot position.

---

``replay_lsn`` — the apply watermark
--------------------------------------

``sentinel.replay_lsn`` is the LSN of the last transaction committed to the
**target** PostgreSQL database by the apply process.  It is updated by
``sentinel_sync_apply()``, called only after ``pgsql_execute(COMMIT)`` returns
successfully.

``replay_lsn`` is used for:

1. **``follow_reached_endpos``** — detecting that endpos has been applied.
2. **Monitoring** — pgcopydb progress display.

It is NOT the apply restart point.  On restart, ``setupReplicationOrigin``
reads ``pg_replication_origin_progress()`` (the durable Postgres origin
catalog) and unconditionally overwrites ``context->previousLSN``.
``sentinel.replay_lsn`` plays no role in the restart cursor.

.. admonition:: replay_lsn invariant

   ``replay_lsn`` only advances to a LSN that has been confirmed durable by
   the target PostgreSQL server (i.e., after a successful ``COMMIT`` that
   included ``pg_replication_origin_xact_setup``).

KEEPALIVE processing commits ``SELECT txid_current()`` with
``pg_replication_origin_xact_setup(previousLSN)`` (the last data commit, not
``KEEPALIVE.lsn``).  After that Postgres COMMIT, ``context->previousLSN`` is
advanced to ``KEEPALIVE.lsn`` and ``replay_lsn`` follows.  This is acceptable:
the Postgres origin records the last data commit; ``replay_lsn`` records the
WAL progress beacon.

---

Why pgcopydb supports endpos mid-transaction
---------------------------------------------

The user-supplied ``--endpos`` LSN is a raw WAL position snapshotted with
``pg_current_wal_lsn()``.  PostgreSQL makes no guarantee that this snapshot
lands at a transaction boundary; it can fall:

- **Before any open transaction** (between consecutive committed transactions)
- **Inside an uncommitted transaction** (past a BEGIN but before its COMMIT)
- **Exactly at a COMMIT boundary** (rarest case; ``COMMIT.end_lsn``)

Rejecting non-boundary endpos values would force operators to coordinate
with source workload — impractical.  Instead pgcopydb handles all three cases:

.. list-table::
   :header-rows: 1
   :widths: 20 30 30 20

   * - Case
     - Condition
     - apply action
     - ``replay_lsn``
   * - At boundary
     - ``endpos = COMMIT.end_lsn``
     - stop after that commit
     - ``= endpos`` ✓
   * - Between txns (Guard 2)
     - ``endpos < beginLSN_next``
     - stop before next txn
     - stays at ``last_commit``
   * - Mid-transaction
     - ``beginLSN < endpos < commitLSN``
     - apply full txn, stop
     - stays at ``commitLSN`` (> endpos)

For the "between txns" and "mid-transaction" cases the straddling transaction
is NOT applied to the target.  On the next ``pgcopydb`` run the slot
re-delivers it from the last safe ``transform_lsn`` position.

---

Internal model for endpos tracking
------------------------------------

pgcopydb tracks two separate concerns:

PostgreSQL-facing LSN (``transform_lsn``, ``flush_lsn``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only advances at committed transaction boundaries.  This is the value reported
to PostgreSQL as ``confirmed_flush_lsn``.  It must never be set to a position
inside an uncommitted transaction.

Internal endpos-reached signal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Indicates that the pipeline has processed everything it should process up to
the user's ``endpos``, regardless of where ``endpos`` fell relative to
transaction boundaries.

This signal flows through two mechanisms:

**a.** ``sentinel.replay_lsn >= sentinel.endpos`` — the primary check.  Fires
when the last applied commit naturally covered ``endpos`` (commit-at-boundary
or mid-txn cases where the full transaction was committed to target and its
``commitLSN >= endpos``).

**b.** ``pipeline_state["transform"].run_state = 'done'``
AND ``pipeline_state["apply"].run_state = 'done'``
AND ``sentinel.endpos != 0`` — the secondary check.  Fires when endpos fell
between or inside transactions and apply exited cleanly without advancing
``replay_lsn`` past ``endpos``.  Both processes mark themselves ``'done'``
(not ``'error'``) only on a successful, intentional exit.

``follow_reached_endpos`` checks (a) first; if (a) misses, it checks (b).

Transform exit for mid-transaction endpos
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the transform process detects that receive has finished but the current
XID's COMMIT has never arrived (``pending_xid != 0`` after receive-done):

1. ``ld_store_iter_output`` sets ``specs->private.midTxnEndpos = true``.
2. The outer transform loop sees the flag, logs the situation, and exits.
3. ``transform_lsn`` stays at the last committed transaction boundary — it is
   NOT advanced to ``endpos``.
4. ``pipeline_state_end("transform", transform_lsn, true)`` records the clean
   exit at the last commit boundary.
5. The apply process is woken over the receive→apply lifecycle pipe (the
   one-way "done at LSN X" signal modelled on PostgreSQL's postmaster
   death-watch; see :ref:`pipe_protocol`).

``sentinel.transform_lsn`` never moves past a committed transaction boundary.
The slot's ``confirmed_flush_lsn`` is unaffected by the mid-transaction endpos.

Apply exit for mid-transaction and between-transaction endpos
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Guard 2** (``endpos < beginLSN`` — endpos lies between the last commit and
the next transaction's BEGIN):

- ``context->previousLSN`` is not advanced to ``endpos``.
- ``stream_apply_sync_sentinel()`` is called with ``previousLSN = last_commit``.
- ``sentinel.replay_lsn = last_commit_lsn``.
- ``context->reachedEndPos = true``; loop exits.
- ``pipeline_state_end("apply", last_commit_lsn, true)`` records the clean exit.

**"No rows + transform done"** (mid-transaction endpos: the straddling
transaction was never committed to the output table):

- Same as Guard 2: ``previousLSN`` not modified, ``replay_lsn = last_commit``.
- ``pipeline_state_end("apply", last_commit_lsn, true)``.

In both cases ``follow_reached_endpos`` catches completion via the secondary
pipeline_state check, not via ``replay_lsn >= endpos``.

Apply driver loop
~~~~~~~~~~~~~~~~~

The apply process runs a single driver loop.  Each iteration:

1. snapshots the in-memory ``pipeline_state`` for the ``apply`` process;
2. dispatches the **transform stage** (``outputDB`` → ``replayDB``), which
   updates that in-memory state for every complete transaction it writes;
3. dispatches the **replay stage** (``replayDB`` → target), which updates the
   state for every transaction it commits; then
4. compares the post-iteration state against the snapshot.

The loop only evaluates its terminal conditions (endpos reached,
mid-transaction endpos, or receive done) when an iteration makes **no**
progress.  This guarantees that a transaction the transform stage has just
produced is always consumed by the replay stage before the loop can declare
itself done — in particular when ``endpos`` lands exactly on a ``COMMIT``
boundary, which is the value ``pg_current_wal_flush_lsn()`` returns right
after a committed batch.

The in-memory ``pipeline_state`` is checkpointed to ``sourceDB`` periodically
and once more at end of processing, rather than once per transaction.

Restart safety
~~~~~~~~~~~~~~

Because ``transform_lsn`` stays at the last commit boundary, the PostgreSQL
slot's ``confirmed_flush_lsn`` is never advanced past a completed transaction.
On the next run:

- The slot re-delivers from ``confirmed_flush_lsn = end_lsn_of_last_commit``.
- The ``>=`` cursor finds ``BEGIN_next`` (whose LSN equals
  ``end_lsn_of_last_commit``).
- Any transaction that straddled the endpos is fully re-delivered and applied.

---

Invariant summary
-----------------

.. list-table::
   :header-rows: 1
   :widths: 30 25 25 20

   * - Value
     - Advances at
     - Never set to
     - Drives
   * - ``transform_lsn``
     - ``COMMIT.end_lsn``
     - position inside uncommitted txn
     - ``confirmed_flush_lsn`` on source PG
   * - ``flush_lsn``
     - same as ``transform_lsn``
     - (derived)
     - replication keepalive feedback
   * - ``replay_lsn``
     - target Postgres COMMIT confirmed
     - position without backing COMMIT
     - ``follow_reached_endpos`` check (a)
   * - ``pipeline_state["apply"].run_state``
     - process exit (``'done'``/``'error'``)
     - —
     - ``follow_reached_endpos`` check (b)
   * - ``context->previousLSN``
     - target Postgres COMMIT confirmed
     - mid-txn or between-txn endpos
     - apply replay cursor; origin restart
