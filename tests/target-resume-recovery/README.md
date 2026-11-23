# target-resume-recovery

Checks that a `pgcopydb clone` completes a full, correct transfer under a target
that repeatedly drops its connections, recovering **purely through pgcopydb's
existing `--resume`** - no internal reconnect-and-replay.

A background killer (`killer.sh`) runs `pg_terminate_backend()` against every
`pgcopydb%` backend on the **target** every 0.3s. Nothing is spared: because
resume restarts an unfinished table's COPY wholesale (`TRUNCATE` + re-COPY),
killing a COPY or `TRUNCATE` backend is just as recoverable as any other, so the
killer targets every phase (table COPY, CREATE INDEX, ADD CONSTRAINT, sequence
`setval`, large objects).

The transfer is driven by a loop that simply re-runs the clone: the first
invocation with `--restart`, every subsequent one with `--resume
--not-consistent`. Almost every invocation is cut down mid-flight; recovery is
nothing more than running `clone --resume` again until one invocation survives a
kill-free window long enough to finish.

The test asserts three things:

1. the killer actually terminated at least one backend (the chaos was real),
2. at least one clone invocation completed (exit 0) while the killer was still
   running (resume-only recovery converges rather than wedging), and
3. after a final settle `clone --resume` with the killer stopped, the target is
   provably identical to the source - matching row counts, sequence value and
   large object count, and, authoritatively, `pgcopydb compare schema` and
   `pgcopydb compare data` (which checksum every table's contents).

This is the counterpart to a reconnect-and-replay approach: instead of teaching
each worker to reconnect in place, it leans entirely on the resume machinery and
an external retry loop, and proves that alone is enough to land a complete
transfer under sustained connection loss.

Run it with:

```
make -C tests target-resume-recovery
```
