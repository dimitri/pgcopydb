-- Dataset for the target-resume-recovery test.
--
-- It exercises every clone code path that a target-side connection reset can
-- interrupt, so that driving the transfer with external `clone --resume` has to
-- recover each of them:
--
--   - table COPY           (resumed by TRUNCATE + re-COPY of the unfinished table)
--   - CREATE INDEX workers (resumed by CREATE INDEX IF NOT EXISTS)
--   - ADD CONSTRAINT       (resumed by the constraint build skipping what exists)
--   - setval() of sequences
--   - large object copy
--
-- The volume is sized so a full clone stays busy for several seconds, giving
-- the backend killer running alongside it a wide window to reset pgcopydb's
-- target connections mid-flight and force multiple resume cycles.

-- A handful of tables, each with a primary key and a couple of secondary
-- indexes, so the COPY, CREATE INDEX and ADD CONSTRAINT paths all get a
-- meaningful amount of work.
create table t1 (id bigserial primary key, payload text not null, n int not null);
create table t2 (id bigserial primary key, payload text not null, n int not null);
create table t3 (id bigserial primary key, payload text not null, n int not null);
create table t4 (id bigserial primary key, payload text not null, n int not null);

insert into t1 (payload, n)
     select repeat(md5(g::text), 8), g % 1000
       from generate_series(1, 120000) g;

insert into t2 (payload, n)
     select repeat(md5(g::text), 8), g % 1000
       from generate_series(1, 120000) g;

insert into t3 (payload, n)
     select repeat(md5(g::text), 8), g % 1000
       from generate_series(1, 120000) g;

insert into t4 (payload, n)
     select repeat(md5(g::text), 8), g % 1000
       from generate_series(1, 120000) g;

create index t1_n_idx on t1 (n);
create index t1_payload_idx on t1 (payload);
create index t2_n_idx on t2 (n);
create index t2_payload_idx on t2 (payload);
create index t3_n_idx on t3 (n);
create index t3_payload_idx on t3 (payload);
create index t4_n_idx on t4 (n);
create index t4_payload_idx on t4 (payload);

-- A standalone sequence advanced to a non-trivial value, so the setval path
-- has a value to carry across that we can assert on afterwards.
create sequence standalone_seq;
select setval('standalone_seq', 987654);

-- A spread of large objects so the blob worker runs several batches.
do $$
declare
  i int;
  loid oid;
begin
  for i in 1..400 loop
    loid := lo_create(0);
    perform lo_put(loid, 0, decode(repeat(md5(i::text), 64), 'escape'));
  end loop;
end
$$;
