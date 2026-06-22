---
--- See https://github.com/dimitri/pgcopydb/issues/894
---

create table tsv
 (
   id    bigint primary key,
   title text,
   body  text,
   tsv   tsvector
 );

insert into tsv values (
  1,
  'hello world',
  'this is a test document',
  to_tsvector('english', 'hello world this is a test document')
);
