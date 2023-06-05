create table test_tbl
 (
   id bigint not null generated always as identity primary key,
   f1 text
 );
