create table test_str_escape
 (
   id bigint not null generated always as identity primary key,
   f1 text
 );

insert into test_str_escape (f1)
     values (E'aaa\naaa'),
            (E'bbb\r\nbbb'),
            (E'ccc');
