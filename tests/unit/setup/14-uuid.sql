create extension "uuid-ossp";

create table uuid(id uuid default uuid_generate_v4(), f1 text);

insert into uuid(f1)
     select x::text from generate_series(100, 199, 1) as t(x);
