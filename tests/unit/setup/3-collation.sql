create table colls
 (
   id bigserial not null primary key,
   f1 text collate "fr-FR-x-icu",
   f2 text collate "de-DE-x-icu"
 );

create collation if not exists mycol
 (
   locale = 'fr-FR-x-icu',
   provider = 'icu'
 );

create table mycoltab
 (
   id bigserial not null primary key,
   f1 text collate mycol,
   f2 text collate "en-US-x-icu"
 );
