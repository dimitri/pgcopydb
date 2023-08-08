-- KEEPALIVE {"lsn":"0/2447888","timestamp":"2023-06-14 11:17:51.978694+0000"}
BEGIN; -- {"xid":491,"lsn":"0/244A550","timestamp":"2023-06-14 11:17:51.979425+0000","commit_lsn":"0/244A850"}
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1000","Fantasy","2022-12-08 00:00:01+00"];
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1001","History","2022-12-09 00:00:01+00"];
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1002","Adventure","2022-12-10 00:00:01+00"];
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1003","Musical","2022-12-11 00:00:01+00"];
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1004","Western","2022-12-12 00:00:01+00"];
COMMIT; -- {"xid":491,"lsn":"0/244A850","timestamp":"2023-06-14 11:17:51.979425+0000"}
BEGIN; -- {"xid":492,"lsn":"0/244A850","timestamp":"2023-06-14 11:17:51.979452+0000"}
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1005","Mystery","2022-12-13 00:00:01+00"];
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1006","Historical drama","2022-12-14 00:00:01+00"];
PREPARE 80429e6 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3);
EXECUTE 80429e6["1008","Thriller","2022-12-15 00:00:01+00"];
-- KEEPALIVE {"lsn":"0/244A978","timestamp":"2023-06-14 11:17:51.979481+0000"}
-- ENDPOS {"lsn":"0/244A978"}
