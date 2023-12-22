-- KEEPALIVE {"lsn":"0/2448730","timestamp":"2023-12-21 17:07:18.986320+0000"}
BEGIN; -- {"xid":491,"lsn":"0/244B450","timestamp":"2023-12-21 17:07:19.3436+0000","commit_lsn":"0/244B750"}
PREPARE 86e87d54 AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9), ($10, $11, $12), ($13, $14, $15);
EXECUTE 86e87d54["1000","Fantasy","2022-12-08 00:00:01+00","1001","History","2022-12-09 00:00:01+00","1002","Adventure","2022-12-10 00:00:01+00","1003","Musical","2022-12-11 00:00:01+00","1004","Western","2022-12-12 00:00:01+00"];
COMMIT; -- {"xid":491,"lsn":"0/244B750","timestamp":"2023-12-21 17:07:19.3436+0000"}
BEGIN; -- {"xid":492,"lsn":"0/244B750","timestamp":"2023-12-21 17:07:19.3543+0000"}
PREPARE 918852ce AS INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9);
EXECUTE 918852ce["1005","Mystery","2022-12-13 00:00:01+00","1006","Historical drama","2022-12-14 00:00:01+00","1008","Thriller","2022-12-15 00:00:01+00"];
-- KEEPALIVE {"lsn":"0/244B878","timestamp":"2023-12-21 17:07:19.3615+0000"}
-- ENDPOS {"lsn":"0/244B878"}
ROLLBACK; -- {"xid":492,"lsn":"0/244B878","timestamp":"2023-12-21 17:07:19.3543+0000"}
