-- KEEPALIVE {"lsn":"0/24488D0","timestamp":"2023-09-17 07:04:04.9104+0000"}
BEGIN; -- {"xid":491,"lsn":"0/244B580","timestamp":"2023-09-17 07:04:04.9662+0000","commit_lsn":"0/244B880"}
PREPARE e844dad6 AS INSERT INTO public.category ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9), ($10, $11, $12), ($13, $14, $15);
EXECUTE e844dad6["1000","Fantasy","2022-12-08 00:00:01+00","1001","History","2022-12-09 00:00:01+00","1002","Adventure","2022-12-10 00:00:01+00","1003","Musical","2022-12-11 00:00:01+00","1004","Western","2022-12-12 00:00:01+00"];
COMMIT; -- {"xid":491,"lsn":"0/244B880","timestamp":"2023-09-17 07:04:04.9662+0000"}
BEGIN; -- {"xid":492,"lsn":"0/244B880","timestamp":"2023-09-17 07:04:04.9703+0000"}
PREPARE 6a9e34e7 AS INSERT INTO public.category ("category_id", "name", "last_update") overriding system value VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9);
EXECUTE 6a9e34e7["1005","Mystery","2022-12-13 00:00:01+00","1006","Historical drama","2022-12-14 00:00:01+00","1008","Thriller","2022-12-15 00:00:01+00"];
-- KEEPALIVE {"lsn":"0/244B9A8","timestamp":"2023-09-17 07:04:04.9770+0000"}
-- ENDPOS {"lsn":"0/244B9A8"}
