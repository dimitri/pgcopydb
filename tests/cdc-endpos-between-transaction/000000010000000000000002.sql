BEGIN; -- {"xid":491,"lsn":"0/244B938","timestamp":"2023-03-14 13:54:44.797711+0000"}
INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES (1000, 'Fantasy', '2022-12-08 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES (1001, 'History', '2022-12-09 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES (1002, 'Adventure', '2022-12-10 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES (1003, 'Musical', '2022-12-11 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") overriding system value VALUES (1004, 'Western', '2022-12-12 00:00:01+00');
COMMIT; -- {"xid":491,"lsn":"0/244BC38","timestamp":"2023-03-14 13:54:44.797711+0000"}
-- KEEPALIVE {"lsn":"0/244BD60","timestamp":"2023-03-14 13:54:44.798094+0000"}
