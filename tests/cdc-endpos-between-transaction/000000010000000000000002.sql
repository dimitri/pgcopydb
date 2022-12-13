BEGIN; -- {"xid":491,"lsn":"0/22E8458","timestamp":"2022-12-08 14:45:11.358714+0000"}
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1000, 'Fantasy', '2022-12-08 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1001, 'History', '2022-12-09 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1002, 'Adventure', '2022-12-10 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1003, 'Musical', '2022-12-11 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1004, 'Western', '2022-12-12 00:00:01+00');
COMMIT; -- {"xid":491,"lsn":"0/22E8758","timestamp":"2022-12-08 14:45:11.358714+0000"}
-- KEEPALIVE {"lsn":"0/22E8880","timestamp":"2022-12-08 14:45:11.359095+0000"}
