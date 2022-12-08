BEGIN; -- {"xid":491,"lsn":"0/22E8318","timestamp":"2022-12-08 14:45:53.226340+0000"}
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1000, 'Fantasy', '2022-12-08 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1001, 'History', '2022-12-09 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1002, 'Adventure', '2022-12-10 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1003, 'Musical', '2022-12-11 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1004, 'Western', '2022-12-12 00:00:01+00');
COMMIT; -- {"xid":491,"lsn":"0/22E8618","timestamp":"2022-12-08 14:45:53.226340+0000"}
BEGIN; -- {"xid":492,"lsn":"0/22E8618","timestamp":"2022-12-08 14:45:53.226746+0000"}
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1005, 'Mystery', '2022-12-13 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1006, 'Historical drama', '2022-12-14 00:00:01+00');
INSERT INTO "public"."category" ("category_id", "name", "last_update") VALUES (1008, 'Thriller', '2022-12-15 00:00:01+00');
COMMIT; -- {"xid":492,"lsn":"0/0","timestamp":"2022-12-08 14:45:53.226746+0000"}
