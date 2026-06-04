BEGIN
PREPARE 564c4023 AS INSERT INTO "public"."items" ("id", "name") overriding system value VALUES ($1, $2), ($3, $4);
EXECUTE 564c4023 ('4', 'cdc-new-1', '5', 'cdc-new-2')
PREPARE 263d90fe AS UPDATE "public"."items" SET "name" = $1 WHERE "id" = $2;
EXECUTE 263d90fe ('seed-updated-1', '1')
PREPARE a940a68d AS DELETE FROM "public"."items" WHERE "id" = $1;
EXECUTE a940a68d ('2')
COMMIT
