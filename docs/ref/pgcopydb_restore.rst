.. _pgcopydb_restore:

pgcopydb restore
================

pgcopydb restore - Restore database objects into a Postgres instance

This command prefixes the following sub-commands:

::

  pgcopydb restore
    schema      Restore a database schema from custom files to target database
    pre-data    Restore a database pre-data schema from custom file to target database
    post-data   Restore a database post-data schema from custom file to target database
    roles       Restore database roles from SQL file to target database
    parse-list  Parse pg_restore --list output from custom file


.. _pgcopydb_restore_schema:

pgcopydb restore schema
-----------------------

pgcopydb restore schema - Restore a database schema from custom files to target database

The command ``pgcopydb restore schema`` uses pg_restore to create the SQL
schema definitions from the given ``pgcopydb dump schema`` export directory.
This command is not compatible with using Postgres files directly, it must
be fed with the directory output from the ``pgcopydb dump ...`` commands.

::

   pgcopydb restore schema: Restore a database schema from custom files to target database
   usage: pgcopydb restore schema  --dir <dir> [ --source <URI> ] --target <URI>

     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --drop-if-exists     On the target database, clean-up from a previous run first
     --no-owner           Do not set ownership of objects to match the original database
     --no-acl             Prevent restoration of access privileges (grant/revoke commands).
     --no-comments        Do not output commands to restore comments
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database


.. _pgcopydb_restore_pre_data:

pgcopydb restore pre-data
-------------------------

pgcopydb restore pre-data - Restore a database pre-data schema from custom file to target database

The command ``pgcopydb restore pre-data`` uses pg_restore to create the SQL
schema definitions from the given ``pgcopydb dump schema`` export directory.
This command is not compatible with using Postgres files directly, it must
be fed with the directory output from the ``pgcopydb dump ...`` commands.

::

   pgcopydb restore pre-data: Restore a database pre-data schema from custom file to target database
   usage: pgcopydb restore pre-data  --dir <dir> [ --source <URI> ] --target <URI>

     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --drop-if-exists     On the target database, clean-up from a previous run first
     --no-owner           Do not set ownership of objects to match the original database
     --no-acl             Prevent restoration of access privileges (grant/revoke commands).
     --no-comments        Do not output commands to restore comments
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database

.. _pgcopydb_restore_post_data:

pgcopydb restore post-data
--------------------------

pgcopydb restore post-data - Restore a database post-data schema from custom file to target database

The command ``pgcopydb restore post-data`` uses pg_restore to create the SQL
schema definitions from the given ``pgcopydb dump schema`` export directory.
This command is not compatible with using Postgres files directly, it must
be fed with the directory output from the ``pgcopydb dump ...`` commands.

::

   pgcopydb restore post-data: Restore a database post-data schema from custom file to target database
   usage: pgcopydb restore post-data  --dir <dir> [ --source <URI> ] --target <URI>

     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --no-owner           Do not set ownership of objects to match the original database
     --no-acl             Prevent restoration of access privileges (grant/revoke commands).
     --no-comments        Do not output commands to restore comments
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database


.. _pgcopydb_restore_roles:

pgcopydb restore roles
----------------------

pgcopydb restore roles - Restore database roles from SQL file to target database

The command ``pgcopydb restore roles`` runs the commands from the SQL script
obtained from the command ``pgcopydb dump roles``. Roles that already exist
on the target database are skipped.

The ``pg_dumpall`` command issues two lines per role, the first one is a
``CREATE ROLE`` SQL command, the second one is an ``ALTER ROLE`` SQL
command. Both those lines are skipped when the role already exists on the
target database.

::

   pgcopydb restore roles: Restore database roles from SQL file to target database
   usage: pgcopydb restore roles  --dir <dir> [ --source <URI> ] --target <URI>

     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use


.. _pgcopydb_restore_parse_list:

pgcopydb restore parse-list
---------------------------

pgcopydb restore parse-list - Parse pg_restore --list output from custom file

The command ``pgcopydb restore parse-list`` outputs pg_restore to list the
archive catalog of the custom file format file that has been exported for
the post-data section.

When using the ``--filters`` option , then the source database connection is
used to grab all the dependend objects that should also be filtered, and the
output of the command shows those pg_restore catalog entries commented out.

A pg_restore archive catalog entry is commented out when its line starts
with a semi-colon character (`;`).

::

   pgcopydb restore parse-list: Parse pg_restore --list output from custom file
   usage: pgcopydb restore parse-list  --dir <dir> [ --source <URI> ] --target <URI>

     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database


Description
-----------

The ``pgcopydb restore schema`` command implements the creation of SQL
objects in the target database, second and last steps of a full database
migration.

When the command runs, it calls ``pg_restore`` on the files found at the
expected location within the ``--target`` directory, which has typically
been created with the ``pgcopydb dump schema`` command.

The ``pgcopydb restore pre-data`` and ``pgcopydb restore post-data`` are
limiting their action to respectively the pre-data and the post-data files
in the source directory..

Options
-------

The following options are available to ``pgcopydb restore schema``:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--target

  Connection string to the target Postgres instance.

--dir

  During its normal operations pgcopydb creates a lot of temporary files to
  track sub-processes progress. Temporary files are created in the directory
  location given by this option, or defaults to
  ``${TMPDIR}/pgcopydb`` when the environment variable is set, or
  then to ``/tmp/pgcopydb``.

--drop-if-exists

  When restoring the schema on the target Postgres instance, ``pgcopydb``
  actually uses ``pg_restore``. When this options is specified, then the
  following pg_restore options are also used: ``--clean --if-exists``.

  This option is useful when the same command is run several times in a row,
  either to fix a previous mistake or for instance when used in a continuous
  integration system.

  This option causes ``DROP TABLE`` and ``DROP INDEX`` and other DROP
  commands to be used. Make sure you understand what you're doing here!

--no-owner

  Do not output commands to set ownership of objects to match the original
  database. By default, ``pg_restore`` issues ``ALTER OWNER`` or ``SET
  SESSION AUTHORIZATION`` statements to set ownership of created schema
  elements. These statements will fail unless the initial connection to the
  database is made by a superuser (or the same user that owns all of the
  objects in the script). With ``--no-owner``, any user name can be used for
  the initial connection, and this user will own all the created objects.

--filters <filename>

  This option allows to exclude table and indexes from the copy operations.
  See :ref:`filtering` for details about the expected file format and the
  filtering options available.

--restart

  When running the pgcopydb command again, if the work directory already
  contains information from a previous run, then the command refuses to
  proceed and delete information that might be used for diagnostics and
  forensics.

  In that case, the ``--restart`` option can be used to allow pgcopydb to
  delete traces from a previous run.

--resume

  When the pgcopydb command was terminated before completion, either by an
  interrupt signal (such as C-c or SIGTERM) or because it crashed, it is
  possible to resume the database migration.

  When resuming activity from a previous run, table data that was fully
  copied over to the target server is not sent again. Table data that was
  interrupted during the COPY has to be started from scratch even when using
  ``--resume``: the COPY command in Postgres is transactional and was rolled
  back.

  Same reasonning applies to the CREATE INDEX commands and ALTER TABLE
  commands that pgcopydb issues, those commands are skipped on a
  ``--resume`` run only if known to have run through to completion on the
  previous one.

  Finally, using ``--resume`` requires the use of ``--not-consistent``.

--not-consistent

  In order to be consistent, pgcopydb exports a Postgres snapshot by calling
  the `pg_export_snapshot()`__ function on the source database server. The
  snapshot is then re-used in all the connections to the source database
  server by using the ``SET TRANSACTION SNAPSHOT`` command.

  Per the Postgres documentation about ``pg_export_snapshot``:

    Saves the transaction's current snapshot and returns a text string
    identifying the snapshot. This string must be passed (outside the
    database) to clients that want to import the snapshot. The snapshot is
    available for import only until the end of the transaction that exported
    it.

  __ https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION-TABLE

  Now, when the pgcopydb process was interrupted (or crashed) on a previous
  run, it is possible to resume operations, but the snapshot that was
  exported does not exists anymore. The pgcopydb command can only resume
  operations with a new snapshot, and thus can not ensure consistency of the
  whole data set, because each run is now using their own snapshot.

--snapshot

  Instead of exporting its own snapshot by calling the PostgreSQL function
  ``pg_export_snapshot()`` it is possible for pgcopydb to re-use an already
  exported snapshot.

Environment
-----------

PGCOPYDB_TARGET_PGURI

  Connection string to the target Postgres instance. When ``--target`` is
  ommitted from the command line, then this environment variable is used.

PGCOPYDB_DROP_IF_EXISTS

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb uses the pg_restore options ``--clean --if-exists`` when
   creating the schema on the target Postgres instance.

Examples
--------

First, using ``pgcopydb restore schema``

::

   $ PGCOPYDB_DROP_IF_EXISTS=on pgcopydb restore schema --source /tmp/target/ --target "port=54314 dbname=demo"
   09:54:37 20401 INFO  Restoring database from "/tmp/target/"
   09:54:37 20401 INFO  Restoring database into "port=54314 dbname=demo"
   09:54:37 20401 INFO  Found a stale pidfile at "/tmp/target//pgcopydb.pid"
   09:54:37 20401 WARN  Removing the stale pid file "/tmp/target//pgcopydb.pid"
   09:54:37 20401 INFO  Using pg_restore for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_restore"
   09:54:37 20401 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54314 dbname=demo' --clean --if-exists /tmp/target//schema/pre.dump
   09:54:38 20401 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54314 dbname=demo' --clean --if-exists --use-list /tmp/target//schema/post.list /tmp/target//schema/post.dump


Then the ``pgcopydb restore pre-data`` and ``pgcopydb restore post-data``
would look the same with just a single call to pg_restore instead of the
both of them.


Using ``pgcopydb restore parse-list`` it's possible to review the filtering
options and see how pg_restore catalog entries are being commented-out.

::

   $ cat ./tests/filtering/include.ini
   [include-only-table]
   public.actor
   public.category
   public.film
   public.film_actor
   public.film_category
   public.language
   public.rental

   [exclude-index]
   public.idx_store_id_film_id

   [exclude-table-data]
   public.rental

   $ pgcopydb restore parse-list --dir /tmp/pagila/pgcopydb --resume --not-consistent --filters ./tests/filtering/include.ini
   11:41:22 75175 INFO  Running pgcopydb version 0.5.8.ge0d2038 from "/Users/dim/dev/PostgreSQL/pgcopydb/./src/bin/pgcopydb/pgcopydb"
   11:41:22 75175 INFO  [SOURCE] Restoring database from "postgres://@:54311/pagila?"
   11:41:22 75175 INFO  [TARGET] Restoring database into "postgres://@:54311/plop?"
   11:41:22 75175 INFO  Using work dir "/tmp/pagila/pgcopydb"
   11:41:22 75175 INFO  Removing the stale pid file "/tmp/pagila/pgcopydb/pgcopydb.pid"
   11:41:22 75175 INFO  Work directory "/tmp/pagila/pgcopydb" already exists
   11:41:22 75175 INFO  Schema dump for pre-data and post-data section have been done
   11:41:22 75175 INFO  Restoring database from existing files at "/tmp/pagila/pgcopydb"
   11:41:22 75175 INFO  Using pg_restore for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_restore"
   11:41:22 75175 INFO  Exported snapshot "00000003-0003209A-1" from the source database
   3242; 2606 317973 CONSTRAINT public actor actor_pkey postgres
   ;3258; 2606 317975 CONSTRAINT public address address_pkey postgres
   3245; 2606 317977 CONSTRAINT public category category_pkey postgres
   ;3261; 2606 317979 CONSTRAINT public city city_pkey postgres
   ;3264; 2606 317981 CONSTRAINT public country country_pkey postgres
   ;3237; 2606 317983 CONSTRAINT public customer customer_pkey postgres
   3253; 2606 317985 CONSTRAINT public film_actor film_actor_pkey postgres
   3256; 2606 317987 CONSTRAINT public film_category film_category_pkey postgres
   3248; 2606 317989 CONSTRAINT public film film_pkey postgres
   ;3267; 2606 317991 CONSTRAINT public inventory inventory_pkey postgres
   3269; 2606 317993 CONSTRAINT public language language_pkey postgres
   3293; 2606 317995 CONSTRAINT public rental rental_pkey postgres
   ;3295; 2606 317997 CONSTRAINT public staff staff_pkey postgres
   ;3298; 2606 317999 CONSTRAINT public store store_pkey postgres
   3246; 1259 318000 INDEX public film_fulltext_idx postgres
   3243; 1259 318001 INDEX public idx_actor_last_name postgres
   ;3238; 1259 318002 INDEX public idx_fk_address_id postgres
   ;3259; 1259 318003 INDEX public idx_fk_city_id postgres
   ;3262; 1259 318004 INDEX public idx_fk_country_id postgres
   ;3270; 1259 318005 INDEX public idx_fk_customer_id postgres
   3254; 1259 318006 INDEX public idx_fk_film_id postgres
   3290; 1259 318007 INDEX public idx_fk_inventory_id postgres
   3249; 1259 318008 INDEX public idx_fk_language_id postgres
   3250; 1259 318009 INDEX public idx_fk_original_language_id postgres
   ;3272; 1259 318010 INDEX public idx_fk_payment_p2020_01_customer_id postgres
   ;3271; 1259 318011 INDEX public idx_fk_staff_id postgres
   ;3273; 1259 318012 INDEX public idx_fk_payment_p2020_01_staff_id postgres
   ;3275; 1259 318013 INDEX public idx_fk_payment_p2020_02_customer_id postgres
   ;3276; 1259 318014 INDEX public idx_fk_payment_p2020_02_staff_id postgres
   ;3278; 1259 318015 INDEX public idx_fk_payment_p2020_03_customer_id postgres
   ;3279; 1259 318016 INDEX public idx_fk_payment_p2020_03_staff_id postgres
   ;3281; 1259 318017 INDEX public idx_fk_payment_p2020_04_customer_id postgres
   ;3282; 1259 318018 INDEX public idx_fk_payment_p2020_04_staff_id postgres
   ;3284; 1259 318019 INDEX public idx_fk_payment_p2020_05_customer_id postgres
   ;3285; 1259 318020 INDEX public idx_fk_payment_p2020_05_staff_id postgres
   ;3287; 1259 318021 INDEX public idx_fk_payment_p2020_06_customer_id postgres
   ;3288; 1259 318022 INDEX public idx_fk_payment_p2020_06_staff_id postgres
   ;3239; 1259 318023 INDEX public idx_fk_store_id postgres
   ;3240; 1259 318024 INDEX public idx_last_name postgres
   ;3265; 1259 318025 INDEX public idx_store_id_film_id postgres
   3251; 1259 318026 INDEX public idx_title postgres
   ;3296; 1259 318027 INDEX public idx_unq_manager_staff_id postgres
   3291; 1259 318028 INDEX public idx_unq_rental_rental_date_inventory_id_customer_id postgres
   ;3274; 1259 318029 INDEX public payment_p2020_01_customer_id_idx postgres
   ;3277; 1259 318030 INDEX public payment_p2020_02_customer_id_idx postgres
   ;3280; 1259 318031 INDEX public payment_p2020_03_customer_id_idx postgres
   ;3283; 1259 318032 INDEX public payment_p2020_04_customer_id_idx postgres
   ;3286; 1259 318033 INDEX public payment_p2020_05_customer_id_idx postgres
   ;3289; 1259 318034 INDEX public payment_p2020_06_customer_id_idx postgres
   ;3299; 0 0 INDEX ATTACH public idx_fk_payment_p2020_01_staff_id postgres
   ;3301; 0 0 INDEX ATTACH public idx_fk_payment_p2020_02_staff_id postgres
   ;3303; 0 0 INDEX ATTACH public idx_fk_payment_p2020_03_staff_id postgres
   ;3305; 0 0 INDEX ATTACH public idx_fk_payment_p2020_04_staff_id postgres
   ;3307; 0 0 INDEX ATTACH public idx_fk_payment_p2020_05_staff_id postgres
   ;3309; 0 0 INDEX ATTACH public idx_fk_payment_p2020_06_staff_id postgres
   ;3300; 0 0 INDEX ATTACH public payment_p2020_01_customer_id_idx postgres
   ;3302; 0 0 INDEX ATTACH public payment_p2020_02_customer_id_idx postgres
   ;3304; 0 0 INDEX ATTACH public payment_p2020_03_customer_id_idx postgres
   ;3306; 0 0 INDEX ATTACH public payment_p2020_04_customer_id_idx postgres
   ;3308; 0 0 INDEX ATTACH public payment_p2020_05_customer_id_idx postgres
   ;3310; 0 0 INDEX ATTACH public payment_p2020_06_customer_id_idx postgres
   3350; 2620 318035 TRIGGER public film film_fulltext_trigger postgres
   3348; 2620 318036 TRIGGER public actor last_updated postgres
   ;3354; 2620 318037 TRIGGER public address last_updated postgres
   3349; 2620 318038 TRIGGER public category last_updated postgres
   ;3355; 2620 318039 TRIGGER public city last_updated postgres
   ;3356; 2620 318040 TRIGGER public country last_updated postgres
   ;3347; 2620 318041 TRIGGER public customer last_updated postgres
   3351; 2620 318042 TRIGGER public film last_updated postgres
   3352; 2620 318043 TRIGGER public film_actor last_updated postgres
   3353; 2620 318044 TRIGGER public film_category last_updated postgres
   ;3357; 2620 318045 TRIGGER public inventory last_updated postgres
   3358; 2620 318046 TRIGGER public language last_updated postgres
   3359; 2620 318047 TRIGGER public rental last_updated postgres
   ;3360; 2620 318048 TRIGGER public staff last_updated postgres
   ;3361; 2620 318049 TRIGGER public store last_updated postgres
   ;3319; 2606 318050 FK CONSTRAINT public address address_city_id_fkey postgres
   ;3320; 2606 318055 FK CONSTRAINT public city city_country_id_fkey postgres
   ;3311; 2606 318060 FK CONSTRAINT public customer customer_address_id_fkey postgres
   ;3312; 2606 318065 FK CONSTRAINT public customer customer_store_id_fkey postgres
   3315; 2606 318070 FK CONSTRAINT public film_actor film_actor_actor_id_fkey postgres
   3316; 2606 318075 FK CONSTRAINT public film_actor film_actor_film_id_fkey postgres
   3317; 2606 318080 FK CONSTRAINT public film_category film_category_category_id_fkey postgres
   3318; 2606 318085 FK CONSTRAINT public film_category film_category_film_id_fkey postgres
   3313; 2606 318090 FK CONSTRAINT public film film_language_id_fkey postgres
   3314; 2606 318095 FK CONSTRAINT public film film_original_language_id_fkey postgres
   ;3321; 2606 318100 FK CONSTRAINT public inventory inventory_film_id_fkey postgres
   ;3322; 2606 318105 FK CONSTRAINT public inventory inventory_store_id_fkey postgres
   ;3323; 2606 318110 FK CONSTRAINT public payment_p2020_01 payment_p2020_01_customer_id_fkey postgres
   ;3324; 2606 318115 FK CONSTRAINT public payment_p2020_01 payment_p2020_01_rental_id_fkey postgres
   ;3325; 2606 318120 FK CONSTRAINT public payment_p2020_01 payment_p2020_01_staff_id_fkey postgres
   ;3326; 2606 318125 FK CONSTRAINT public payment_p2020_02 payment_p2020_02_customer_id_fkey postgres
   ;3327; 2606 318130 FK CONSTRAINT public payment_p2020_02 payment_p2020_02_rental_id_fkey postgres
   ;3328; 2606 318135 FK CONSTRAINT public payment_p2020_02 payment_p2020_02_staff_id_fkey postgres
   ;3329; 2606 318140 FK CONSTRAINT public payment_p2020_03 payment_p2020_03_customer_id_fkey postgres
   ;3330; 2606 318145 FK CONSTRAINT public payment_p2020_03 payment_p2020_03_rental_id_fkey postgres
   ;3331; 2606 318150 FK CONSTRAINT public payment_p2020_03 payment_p2020_03_staff_id_fkey postgres
   ;3332; 2606 318155 FK CONSTRAINT public payment_p2020_04 payment_p2020_04_customer_id_fkey postgres
   ;3333; 2606 318160 FK CONSTRAINT public payment_p2020_04 payment_p2020_04_rental_id_fkey postgres
   ;3334; 2606 318165 FK CONSTRAINT public payment_p2020_04 payment_p2020_04_staff_id_fkey postgres
   ;3335; 2606 318170 FK CONSTRAINT public payment_p2020_05 payment_p2020_05_customer_id_fkey postgres
   ;3336; 2606 318175 FK CONSTRAINT public payment_p2020_05 payment_p2020_05_rental_id_fkey postgres
   ;3337; 2606 318180 FK CONSTRAINT public payment_p2020_05 payment_p2020_05_staff_id_fkey postgres
   ;3338; 2606 318185 FK CONSTRAINT public payment_p2020_06 payment_p2020_06_customer_id_fkey postgres
   ;3339; 2606 318190 FK CONSTRAINT public payment_p2020_06 payment_p2020_06_rental_id_fkey postgres
   ;3340; 2606 318195 FK CONSTRAINT public payment_p2020_06 payment_p2020_06_staff_id_fkey postgres
   ;3341; 2606 318200 FK CONSTRAINT public rental rental_customer_id_fkey postgres
   ;3342; 2606 318205 FK CONSTRAINT public rental rental_inventory_id_fkey postgres
   ;3343; 2606 318210 FK CONSTRAINT public rental rental_staff_id_fkey postgres
   ;3344; 2606 318215 FK CONSTRAINT public staff staff_address_id_fkey postgres
   ;3345; 2606 318220 FK CONSTRAINT public staff staff_store_id_fkey postgres
   ;3346; 2606 318225 FK CONSTRAINT public store store_address_id_fkey postgres
