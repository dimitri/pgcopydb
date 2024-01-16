### pgcopydb v0.15 (January 10, 2024) ###

### Added
* Skip creating Large Objects workers when database has zero of them. (#599)
* Add millisecond level resolution to log lines (#550)
* Support skipping extensions & filtering in pgcopydb dump/restore (#572)
* Add option to output numeric as string on wal2json (#576)
* Parallelize pg_restore operations (#561)
* Add byte level details to list progress command (#503)
* Rollback incomplete txns on graceful exit (#544)
* Set GUCs in the connection for COPY to target. (#541)

### Changed
* Use SQLite to store our internal copy of the source catalogs. (#569)
* Debian: B-D on libsqlite3-dev (#628)
* Use libpq Single Row Mode when fetching catalogs. (#584)
* Use SQLite for LSN tracking. (#625)
* Use SQLite catalogs for top-level timings too. (#616)
* Use our SQLite catalogs for the pgcopydb sentinel table. (#601)
* Refactor code to fetch current LSN position from Postgres. (#608)
* Review CDC/Follow tests workdir cleanup (and git registration). (#607)
* Removes copy-db command (#605)
* Remove tbldir and idxdir in the workdir (not needed anymore). (#604)
* Remove summary files, use our SQLite database instead. (#590)
* Review the COPY arguments API to fix TRUNCATE calls. (#597)
* Set application name to process title and pid (#553)
* Use same connection in table copy worker. (#542)
* Invalidate context state during cleanup (#547)

### Fix
* Fix gcc warnings found when building on i386. (#629)
* Use %zu for size_t values (sizeof, strlen). (#627)
* Fix our exclude-index filtering. (#626)
* Problem: `pgcopydb copy extension` fails when sequences exists part of ext configuration (#621)
* Fix numerous issues with argument parsing (#617)
* Problem: Incorrect flush LSN from source being used to find durable LSN (#615)
* Fix pgcopydb list progress command. (#610)
* Fix sub-process "success" condition and error handling. (#603)
* Fix error handling (--fail-fast) for copy workers. (#598)
* Fix escaping of identifiers while transforming for wal2json plugin. (#595)
* Fix crash on exit with --debug and PGCOPYDB_LOG_FILENAME (#589)
* Fix stack pointer access while cleaning up resources on exit (#592)
* Fix issues with --skip-extensions flag (#587)
* Fix args index bug due to incorrect evaluation order (#585)
* Fix extra quotes of identifier in ALTER database properties (#583)
* Fix interrupt handling in queue_receive. (#580)
* Fix Postgres connection handling in Large Object related code. (#579)
* Fix application_name truncation notice messages. (#578)
* Fix division-by-zero hazard in random_between macro. (#571)
* Fix the listSourceTablesNoPKSQL queries (#570)
* Compare endpos only if it is valid (#568)
* Fix issue where the replay process fails to reach the end position. (#566)
* Fix duplicate key errors on resuming continued txn (#555)
* Close pipes created by follow after forking childs (#563)
* Fix copying blobs that are not included in snapshot. (#558)
* Fix memory corruption while calling SysV msgsnd & msgrcv (#551)
* Fix some copy paste errors (#543)

### pgcopydb v0.14 (November 20, 2023) ###

This is a bugfix release with a strong focus on reliability, in particular
in the logical decoding parts of pgcopydb (the --follow implementation).

### Added
* Implement multi-value inserts (#465)
* feat(container): multi arch build (#480)
* Add support for database properties. (#491)
* Add tests coverage for Postgres 16. (#478)
* Set Process Titles as seen in ps/top/htop etc. (#435)
* Implement same-table concurrency using Postgres CTID column. (#410)
* When applying --follow changes, set synchronous_commit to off. (#425)
* Use PREPARE/EXECUTE statements for applying DML in follow mode. (#420)
* Use --table-jobs processes when computing pgcopydb compare data. (#419)
* Implement concurrent workers for Large Objects data copy. (#411)

### Changed
* Improve migration speed by using same connection in a LOB worker. (#533)
* Improve and fix connection timeout and total retry timeout. (#528)
* Have the follow process read the schema from disk when it's been exported. (#523)
* use LOG_WARN for warnings instead of LOG_ERROR (#506)
* Improve Handling of replay_lsn for Idle Source (#519)
* Update the lib/parson vendored-in library to a new version. (#512)
* Review logical decoding client tracking of LSNs. (#502)
* Add check constraint, language entry mapping (#497)
* Ensure transactions are not read only by default (#490)
* Switch to using libpq async API. (#488)
* Improve error handling when COPY fails. (#485)
* Switch our call to PQgetCopyData to async in pg_copy(). (#479)
* Rewrite the ad-hoc parser for Postgres Archive TOC items. (#469)
* Use an intermediate file on-disk for pg_restore --list output. (#467)
* Add a way to debug parsing a list file. (#470)
* Improve connection string TCP keepalive parameters handling. (#457)
* Review the TCP keepalives settings. (#436)
* Do not reset sequences on copy --follow failure. (#424)
* Refactor the cli_compare code. (#418)
* Use Postgres async queries to compute checksums. (#416)
* Improve pgcopydb compare data checksum computation. (#407)

### Fix
* fix inherit data copy (#538)
* Use dynamically allocated QMessage for msgrcv. (#534)
* exclude generated columns from COPY statements (#517)
* Update pgcopydb sentinel in the main follow process. (#521)
* Remove the usage of txn metadata file (#525)
* fix password handling in safeURI (#522)
* Update README.md (One Typo Fixed) (#527)
* Fix lib/subcommands.c sprintf usage. (#520)
* Ensure NULL-termination of string when unescaping single-quotes (#514)
* add archive entry mapping for LARGE OBJECTS and ROW SECURITY (#515)
* Use target dbname when setting db parameters (#509)
* Fix various issues with docs (#494)
* Fix data loss when reading large transaction bodies (#495)
* Fix test runs for outside contributors
* Review static memory size of Postgres identifier names. (#486)
* Fix column name escaping with test_decoding plugin (#482)
* Fixing PING documentation (#476)
* fix drop cascade issue (#475)
* Fix SQL identifier quoting, generalize using format %I. (#464)
* Fix error handling of fork() calls. (#459)
* Fix set_ps_title to avoid calling sformat() on possibly short buffers. (#437)
* Fix escaping double-quotes in SQL identifiers. (#434)
* Fix memory leaks in transform code (#433)
* Assorted fixes from review, including stream_read_context retry loop. (#423)
* Fix a SEGFAULT that was hidden by our waitpid() calls. (#422)
* Fix exit() calls that should have been return false; (#421)
* Fix filtering on schema names. (#415)
* Fix a compile warning on Linux related to printf format strings. (#406)


### pgcopydb v0.13 (July 27, 2023) ###

This is a bugfix release that still manages to include some new features.

### Added
* pgcopydb compare schema|data (#404)
* Implement the new filter "include-only-schema". (#403)
* Implement extension requirements support. (#400)
* Add facilities to list extension versions. (#378)
* Implement --skip-extension-comments. (#356)

### Changed
* Add Postgres backend PID to more statements. (#396)
* Add Postgres backend PID to our SQL (error) logs. (#393)
* Only update sentinel.startpos up to flush_lsn. (#372)
* Review double-precision format string from %g to %f. (#368)

### Fix
* Fix schema_prepare_pgcopydb_table_size to use dynamic memory. (#402)
* Fix log message typos (missing quotes in \"%s\"). (#401)
* Fix support for DEFERRABLE and INITIALLY DEFERRED constraints. (#398)
* Fix sequences dependencies tracking. (#397)
* Fix issue with resuming streaming of interleaved transactions. (#394)
* Attempt to fix a segfault. (#391)
* Fix a typo in the filtering docs: include-only-table. (#389)
* Fix a couple bugs in pgcopydb clone --follow. (#386)
* Fix our COPY statements for tables with zero column. (#385)
* Assorted fixes for the test_decoding parser. (#380)
* The cli_stream module didn't get the pg_strdup() memo for DSNs. (#379)
* The cli_stream module didn't get the pg_strdup() memo for DSNs. (#371)
* Implement multi-parts read from Unix PIPE. (#370)
* Fix NULL connection string in: pgcopydb list schema. (#369)
* Refactor how ld_stream writes to PIPE stdout. (#367)
* Fix skipping streaming messages when resuming from latest JSON file. (#359)
* Fix stream_apply_wait_for_sentinel() return value when interrupted. (#358)
* Fix stream_transform_resume() cache invalidation of the SQL file. (#357)
* Fix stream_transform_resume to share context with stream_transform_file. (#355)
* Fix our PG_VERSION_STRING_MAX_LENGTH to host more. (#353)

### pgcopydb v0.12 (June 28, 2023) ###

This is a bugfix release with a strong focus on our logical decoding client.
The support for test_decoding UPDATE messages is improved, and a lot of bugs
related to how we split JSON messages in separate files (same as Postgres
WAL naming) have now been found and fixed.

An important fix has been implemented with respect to how Postgres snapshots
and logical decoding works. The replication command CREATE_REPLICATION_SLOT
is able to export a snapshot but can not import one, which means that
`pgcopydb snapshot` now has a new `--follow` command which creates a
replication slot and exports a snapshot to re-use in the rest of the
commands.

### Added
* Add table column list to schema.json. (#315)
* Improve parsing of UPDATE messages from test_decoding. (#329)
* Implement pgcopydb list databases. (#270)
* Implement pgcopydb list progress --summary --json. (#235)
* Implement the feature to log to file, with support for JSON. (#234)
* Introduce new option --skip-vacuum. (#230)
* Implement a --fail-fast option. (#222)

### Changed
* Set statement_timeout and lock_timeout to 0. (#344)
* Have --skip-extensions also skip pre-existing schemas on the target. (#341)
* Quote sequence names when checking for privileges. (#326)
* Use dynamic memory for connection strings handling. (#323)
* Introduce a new pgcopydb internal message: ENDPOS. (#321)
* Remove dead code (hostname_from_uri). (#325)
* Remove unnecessary reading for commit_lsn after reaching startpos. (#318)
* Switch from semaphores to message queue to share workload. (#305)
* Use JSON format for work-directory summary files. (#300)
* At follow mode switch, skip empty transform queues. (#301)
* When using --trace then enforce logging of the apply SQL statements. (#252)
* Use dynamic memory for variable length schema parts. (#249)
* Refrain from logging user data. (#227)
* Improve logs. (#215)
* Use OVERRIDING SYSTEM VALUE in INSERT statements in follow mode. (#214)
* Create new process group for pgcopydb initialization. (#211)
* Check that we have sequence privileges before selecting metadata. (#212)
* Only create pgcopydb schema and table_size table when --cache is used. (#210)
* Switch docs PDF building to xelatex engine. (#209)

### Fixed
* Fix pgcopydb stream cleanup. (#351)
* Fix transform escape rules for SQL escape syntax, again. (#349)
* Fix resuming the transform process. (#348)
* Ensure startpos is updated when closing stream. (#345)
* Skip importing snapshot we won't use, fix pgsql error handling. (#340)
* Fix transform to SQL to escape string values. (#337)
* Fix transforming UPDATE messages WHERE/SET clauses. (#333)
* Fix schema queries for exclusion filters. (#314)
* When applying CDC (logical replication), set role to replica. (#308)
* Add unit testing coverage for "generated as identity" columns. (#309)
* Fix file rotation issue during streaming. (#298)
* Fix apply when file starts with non-begin/keepalive statements. (#304)
* transform: Fix transformation to empty txn when first line is COMMIT. (#303)
* Fix empty xid and timestamp for continued txn COMMIT messages. (#302)
* Fix missing data of txns whose BEGIN LSN is less than consistent_point. (#295)
* Filter out dropped columns (#294)
* Use column names in COPY statements. (#290)
* Fix how snapshot are exported when using CDC. (#279)
* Refrain from early exits on signals when sending messages to queues. (#285)
* Fix pgsql is_response_ok to accept also PGRES_COPY_BOTH. (#284)
* Fix off-by-one in size of transform messages array. (#283)
* Fix double precision out of range during COPY. (#281)
* Fix the transition between replay operating modes. (#277)
* Review and fix connection management for sentinel async updates. (#273)
* Update pgcopydb sentinel's replay_lsn asynchronously. (#267)
* Count pg_restore --list lines and dimension our array accordingly. (#268)
* Fix a typo in docs (#261)
* Assorted streaming fixes, including skipping of empty transactions. (#257)
* Fix wal2json bytea values. (#253)
* Transform empty transactions from the JSON to the SQL files. (#251)
* Fix special characters in SQL queries. (#248)
* Fix exclude-schema filtering to apply to pg_dump and pg_restore. (#247)
* Fix Postgres catalog queries that implement sequences filtering. (#246)
* Fix pgsql_state_is_connection_error. (#244)
* Fix parsing --fail-fast option, which requires no argument. (#243)
* Fix command line option log level increments (--verbose --notice). (#233)
* Fix when we VACUUM ANALYZE. (#228)
* Fix how we skip concurrent build of certain indexes. (#223)
* Ensure clean-up of System V resources. (#216)
* Fix the `pgcopydb list collations` SQL query. (#213)


### pgcopydb v0.11 (March 15, 2023) ###

This release introduces support for the Postgres code logical decoding
plugin test_decoding in addition to wal2json, and also introduces the fully
automated switch from prefetch and catchup replication to live streaming.

More features are added such as support for skipping collations, support for
skipping role passwords, a feature to cache the `pg_table_size()` results
and avoid re-computing it again when doing more than one migration with
pgcopydb, and the new `pgcopydb ping` command.

This release also contains the usual amount of bug fixes and improvements,
code refactoring, etc.

### Added
* Automatically switch from prefetch/catchup to live replay mode. (#199)
* Implement pgcopydb stream replay. (#196)
* Implement support for --no-role-passwords. (#205)
* Implement "live" streaming of changes from source to target. (#185)
* Implement new command: pgcopydb ping (#191)
* Support unlogged tables for data copy (#183)
* Implement --skip-collations. (#160)
* Implement support for logical decoding plugin test_decoding. (#156)
* Implement pgcopydb list tables --drop-cache. (#150)
* Implement an option to cache pg_table_size() results. (#146)

### Changed
* Refactor our internal representation for Logical Messages. (#198)
* Refactor our clone --follow implementation. (#197)
* Refactor the transform process management. (#194)
* When streaming into a JSON file, write to a partial file first. (#187)
* When transforming into a SQL file, write to a partial file first. (#186)
* Arrange to use Logical Replication protocol metadata. (#155)
* Refrain from using wal2json computed column "nextlsn". (#151)

### Fixed
* Fix internal return value that prevented error handling. (#204)
* Avoid using stdin/stdout/stderr as variable names (#193)
* Refactor debugParameters string building to use PQExpBuffer. (#190)
* Fix same-table-copy query to use cached pgcopydb.table_size. (#184)
* Fix stream_read_context retry loop. (#181)
* Fix `dir` of stream cleanup and do cleanup for other commands. (#178)
* Fix huge memory allocations copydb_prepare_table_specs. (#175)
* Fix the SQL query that retrieves the column name for partitioning. (#176)
* Fix `lsn` for KEEPALIVE action. (#164)
* Blind fix attempt for a reported segfault. (#173)
* Error out early when work dir does not exists and is expected to. (#171)
* Trivial: Write index OIDs as unsigned integers correctly to the summary file & fix partitioning for tables with bigint primary keys with values gre>
* Fix transform process to handle endpos in between transactions. (#166)
* One of our syscalls (to mkdir) failed to include the OS error message. (#162)
* Fix migration failure of an empty database with --drop-if-exists. (#152)
* Adding dir option pgcopydb list progress command (#148)

### Packaging fixes
* Fix debian B-D in our debian build Dockerfiles. (#145)

### pgcopydb v0.10 (November 3, 2022) ###

Bug fix release, with added compatibility to Postgres 9.5, 9.6 and 10.

### Added
* Implement our own --drop-if-exists approach. (#133)
* Implement a retry strategy in case of Connection Exception. (#129)
* Implement Logical Decoding compatibility with Postgres 9.6. (#124)
* Also install tcp_keepalives_idle timeout on the target connection. (#120)

### Changed
* Made dropping of replication origin an idempotent call where we drop it only if it exists (#142)
* Enforce argc == 0 for commands without arguments. (#128)

### Fixed
* When transforming into SQL statements, double quote column names. (#141)
* Improve error message when schema.json file does not exists. (#140)
* Fix parsing pg_table_size() result, could be NULL. (#139)
* Make sure to delete the Transform Queue on exit. (#134)
* Fix parsing ACL and COMMENT archive entries from pg_dump/pg_restore. (#132)
* Fix Postgres version string max length. (#131)
* Fix parsing a JSON switch ("X") message. (#130)
* Refrain from removing the version file in make clean. (#125)
* Make it easier to navigate the source code. (#121)

### Packaging fixes
* Debian: B-D on libzstd-dev. (Closes: #1022290)
* Note where tests/extensions/countries.sql is from
* debian/copyright: Add unsplash photo license

### pgcopydb v0.9 (September 30, 2022) ###

Improve pgcopydb with same-table concurrency support, lots of bug fixes
including the new support for Extension Configuration Tables and the
automated sequences reset after a --follow cutover. New options to control
verbosity have also been added: --debug and --trace are added to --verbose.

The pgcopydb internal sub-process model has been entirely reviewed. In
version 0.9 a System V queue mechanism is used to communicate jobs from a
main process to a list of worker processes. This has been applied to CREATE
INDEX and VACUUM jobs, in a way that with --index-jobs 4 pgcopydb now starts
4 create index worker processes at start-up; where it would before create
worker processes each time a table copy would be finished.

As a result the --index-jobs limit is now enforced correctly.

### Added
* Implement same-table concurrency. (#85)
* Add support for Extension Configuration Tables (#101)
* Reset sequences at the end of pgcopydb clone --follow. (#76)
* Add a summary line for the time taken to query catalogs. (#111)
* Implement --debug and --trace, adding to --verbose. (#97)
* Implement pgcopydb list progress [ --json ] (#95)
* Store the schema elements to a schema.json file. (#94)

### Changed
* Set idle_in_transaction_session_timeout to zero. (#116)
* Make sure to transform JSON files in all cases. (#115)
* Improve pgcopydb follow docs, and some more. (#108)
* Improve docs: intro, design, concurrency. (#102)
* A round of documentation updates. (#98)
* Improve documentation of the --follow option. (#81)

### Fixed
* Bug fixes for the transform process. (#114)
* Use docker-compose run in tests, instead of docker-compose up. (#113)
* From the lab: we could have more lines in a WAL.json file. (#112)
* Fix docs for pgcopydb dump schema --target etc. (#109)
* Add more pg restore object descriptions, from shared logs. (#107)
* Fix/index jobs (#106)
* Improve clone --follow sub-process error handling. (#100)
* Fix reltuples processing: it might be NULL. (#99)
* Add some other missing pg_restore list descriptions. (#96)
* Fix version number when building Docker images. (#93)
* Improve docs and help strings. (#87)
* Fix environment variable names for table and index jobs. (#86)
* When pgcopydb clone --follow is used, setup before forking subprocesses. (#83)
* Fix unit testing to follow latest changes in the pagila database. (#82)

### pgcopydb v0.8 (July 20, 2022) ###

Implement support for Change Data Capture, the ability to replay changes
happening on the source database during and after the base copy. This is
available thanks to the new `--follow` option and allows keeping the target
database up-to-date. The Change Data Capture for Postgres is implemented
using the Logical Decoding framework and the wal2json logical decoding
plugin.

In the course of adding that capability to pgcopydb, the command line
interface have been updated. The main command is now `pgcopydb clone` and
the command `pgcopydb fork` is an alias for it. The command `pgcopydb
copy-db` has been kept around for backwards compatibility, and will be
removed in a later release.

### Added
* Run our tests suites in the GitHub Action CI.
* Implement `pgcopydb snapshot`. (#63)
* Implement support for copying roles. (#64)
* Implement pgcopydb stream prefetch. (#65)
* Implement pgcopydb stream catchup. (#66)
* Implement pgcopydb follow. (#68)
* Implement pgcopydb clone --follow. (#72)

### Changed
* Set `tcp_keepalives_idle` to 60s on the source database.
* Review the pgcopydb commands. (#62)

### Fixed
* Fix logging of total reltuples. (#47)
* Fix pgcopydb list tables --without-pkeys SQL queries. (#55)
* Make sure to use pg_restore --single-transaction.
* Fix NULL columns processing when listing schema dependencies. (#71)

### pgcopydb v0.7 (May 24, 2022) ###

Bug fix release on-top of v0.6. The filtering was not applied to the
--pre-data parts of the schema.

### Added
* Use 'pgcopydb' as our application_name in Postgres connections. (#42)
* Implement pgcopydb copy schema. (#46)

### Changed
* Skip long-running transactions when --not-consistent is used. (#45)

### Fixed
* Fix filtering of the pre-data section of the dump. (#44)

### pgcopydb v0.6 (May 16, 2022) ###

Improve pgcopydb with filtering support and the usual amount of bug fixes,
thanks to community feedback and contributions. Of note is added support for
the FreeBSD platform.

### Added
* Implement filtering support. (#19)
* Implement Unit/Regression testing suite. (#34)

### Changed
* Add versioning information to the logs output. (#36)
* Log size information about tables that are migrated (tuples, bytes). (#38)

### Fixed
* Percent-escape Postgres URI parameters. (#32)
* Fix support for inherited tables. (#35)
* Use pg_roles instead of pg_authid. (#40)

### pgcopydb v0.5 (March 11, 2022) ###

Quick turnaround release with Postgres connection management bug fixes. The
bug was caught by our debian test suite which defaults to using SSL, and
using SSL makes the problem systematic rather than unlucky.

### Changed
* Refrain from using XDG_RUNTIME_DIR for temp files. (#26)
* Increase the default timeout from 2s to 10s. (#28)

### Fixed
* Assorted bug fixes for Postgres connection handling. (#27)
* Hide more connection string passwords from pgcopydb output. (#29)

### pgcopydb v0.4 (March 8, 2022) ###

Improve pgcopydb with large object support, better documentation, and the
usual amount of bug fixes, thanks to community feedback.

#### Added
* Implement --restart, --resume --not-consistent. (#9)
* Implement --no-acl --no-comments options. (#10)
* pgcopydb list tables --without-pkey.
* Implement --snapshot, allowing to use an externally exported snapshot. (#11)
* Implement support for Large Objects (#20)

#### Changed
* Avoid logging connection string passwords. (#15)
* Implement setting GUC values to our src/dst Postgres connections. (#17)
* Improve the README with installation instructions, and docs link. (#24)
* Add a link from the main docs page back to the github project page.

#### Fixed
* Implement signal handling in the main loops. (#16)
* Fix support for exclusion constraints. (#25)

### pgcopydb v0.3 (January 24, 2022) ###

Improve pgcopydb with sequences reset support, better documentation, some
bug fixes and a better sub-process strategy.

#### Added
* Implement pgcopydb copy-db --no-owner.
* Implement pgcopydb copy commands.

#### Changed
* Share a single snapshot on the source database for the whole operations.
* Review the worker processes strategy and refactor the code.

#### Fixed
* Implement SET SEQUENCE support.
* Introduce --skip-large-objects (--skip-blobs).

### pgcopydb v0.2 (January 13, 2022) ###

Fix documentation and man pages structure, and sphinx integration/setup bugs
with the old version still active in debian buster.

### pgcopydb v0.1 (January 13, 2022) ###

First release of pgcopydb.
