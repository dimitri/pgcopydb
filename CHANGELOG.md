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
