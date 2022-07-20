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
