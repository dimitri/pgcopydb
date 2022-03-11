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
