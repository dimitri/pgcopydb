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
