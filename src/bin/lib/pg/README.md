# Postgres code

This directory contains PostgreSQL code that we have vendored-in.


Some parts of pg_dump has been imported to deal with the lack of a libpgdump
interface. In doing that, dependencies to common code (such as
ScanKeywordLookup and ScanKeywordCategories) has been removed, including
call sites to fmdId (now always double-quoted).
