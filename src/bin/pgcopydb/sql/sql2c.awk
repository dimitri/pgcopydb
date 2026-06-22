# sql2c.awk — convert SQL files to C string literal data for pgcopydb
#
# Invoked by the sql/ sub-make, which is called from src/bin/pgcopydb/Makefile:
#   awk -f sql2c.awk *.sql > sql_queries_data.inc
#
# For each input file emits one static const char array.  The C symbol name
# is 'sql_' + the filename stem; no transformation is applied, so filenames
# must use only [a-z0-9_] characters.
#
# Semicolons at the end of .sql files are preserved in the embedded C string.
# The .sql files carry a trailing ';' for psql copy-paste convenience, and
# all current call sites use PQsendQuery (not PQprepare), which accepts them.

BEGIN {
	printf "/* Auto-generated — do not edit.\n"
	printf " * Run 'make gen-sql' after modifying any .sql file.\n"
	printf " */\n\n"
	prev = ""
}

FILENAME != prev {
	if (prev != "") printf ";\n\n"
	n = split(FILENAME, a, "/")
	stem = a[n]
	sub(/\.sql$/, "", stem)
	printf "static const char sql_%s[] =\n", stem
	prev = FILENAME
}

{
	gsub(/\\/, "\\\\")
	gsub(/"/, "\\\"")
	printf "\t\"%s\\n\"\n", $0
}

END {
	if (prev != "") printf ";\n"
}
