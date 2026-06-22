# sql2c.awk — convert SQL files to C string literal data for pgcopydb
#
# Invoked by the sql/ sub-make, which is called from src/bin/pgcopydb/Makefile:
#   awk -f sql2c.awk *.sql > sql_queries_data.inc
#
# For each input file emits one static const char array.  The C symbol name
# is 'sql_' + the filename stem; no transformation is applied, so filenames
# must use only [a-z0-9_] characters.
#
# The trailing ';' of each .sql file is stripped from the embedded C string.
# The .sql files carry a trailing ';' for psql copy-paste convenience, but
# some call sites interpolate the SQL into a subquery (FROM (%s)) which
# breaks when the string contains a semicolon.

BEGIN {
	printf "/* Auto-generated — do not edit.\n"
	printf " * Run 'make gen-sql' after modifying any .sql file.\n"
	printf " */\n\n"
	prev = ""
	pending = ""
	have_pending = 0
}

FILENAME != prev {
	if (have_pending) {
		sub(/;$/, "", pending)
		gsub(/\\/, "\\\\", pending)
		gsub(/"/, "\\\"", pending)
		if (pending != "") printf "\t\"%s\\n\"\n", pending
		printf ";\n\n"
	}
	n = split(FILENAME, a, "/")
	stem = a[n]
	sub(/\.sql$/, "", stem)
	printf "static const char sql_%s[] =\n", stem
	prev = FILENAME
	pending = ""
	have_pending = 0
}

{
	if (have_pending) {
		gsub(/\\/, "\\\\", pending)
		gsub(/"/, "\\\"", pending)
		printf "\t\"%s\\n\"\n", pending
	}
	pending = $0
	have_pending = 1
}

END {
	if (have_pending) {
		sub(/;$/, "", pending)
		gsub(/\\/, "\\\\", pending)
		gsub(/"/, "\\\"", pending)
		if (pending != "") printf "\t\"%s\\n\"\n", pending
	}
	if (prev != "") printf ";\n"
}
