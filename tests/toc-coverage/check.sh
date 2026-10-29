#!/bin/bash
#
# tests/toc-coverage/check.sh
#
# Verifies pgcopydb's Archive TOC lexer/parser against the real,
# captured pg_restore --list output in expected-pg-restore-list.txt
# (see fixture.sql for how it was produced). Needs only the already-
# built pgcopydb binary, no live Postgres connection: `list toc` reads
# the saved list file directly.
#
# Two checks:
#
#   1. --use-list round-trips the file byte-identically (minus the
#      header comment block a --use-list input doesn't carry): every
#      entry parses and prints back out exactly as pg_restore itself
#      would have written it, ACL/COMMENT entries included.
#
#   2. A handful of specific entries -- the ones this script exists to
#      guard, see fixture.sql's own comments for why each was added --
#      come back with the exact structured fields via --json: name/args/
#      argsList/leftArg/rightArg (Phase 1-3, FUNCTION/PROCEDURE/AGGREGATE/
#      OPERATOR signatures), and, as of Phase 4, the same signature split
#      applied to the plain (non-ACL/COMMENT) FUNCTION/PROCEDURE/AGGREGATE
#      tag itself, plus castSourceType/TargetType, userMappingUser/Server,
#      defaultAclObjectType, transformType/Language and accessMethod for
#      CAST, USER MAPPING, DEFAULT ACL, TRANSFORM and OPERATOR CLASS/
#      FAMILY.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGCOPYDB="${PGCOPYDB:-$SCRIPT_DIR/../../src/bin/pgcopydb/pgcopydb}"
EXPECTED="$SCRIPT_DIR/expected-pg-restore-list.txt"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

if [ ! -x "$PGCOPYDB" ]; then
    echo "pgcopydb binary not found at $PGCOPYDB -- build it first (make -C src/bin/pgcopydb)" >&2
    exit 1
fi

echo "Checking --use-list round-trips $EXPECTED byte-identically..."

"$PGCOPYDB" list toc --restore-list "$EXPECTED" --use-list "$OUT" > /dev/null

# expected-pg-restore-list.txt carries the real pg_restore -l header
# comment block (lines up to and including "; Selected TOC Entries:"
# and the blank "; " that follows it); a --use-list FILE doesn't need
# one, so $OUT starts right at the first real entry instead.
header_lines=$(grep -n '^; Selected TOC Entries:$' "$EXPECTED" | cut -d: -f1)
first_entry_line=$((header_lines + 2))

if ! diff -u "$OUT" <(tail -n "+${first_entry_line}" "$EXPECTED"); then
    echo "FAIL: --use-list output doesn't match the captured fixture" >&2
    exit 1
fi

echo "OK"
echo
echo "Checking structured fields for specific ACL/COMMENT entries..."

json="$("$PGCOPYDB" list toc --restore-list "$EXPECTED" --json)"

check_field() {
    local label="$1"
    local grep_for="$2"
    local must_contain="$3"

    local line
    line="$(echo "$json" | grep -F "$grep_for")"

    if [ -z "$line" ]; then
        echo "FAIL: $label -- no entry matching \"$grep_for\" in --json output" >&2
        exit 1
    fi

    if [[ "$line" != *"$must_contain"* ]]; then
        echo "FAIL: $label -- expected \"$must_contain\" in: $line" >&2
        exit 1
    fi

    echo "OK: $label"
}

# COMMENT ON FUNCTION fixture_add(a integer, b integer): a plain,
# multi-argument function signature splits into two arguments.
check_field "FUNCTION fixture_add args split" \
    '"name":"fixture_add"' \
    '"argsList":["a integer","b integer"]'

# COMMENT ON PROCEDURE fixture_proc(IN a integer): the IN argmode
# keyword is kept as part of the one argument, not stripped or split.
check_field "PROCEDURE fixture_proc keeps argmode" \
    '"name":"fixture_proc"' \
    '"argsList":["IN a integer"]'

# COMMENT ON AGGREGATE fixture_count_fn(*): format_function_arguments'
# own is_agg special case ("(*)" literally) is zero arguments, not one
# literal "*" argument.
check_field "zero-arg AGGREGATE fixture_count_fn(*)" \
    '"name":"fixture_count_fn"' \
    '"args":"*","argsList":[]'

# COMMENT ON FUNCTION fixture_scale(b integer[], a integer): an
# array-typed argument among several still splits correctly.
check_field "FUNCTION fixture_scale array-typed arg" \
    '"name":"fixture_scale"' \
    '"argsList":["b integer[]","a integer"]'

# COMMENT ON FUNCTION "Fixture Weird Fn"(a integer): a double-quoted
# function name is unescaped into item->name (no quotes here)...
check_field "quoted FUNCTION name unescaped" \
    '"tag":"COMMENT"' \
    '"name":"Fixture Weird Fn"'

# ...yet --use-list above still round-tripped it WITH its quotes, since
# that check already passed against the whole file: nothing further to
# assert here, this is just where that fact is most relevant.

# COMMENT ON OPERATOR @-@ (NONE, integer): a prefix (left-unary)
# operator's missing side is absent (hasLeftArg false), not "NONE".
check_field "prefix OPERATOR @-@ left arg absent" \
    '"name":"@-@"' \
    '"leftArg":"","rightArg":"integer"'

# COMMENT ON OPERATOR === (integer, integer): both operand types
# present for a plain binary operator.
check_field "binary OPERATOR === both args" \
    '"name":"==="' \
    '"leftArg":"integer","rightArg":"integer"'

# Phase 4: the plain (non-ACL/COMMENT) FUNCTION/PROCEDURE/AGGREGATE tag's
# own signature, "fixture_add(integer, integer)", splits the same way its
# ACL/COMMENT counterpart already does above -- bare type names only, no
# argmode/argname (format_function_signature's own shape, not pg_get_
# function_identity_arguments()'s).
check_field "plain FUNCTION fixture_add signature split" \
    '"tag":"FUNCTION","schema":"toc_fixture","relation":"","name":"fixture_add"' \
    '"args":"integer, integer","argsList":["integer","integer"]'

# CAST (toc_fixture.positive_int AS integer): both the plain entry and its
# COMMENT split into castSourceType/castTargetType.
check_field "plain CAST source/target split" \
    '"tag":"CAST"' \
    '"castSourceType":"toc_fixture.positive_int","castTargetType":"integer"'

check_field "COMMENT ON CAST source/target split" \
    '"tag":"COMMENT","schema":"","relation":"","name":"(toc_fixture.positive_int AS integer)"' \
    '"castSourceType":"toc_fixture.positive_int","castTargetType":"integer"'

# USER MAPPING postgres SERVER fixture_server: splits into user/server.
check_field "USER MAPPING user/server split" \
    '"tag":"USER MAPPING"' \
    '"userMappingUser":"postgres","userMappingServer":"fixture_server"'

# DEFAULT PRIVILEGES FOR TABLES: splits out the fixed object-type word.
check_field "DEFAULT ACL object type split" \
    '"tag":"DEFAULT ACL"' \
    '"defaultAclObjectType":"TABLES"'

# TRANSFORM FOR integer LANGUAGE plpgsql: both the plain entry and its
# COMMENT split into transformType/transformLanguage. See
# https://www.postgresql.org/docs/current/sql-createtransform.html --
# a TRANSFORM tells a procedural language (here, a fixture-only
# do-nothing C extension standing in for PL/Python or PL/Perl, see
# fixture_transform_ext/README) how to convert a given SQL type to and
# from that language's own native representation.
check_field "plain TRANSFORM type/language split" \
    '"tag":"TRANSFORM"' \
    '"transformType":"integer","transformLanguage":"plpgsql"'

check_field "COMMENT ON TRANSFORM type/language split" \
    '"tag":"COMMENT","schema":"","relation":"","name":"FOR integer LANGUAGE plpgsql"' \
    '"transformType":"integer","transformLanguage":"plpgsql"'

# COMMENT ON OPERATOR FAMILY/CLASS ... USING btree: narrows name down to
# the bare object name and splits out the access method.
check_field "OPERATOR FAMILY USING split" \
    '"tag":"COMMENT","schema":"toc_fixture","relation":"","name":"fixture_int_ops"' \
    '"accessMethod":"btree"'

check_field "OPERATOR CLASS USING split" \
    '"tag":"COMMENT","schema":"toc_fixture","relation":"","name":"fixture_int_ops_class"' \
    '"accessMethod":"btree"'

echo
echo "All toc-coverage checks passed."
