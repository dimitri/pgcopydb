%{
/*
 * src/bin/pgcopydb/archive_toc_lexer.l
 *     flex lexer for a whole pg_restore --list line:
 *
 *       dumpId ';' catalogOid objectOid TAG remainder
 *
 * ahprintf(AH, "%d; %u %u %s %s %s %s\n", te->dumpId,
 *          te->catalogId.tableoid, te->catalogId.oid,
 *          te->desc, sanitized_schema, sanitized_name, sanitized_owner);
 * (PrintTOCSummary, pg_backup_archiver.c)
 *
 * Tokenizes the WHOLE line in one continuous stream feeding archive_toc_
 * grammar.y, which assembles the ArchiveContentItem directly from the
 * tokens -- no opaque blob is ever handed to C code to re-split.
 *
 * Token names deliberately follow scan.l/gram.y's own convention rather
 * than a uniform TOK_ prefix: plain punctuation is a bare character
 * literal ('(', ')', ',', '.', '-', ';'), a keyword this format's own
 * further-structured shapes use as a fixed separator is just its own
 * word (AS, FOR, LANGUAGE, SERVER, USING, ON, DOMAIN), and WORD/LASTWORD/
 * SPACE/OID/NO_OWNER/UNKNOWN are named for what they are -- so a grammar
 * rule reads like the input line it matches, e.g. NESTED_OPERATOR WORD
 * '(' typename ',' SPACE typename ')' owner (see archive_toc_grammar.y's
 * own file header for why that match-the-spelling property matters
 * here, and TOP_/NESTED_ keeps the tag-shape groups themselves
 * distinguishable). Unlike gram.y's own SPACE-is-never-a-token
 * convention, SPACE stays a real, returned token here on purpose: this
 * format is whitespace-*delimited* (a RELATION-shaped entry's own
 * relname/name boundary is literally "wherever the first space is"),
 * not whitespace-insignificant the way SQL's own tokens are, so the
 * grammar genuinely needs to see it, not just skip it.
 *
 * Three things make one continuous stream possible:
 *
 * 1. Every TAG keyword (dumpId's own ACL/COMMENT/TABLE/FUNCTION/CAST/...)
 *    is matched exactly once, by the same INITIAL ruleset below, whether
 *    it's the line's own outer tag or (for an ACL/COMMENT entry) the
 *    object type nested inside its remainder: the grammar re-enters
 *    INITIAL for the latter via a mid-rule action (archive_toc_begin_
 *    nested_tag, called right after ACL/COMMENT's own "- " or "nspname "
 *    is shifted), toggling archive_toc_nested_mode so the SAME matched
 *    keyword text maps to a different (nested-shaped) token -- see
 *    archive_toc_tag_token.
 *
 * 2. Everything after a tag keyword is tokenized uniformly by TOCBODY/
 *    TOCSBODY (or their quote-aware twins TOCQBODY/TOCQSBODY, see below)
 *    into WORD, SPACE, the handful of separator keywords, and structural
 *    punctuation. archive_toc_grammar.y's own productions -- not any
 *    string-splitting here or in archive_toc_parser.c -- decide how many WORDs (and
 *    SPACEs between them) make up one field, and recognize a nested
 *    parenthesized group (a type's own typmod) by ordinary grammar
 *    recursion, not a hand-counted depth variable. A multi-word field's
 *    own exact original spacing and punctuation is recovered not by
 *    reassembling it from already-tokenized pieces, which would have
 *    to guess arbitrary, unescaped text back, but by slicing the source
 *    text out verbatim between two points (see the `mark` nonterminal
 *    in archive_toc_grammar.y) -- SPACE's own job in the grammar is
 *    only ever to mark where one field's text ends and the next begins
 *    (e.g. a RELATION-shaped entry's relname/name boundary), never to
 *    get reassembled into that field's own value.
 *
 * 3. The one genuinely lexical fact this format encodes positionally
 *    rather than with its own keyword -- the owner field, simply the
 *    LAST space-delimited token on the line, or absent (a lone trailing
 *    space with nothing after it) -- is resolved by flex's own end-of-
 *    buffer anchor ($): WORD and the interior-whitespace rule each have
 *    a $-anchored twin (LASTWORD, NO_OWNER) that only matches at the
 *    true end of the scanned buffer, which archive_toc_lex_line (below)
 *    arranges for by appending a '\n' before scanning -- flex's $ needs
 *    a real line ending to anchor against, verified empirically:
 *    yy_scan_string on a string with no trailing '\n' never triggers a
 *    $-anchored rule, no matter how the buffer actually ends. This makes
 *    the owner boundary a property the LEXER discovers as it goes, not
 *    something any caller pre-scans for.
 *
 * Two start conditions handle the one further split this format itself
 * makes: TOCBODY is raw text with no escaping convention at all (every
 * top-level tag's own remainder, and most nested ACL/COMMENT object
 * types) -- a literal '"' or '.' in it is just another character, not
 * something to unescape or treat as a separator, because pg_dump's own
 * sanitize_line() only ever escapes newlines. TOCQBODY is its twin for
 * the handful of nested object types (TABLE/VIEW/MATERIALIZED_VIEW/
 * FOREIGN_TABLE/INDEX/SEQUENCE/COLUMN/CONSTRAINT/TRIGGER/POLICY/RULE/
 * FUNCTION/PROCEDURE/AGGREGATE/OPERATOR) whose own identifier pg_dump
 * really did build with fmtId(), so really can be double-quoted --
 * archive_toc_tag_token picks whichever one applies. Getting this
 * backwards is exactly what broke CI the first time around: a table
 * literally named `column.acl "quoted" test` has no quoting at all at
 * the top level, so treating its own '"' as the start of a quoted
 * identifier (TOCBODY wrongly behaving like TOCQBODY) silently dropped
 * the whole entry.
 *
 * TOCBODY/TOCQBODY each have a further own twin, TOCSBODY/TOCQSBODY
 * (plus NAMESPACE's own twin NAMESPACES, reached first for a top-level
 * entry so it can BEGIN() the right one of the two once nspname is
 * done): the handful of shapes that have one of pg_dump's own fixed
 * keyword separators in their remainder (CAST, TRANSFORM, USER MAPPING,
 * DEFAULT ACL, an OPERATOR CLASS/FAMILY ACL/COMMENT, a relation-owned
 * ACL/COMMENT) reach TOCSBODY/TOCQSBODY, where AS/FOR/LANGUAGE/SERVER/
 * USING/ON/CAST/USER MAPPING/DEFAULT PRIVILEGES are active rules; every
 * other shape reaches plain TOCBODY/TOCQBODY, where none of those nine
 * patterns exist at all. See the TOCSBODY/TOCQSBODY comment, just above
 * this file's first %%, and archive_toc_tag_token, which is the one
 * place that already knows an entry's whole shape and so is the one
 * place that picks which of the two to BEGIN() into -- this is the
 * same backwards-is-a-silent-bug hazard as TOCBODY/TOCQBODY's own
 * split above, one level further in: a table literally named `... AS
 * (SELECT ...` (see tests/unit/setup/6-multiline-table-name.sql) has
 * no AS keyword of its own at all, so it must never reach TOCSBODY.
 *
 * Every token that carries a value (an OID's number, a tag keyword's
 * ArchiveItemDesc, a WORD's text) sets it in archive_toc_lval -- bison's
 * own per-token value slot, copied onto its parse stack the moment the
 * token is shifted -- rather than in a plain global a grammar action
 * might read later: bison's LALR engine can pull the NEXT token (to
 * decide whether to shift or reduce) before running a reduce action for
 * tokens already shifted, so a plain global set by this lexer would get
 * overwritten by that lookahead before anything downstream read it.
 * archive_toc_word below is not that -- it is this lexer's OWN, purely
 * internal accumulator for a double-quoted identifier being matched
 * across possibly several TOCQIDENT rules, copied into archive_toc_lval
 * only once, right when that one token is finished and returned.
 */

#include <stdlib.h>
#include <string.h>

#include "parson.h"

#include "pgcmd.h"
#include "string_utils.h"
#include "archive_toc_lexer.h"
#include "archive_toc_grammar.h"

static int archive_toc_tag_token(ArchiveItemDesc desc);
static int archive_toc_word_or_keyword_token(bool isLast);
static void archive_toc_ident_append(const char *s, int len);

/* purely an internal multi-match accumulator, see the file header */
static char archive_toc_word[BUFSIZE];

/*
 * archive_toc_expect_domain and archive_toc_expect_transform_kw gate the
 * two context-sensitive reclassifications archive_toc_word_or_keyword_
 * token performs -- see that function's own comment. Declared here,
 * ahead of the rule section below that sets/reads them, rather than
 * alongside archive_toc_tag_token further down (past the first %%):
 * the rules themselves run inside archive_toc_lex(), generated from
 * the section ABOVE that %%, so a declaration placed after it, like
 * archive_toc_nested_mode's own, would not be visible there yet.
 */
static bool archive_toc_expect_domain = false;
static bool archive_toc_expect_transform_kw = false;

/*
 * archive_toc_token_end is the end of whichever token archive_toc_lex()
 * just matched, as a raw pointer into flex's own stable internal scan
 * buffer (the one yy_scan_string allocates and keeps for as long as the
 * current line's scan lasts, not the caller's own copy, see archive_
 * toc_lex_line below). YY_USER_ACTION runs this before EVERY rule's own
 * action, in every start condition, with no per-rule code needed.
 *
 * archive_toc_grammar.y's own `mark` nonterminal (a zero-width mid-rule
 * placeholder) reads this at specific, fixed points in a production --
 * right after the last token of a span it wants to capture verbatim --
 * to slice that exact source text out, byte for byte, rather than
 * reassembling it from already-tokenized pieces. Safe to read only from
 * a mid-rule action positioned immediately after the token it means to
 * mark -- see `mark`'s own comment for why reading it from an ordinary
 * (end-of-rule) action would not be.
 */
const char *archive_toc_token_end;

#define YY_USER_ACTION \
	archive_toc_token_end = yytext + yyleng;

%}

%option prefix="archive_toc_"
%option noyywrap
%option nounput
%option noinput
%option never-interactive
%option stack

/*
 * TOCBODY/TOCQBODY and their own "S" twins, TOCSBODY/TOCQSBODY, are this
 * format's one further split carried all the way into the start-condition
 * set itself, not just into TOCBODY/TOCQBODY's own quoting split (see the
 * file header): TOCSBODY/TOCQSBODY are reached ONLY for the handful of
 * shapes that actually have one of pg_dump's own fixed keyword separators
 * in their remainder (CAST, TRANSFORM, USER MAPPING, DEFAULT ACL, an
 * OPERATOR CLASS/FAMILY ACL/COMMENT, a relation-owned ACL/COMMENT) -- see
 * archive_toc_tag_token, the one place that already knows this entry's
 * whole shape and picks which of the four to BEGIN(). Every other shape
 * (TOP_SIMPLE/TOP_RELATION/TOP_FUNCLIKE/TOP_OPERATOR/NESTED_EXTENSION/
 * NESTED_GENERIC/NESTED_RELATION_LIKE/NESTED_COLUMN/NESTED_FUNCLIKE/
 * NESTED_OPERATOR) reaches plain TOCBODY/TOCQBODY, where AS/FOR/LANGUAGE/
 * SERVER/USING/ON/CAST/USER MAPPING/DEFAULT PRIVILEGES simply have no
 * rule at all -- not a rule that matches and then backs off, a table
 * literally named `... AS (SELECT ...` (a real, already-hit regression,
 * not a hypothetical: see tests/unit/setup/6-multiline-table-name.sql)
 * never has a chance to be seen as anything but plain WORD/SPACE text,
 * because the AS pattern isn't active in the state that scans it.
 *
 * NAMESPACE's own twin, NAMESPACES, exists for the same reason one level
 * up: a top-level CAST/TRANSFORM/USER MAPPING/DEFAULT ACL entry's own
 * nspname still has to be scanned (same two rules as NAMESPACE) before
 * its body does, so NAMESPACES is reached instead of NAMESPACE precisely
 * so IT can BEGIN(TOCSBODY) rather than BEGIN(TOCBODY) once nspname is
 * done -- see archive_toc_tag_token's top-level switch. ACL/COMMENT
 * themselves always use plain NAMESPACE: their own nested object type
 * (archive_toc_begin_nested_tag, below) re-enters INITIAL before any
 * TOCBODY/TOCQBODY token is ever scanned, so which of TOCBODY/TOCSBODY
 * NAMESPACE's own rules would have landed in is moot for them.
 *
 * %option stack (above) backs TOCQIDENT's own quote: TOCQBODY and
 * TOCQSBODY both push it with yy_push_state on '"' and it pops back with
 * yy_pop_state on the closing '"', so a quoted identifier returns to
 * whichever of the two sent it there, with no flag or extra state of its
 * own needed to remember which.
 */
%x NAMESPACE
%x NAMESPACES
%x TOCBODY
%x TOCSBODY
%x TOCQBODY
%x TOCQSBODY
%x TOCQIDENT

%%

<INITIAL>{

[0-9]+                            { archive_toc_lval.oid = strtoull(yytext, NULL, 10);
                                     return OID; }
";"                                { return ';'; }
" "+                               ;

"ACCESS METHOD"" "+                   { return archive_toc_tag_token(ARCHIVE_TAG_ACCESS_METHOD); }
"ACL"" "+                             { return archive_toc_tag_token(ARCHIVE_TAG_ACL); }
"AGGREGATE"" "+                       { return archive_toc_tag_token(ARCHIVE_TAG_AGGREGATE); }
"ATTRDEF"" "+                         { return archive_toc_tag_token(ARCHIVE_TAG_ATTRDEF); }
"BLOB DATA"" "+                       { return archive_toc_tag_token(ARCHIVE_TAG_BLOB_DATA); }
"BLOB"" "+                            { return archive_toc_tag_token(ARCHIVE_TAG_BLOB); }
"CAST"" "+                            { return archive_toc_tag_token(ARCHIVE_TAG_CAST); }
"CHECK CONSTRAINT"" "+                { return archive_toc_tag_token(ARCHIVE_TAG_CHECK_CONSTRAINT); }
"COLLATION"" "+                       { return archive_toc_tag_token(ARCHIVE_TAG_COLLATION); }
"COLUMN"" "+                          { return archive_toc_tag_token(ARCHIVE_TAG_COLUMN); }
"COMMENT"" "+                         { return archive_toc_tag_token(ARCHIVE_TAG_COMMENT); }
"CONSTRAINT"" "+                      { return archive_toc_tag_token(ARCHIVE_TAG_CONSTRAINT); }
"CONVERSION"" "+                      { return archive_toc_tag_token(ARCHIVE_TAG_CONVERSION); }
"DATABASE"" "+                        { return archive_toc_tag_token(ARCHIVE_TAG_DATABASE); }
"DEFAULT ACL"" "+                     { return archive_toc_tag_token(ARCHIVE_TAG_DEFAULT_ACL); }
"DEFAULT"" "+                         { return archive_toc_tag_token(ARCHIVE_TAG_DEFAULT); }
"DOMAIN"" "+                          { return archive_toc_tag_token(ARCHIVE_TAG_DOMAIN); }
"DUMMY TYPE"" "+                      { return archive_toc_tag_token(ARCHIVE_TAG_DUMMY_TYPE); }
"EVENT TRIGGER"" "+                   { return archive_toc_tag_token(ARCHIVE_TAG_EVENT_TRIGGER); }
"EXTENSION"" "+                       { return archive_toc_tag_token(ARCHIVE_TAG_EXTENSION); }
"FK CONSTRAINT"" "+                   { return archive_toc_tag_token(ARCHIVE_TAG_FK_CONSTRAINT); }
"FOREIGN DATA WRAPPER"" "+            { return archive_toc_tag_token(ARCHIVE_TAG_FOREIGN_DATA_WRAPPER); }
"FOREIGN SERVER"" "+                  { return archive_toc_tag_token(ARCHIVE_TAG_FOREIGN_SERVER); }
"FOREIGN TABLE"" "+                   { return archive_toc_tag_token(ARCHIVE_TAG_FOREIGN_TABLE); }
"FUNCTION"" "+                        { return archive_toc_tag_token(ARCHIVE_TAG_FUNCTION); }
"INDEX ATTACH"" "+                    { return archive_toc_tag_token(ARCHIVE_TAG_INDEX_ATTACH); }
"INDEX"" "+                           { return archive_toc_tag_token(ARCHIVE_TAG_INDEX); }
"LANGUAGE"" "+                        { return archive_toc_tag_token(ARCHIVE_TAG_LANGUAGE); }
"LARGE OBJECT"" "+                    { return archive_toc_tag_token(ARCHIVE_TAG_LARGE_OBJECT); }
"MATERIALIZED VIEW DATA"" "+          { return archive_toc_tag_token(ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW); }
"MATERIALIZED VIEW"" "+               { return archive_toc_tag_token(ARCHIVE_TAG_MATERIALIZED_VIEW); }
"OPERATOR CLASS"" "+                  { return archive_toc_tag_token(ARCHIVE_TAG_OPERATOR_CLASS); }
"OPERATOR FAMILY"" "+                 { return archive_toc_tag_token(ARCHIVE_TAG_OPERATOR_FAMILY); }
"OPERATOR"" "+                        { return archive_toc_tag_token(ARCHIVE_TAG_OPERATOR); }
"POLICY"" "+                          { return archive_toc_tag_token(ARCHIVE_TAG_POLICY); }
"PROCEDURAL LANGUAGE"" "+             { return archive_toc_tag_token(ARCHIVE_TAG_PROCEDURAL_LANGUAGE); }
"PROCEDURE"" "+                       { return archive_toc_tag_token(ARCHIVE_TAG_PROCEDURE); }
"PUBLICATION TABLES IN SCHEMA"" "+    { return archive_toc_tag_token(ARCHIVE_TAG_PUBLICATION_TABLES_IN_SCHEMA); }
"PUBLICATION TABLE"" "+               { return archive_toc_tag_token(ARCHIVE_TAG_PUBLICATION_TABLE); }
"PUBLICATION"" "+                     { return archive_toc_tag_token(ARCHIVE_TAG_PUBLICATION); }
"ROW SECURITY"" "+                    { return archive_toc_tag_token(ARCHIVE_TAG_ROW_SECURITY); }
"RULE"" "+                            { return archive_toc_tag_token(ARCHIVE_TAG_RULE); }
"SCHEMA"" "+                          { return archive_toc_tag_token(ARCHIVE_TAG_SCHEMA); }
"SEQUENCE OWNED BY"" "+               { return archive_toc_tag_token(ARCHIVE_TAG_SEQUENCE_OWNED_BY); }
"SEQUENCE SET"" "+                    { return archive_toc_tag_token(ARCHIVE_TAG_SEQUENCE_SET); }
"SEQUENCE"" "+                        { return archive_toc_tag_token(ARCHIVE_TAG_SEQUENCE); }
"SERVER"" "+                          { return archive_toc_tag_token(ARCHIVE_TAG_SERVER); }
"SHELL TYPE"" "+                      { return archive_toc_tag_token(ARCHIVE_TAG_SHELL_TYPE); }
"STATISTICS"" "+                      { return archive_toc_tag_token(ARCHIVE_TAG_STATISTICS); }
"SUBSCRIPTION"" "+                    { return archive_toc_tag_token(ARCHIVE_TAG_SUBSCRIPTION); }
"TABLE ATTACH"" "+                    { return archive_toc_tag_token(ARCHIVE_TAG_TABLE_ATTACH); }
"TABLE DATA"" "+                      { return archive_toc_tag_token(ARCHIVE_TAG_TABLE_DATA); }
"TABLE"" "+                           { return archive_toc_tag_token(ARCHIVE_TAG_TABLE); }
"TEXT SEARCH CONFIGURATION"" "+       { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_CONFIGURATION); }
"TEXT SEARCH DICTIONARY"" "+          { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_DICTIONARY); }
"TEXT SEARCH PARSER"" "+              { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_PARSER); }
"TEXT SEARCH TEMPLATE"" "+            { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_TEMPLATE); }
"TRANSFORM"" "+                       { return archive_toc_tag_token(ARCHIVE_TAG_TRANSFORM); }
"TRIGGER"" "+                         { return archive_toc_tag_token(ARCHIVE_TAG_TRIGGER); }
"TYPE"" "+                            { return archive_toc_tag_token(ARCHIVE_TAG_TYPE); }
"USER MAPPING"" "+                    { return archive_toc_tag_token(ARCHIVE_TAG_USER_MAPPING); }
"VIEW"" "+                            { return archive_toc_tag_token(ARCHIVE_TAG_VIEW); }

.                                  { return UNKNOWN; }

}

<NAMESPACE>{

  /*
   * Both rules consume their own mandatory trailing separator space
   * directly (sanitize_line() -- see dumputils.c -- always prints
   * exactly one, namespace/name/owner's own common separator): nothing
   * else ever needs to see that one space as its own token, so folding
   * it in here means every grammar rule using `nspname` doesn't have to
   * spell out a SPACE right after it.
   *
   * "-" specifically is sanitize_line(te->namespace, true)'s own literal
   * substitution for a NULL te->namespace (see PrintTOCSummary in
   * pg_backup_archiver.c) -- not a value to pass through, a POSITION
   * that means "this object has no schema" (a global object: SCHEMA,
   * DATABASE, EXTENSION, PUBLICATION, SUBSCRIPTION, USER MAPPING, CAST,
   * TRANSFORM, EVENT TRIGGER, SERVER, FOREIGN DATA WRAPPER, ...) --
   * mirroring NO_OWNER's own token for the exact same idiom one field
   * over (PrintTOCSummary's sanitize_line(te->owner, false) call uses
   * want_hyphen=false there, so an absent owner is never "-", only ever
   * this trailing-space convention; absent namespace is the only field
   * that ever uses the literal hyphen).
   */
"-"" "                             { BEGIN(TOCBODY); return NO_NSPNAME; }
[^ \n]+" "                         { BEGIN(TOCBODY);
                                     yytext[yyleng - 1] = '\0';
                                     archive_toc_lval.word.text = strdup(yytext);
                                     archive_toc_lval.word.quoted = false;
                                     archive_toc_lval.word.end = archive_toc_token_end;
                                     return WORD; }

}

<NAMESPACES>{

  /*
   * NAMESPACES's own twin of NAMESPACE's two rules above -- identical
   * except for landing in TOCSBODY rather than TOCBODY, see this file's
   * TOCSBODY/TOCQSBODY comment above the first %%.
   */
"-"" "                             { BEGIN(TOCSBODY); return NO_NSPNAME; }
[^ \n]+" "                         { BEGIN(TOCSBODY);
                                     yytext[yyleng - 1] = '\0';
                                     archive_toc_lval.word.text = strdup(yytext);
                                     archive_toc_lval.word.quoted = false;
                                     archive_toc_lval.word.end = archive_toc_token_end;
                                     return WORD; }

}

<TOCBODY,TOCSBODY>{

  /*
   * Shared by TOCBODY and TOCSBODY: these eight rules (SPACE/NO_OWNER/
   * punctuation/WORD/LASTWORD) apply identically to every TOCBODY-
   * reached shape whether or not it has a further-structured keyword
   * vocabulary of its own -- see this file's TOCSBODY/TOCQSBODY comment
   * above the first %%. Only TOCSBODY additionally has the keyword
   * rules below; this block is unchanged from before that split.
   */
" "+$                              { return NO_OWNER; }

" "+                               { archive_toc_lval.pos = archive_toc_token_end;
                                     return SPACE; }

"("                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return '('; }
")"                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return ')'; }
","                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return ','; }
"."                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return '.'; }

[^ ().,\n]+$                       { return archive_toc_word_or_keyword_token(true); }
[^ ().,\n]+                        { return archive_toc_word_or_keyword_token(false); }

}

<TOCSBODY>{

  /*
   * TOCSBODY-only: this format's own fixed keyword separators, one per
   * further-structured shape (CAST's " AS ", TRANSFORM's own "FOR ...
   * LANGUAGE ...", USER MAPPING's " SERVER ", DEFAULT ACL's "FOR ..."
   * -- see pg_dump.c's own dumpCast/dumpTransform/dumpUserMappings/
   * dumpDefaultACL), always with exactly the single space pg_dump's own
   * fixed sprintf()-style formatting puts on each side. Matching ALL of
   * that (keyword and both surrounding runs of whitespace) as one token
   * means the grammar itself never has to spell out an explicit SPACE
   * around any of them (see archive_toc_grammar.y's own file header) --
   * "one or more spaces" on each side rather than hard-coding exactly
   * one still correctly handles a hand-edited file with irregular
   * spacing, at the cost of no longer being able to tell such a file
   * apart from a normal one. CAST_KW below is the same idea with only a
   * trailing space to bake in (see its own comment); DOMAIN and
   * TRANSFORM_KW are the two keywords left that genuinely need post-
   * match recognition instead, since neither has a whitespace
   * convention of its own to bake into a dedicated pattern.
   *
   * Unlike the flag this block used to be gated by, these patterns are
   * simply absent from TOCBODY: a table literally named `... AS
   * (SELECT ...` (see tests/unit/setup/6-multiline-table-name.sql)
   * reaches plain TOCBODY, where there is no AS rule at all for its own
   * " AS " run to match -- the plain SPACE/WORD rules above are the
   * only ones active there, no REJECT fallback needed.
   */
" "+"AS"" "+                       { return AS; }
  /*
   * FOR's own leading space is "zero or more", not "one or more" like
   * the rest of this block: NESTED_TRANSFORM reaches it with no
   * separator left to consume (its own tag keyword's pattern,
   * <INITIAL> above, already baked in the one space that used to sit
   * here -- unlike TOP_TRANSFORM's/TOP_DEFAULT_ACL's repeated-word
   * tokens ahead of it, which still leave FOR's own leading space
   * alone, see CAST_KW/TRANSFORM_KW/DEFAULT_ACL_KW's own comments).
   */
" "*"FOR"" "+                      { archive_toc_lval.pos = yytext + strspn(yytext, " ");
                                     return FOR; }
" "+"LANGUAGE"" "+                 { return LANGUAGE_KW; }
" "+"SERVER"" "+                   { return SERVER_KW; }
" "+"USING"" "+                    { return USING; }

  /*
   * CAST's and USER MAPPING's own repeated tag words (dumpCast's and
   * dumpUserMappings' labelq always write "CAST (sourcetype AS
   * targettype)" / "USER MAPPING username SERVER servername" for the
   * remainder, pg_dump.c) -- unlike the four above, nothing ever
   * precedes either with a run of whitespace of its own (nspname's own
   * WORD/NO_NSPNAME pattern already consumes the single separator
   * space ahead of it, see <NAMESPACE>/<NAMESPACES> above), so only the
   * trailing space needs baking in. DEFAULT ACL's own repeated "DEFAULT
   * PRIVILEGES" (dumpDefaultACL, pg_dump.c) needs neither: FOR's own
   * pattern already bakes the separator space ahead of it, same as
   * TRANSFORM_KW below -- but its one internal space, between the two
   * words, is still part of this single fixed literal, not something
   * a later token could ever consume on its own.
   */
"CAST"" "+                         { return CAST_KW; }
"USER"" "+"MAPPING"" "+            { return USER_MAPPING_KW; }
"DEFAULT"" "+"PRIVILEGES"          { return DEFAULT_ACL_KW; }

}

<TOCQBODY,TOCQSBODY>{

  /*
   * Shared by TOCQBODY and TOCQSBODY, mirroring TOCBODY/TOCSBODY's own
   * split above -- including the quote rule, which pushes onto flex's
   * own state stack (%option stack) rather than a fixed BEGIN(), so
   * TOCQIDENT's closing quote can pop back to whichever of the two sent
   * it there (see this file's TOCSBODY/TOCQSBODY comment, above the
   * first %%).
   */
" "+$                              { return NO_OWNER; }

" "+                               { archive_toc_lval.pos = archive_toc_token_end;
                                     return SPACE; }

\"                                 { yy_push_state(TOCQIDENT);
                                     archive_toc_word[0] = '\0'; }

"("                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return '('; }
")"                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return ')'; }
","                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return ','; }
"."                                { archive_toc_lval.pos = archive_toc_token_end;
                                     return '.'; }

[^ ().,\"\n]+$                     { return archive_toc_word_or_keyword_token(true); }
[^ ().,\"\n]+                      { return archive_toc_word_or_keyword_token(false); }

}

<TOCQSBODY>{

  /*
   * TOCQSBODY-only: NESTED_ON_RELATION's own " ON " separator, the one
   * structured keyword any quote-aware shape uses. Unlike the flag this
   * used to be gated by, TOCQBODY (NESTED_RELATION_LIKE/COLUMN/FUNCLIKE/
   * OPERATOR, none of which ever expect ON) simply has no ON rule at
   * all, so e.g. a quoted column name containing " ON " is never even
   * offered the chance to match it.
   */
" "+"ON"" "+                       { archive_toc_expect_domain = true;
                                     return ON; }

}

<TOCQIDENT>{

\"\"                               { archive_toc_ident_append("\"", 1); }
\"                                 { yy_pop_state();
                                     archive_toc_lval.word.text = strdup(archive_toc_word);
                                     archive_toc_lval.word.quoted = true;
                                     archive_toc_lval.word.end = archive_toc_token_end;
                                     return WORD; }
[^\"\n]+                           { archive_toc_ident_append(yytext, yyleng); }

}

%%

/*
 * archive_toc_tag_token classifies a just-matched tag keyword into the
 * bison token for its own shape, which differs depending on whether this
 * keyword is the line's own outer tag or (archive_toc_nested_mode true)
 * the object type nested inside an ACL/COMMENT entry's remainder -- see
 * the file header and archive_toc_begin_nested_tag below. Either way,
 * archive_toc_lval.tag is set so the grammar action that consumes this
 * token can read the specific ArchiveItemDesc and its literal keyword
 * text straight off bison's own parse stack, into item->desc/description
 * or item->objectType/objectTypeText.
 *
 * Every one of these keywords' own pattern bakes in its mandatory
 * trailing separator space (one or more, same "-"" "/WORD" " convention
 * as NAMESPACE's own rules, see archive_toc_lexer.l's file header),
 * so yytext itself still has it -- trimmed off here, once, rather than
 * by every individual pattern's own action, since it's the one thing
 * they'd all repeat identically. A multi-word tag's own internal space
 * ("DEFAULT ACL", "USER MAPPING", ...) is untouched: only space run
 * actually at the end goes.
 *
 * ACL and COMMENT themselves switch to NAMESPACE (their own outer
 * namespace field, exactly like every other top-level tag); every other
 * top-level tag does too. A nested object type never has a namespace of
 * its own (the ACL/COMMENT's own one already covers it), so it goes
 * straight to TOCBODY or TOCQBODY instead.
 */
static bool archive_toc_nested_mode = false;

static int
archive_toc_tag_token(ArchiveItemDesc desc)
{
	int len = yyleng;

	while (len > 0 && yytext[len - 1] == ' ')
	{
		--len;
	}
	yytext[len] = '\0';

	archive_toc_lval.tag.desc = desc;
	archive_toc_lval.tag.text = strdup(yytext);

	if (!archive_toc_nested_mode)
	{
		/*
		 * Each case below picks its own BEGIN() target, NAMESPACE or
		 * NAMESPACES, instead of one unconditional BEGIN(NAMESPACE) up
		 * front: NAMESPACE's own two rules land in TOCBODY once nspname
		 * is done, NAMESPACES's twin lands in TOCSBODY -- see this
		 * file's TOCSBODY/TOCQSBODY comment, above the first %%. ACL/
		 * COMMENT always use plain NAMESPACE: their own nested object
		 * type (archive_toc_begin_nested_tag, below) re-enters INITIAL
		 * before any TOCBODY/TOCSBODY token is ever scanned, so which of
		 * the two NAMESPACE's own rules would have reached is moot for
		 * them.
		 */
		switch (desc)
		{
			case ARCHIVE_TAG_ACL:
			{
				BEGIN(NAMESPACE);
				return ACL;
			}

			case ARCHIVE_TAG_COMMENT:
			{
				BEGIN(NAMESPACE);
				return COMMENT;
			}

			case ARCHIVE_TAG_FUNCTION:
			case ARCHIVE_TAG_PROCEDURE:
			case ARCHIVE_TAG_AGGREGATE:
			{
				BEGIN(NAMESPACE);
				return TOP_FUNCLIKE;
			}

			case ARCHIVE_TAG_OPERATOR:
			{
				BEGIN(NAMESPACE);
				return TOP_OPERATOR;
			}

			case ARCHIVE_TAG_CAST:
			{
				BEGIN(NAMESPACES);
				return TOP_CAST;
			}

			case ARCHIVE_TAG_TRANSFORM:
			{
				BEGIN(NAMESPACES);
				archive_toc_expect_transform_kw = true;
				return TOP_TRANSFORM;
			}

			case ARCHIVE_TAG_USER_MAPPING:
			{
				BEGIN(NAMESPACES);
				return TOP_USER_MAPPING;
			}

			case ARCHIVE_TAG_DEFAULT_ACL:
			{
				BEGIN(NAMESPACES);
				return TOP_DEFAULT_ACL;
			}

			case ARCHIVE_TAG_ATTRDEF:
			case ARCHIVE_TAG_CHECK_CONSTRAINT:
			case ARCHIVE_TAG_CONSTRAINT:
			case ARCHIVE_TAG_DEFAULT:
			case ARCHIVE_TAG_FK_CONSTRAINT:
			case ARCHIVE_TAG_INDEX_ATTACH:
			case ARCHIVE_TAG_POLICY:
			case ARCHIVE_TAG_PUBLICATION_TABLES_IN_SCHEMA:
			case ARCHIVE_TAG_PUBLICATION_TABLE:
			case ARCHIVE_TAG_RULE:
			case ARCHIVE_TAG_TABLE_ATTACH:
			case ARCHIVE_TAG_TRIGGER:
			{
				BEGIN(NAMESPACE);
				return TOP_RELATION;
			}

			/*
			 * default here is not an unrecognized-tag catch-all: every
			 * desc that can reach this switch already comes from one of
			 * the keyword patterns at the top of this file (%% section),
			 * each tied to one specific ArchiveItemDesc -- text this
			 * grammar genuinely doesn't recognize never gets here at all
			 * (it just never matches any of those patterns, so it scans
			 * as an ordinary WORD instead). What lands in default is
			 * every top-level tag with no further-structured shape of
			 * its own -- ACCESS METHOD, DATABASE, SCHEMA, TYPE, and the
			 * rest -- which TOP_SIMPLE's own grammar production (one
			 * nspname/name/owner shape) already covers correctly, so
			 * there is nothing more specific to dispatch them to.
			 */
			default:
			{
				BEGIN(NAMESPACE);
				return TOP_SIMPLE;
			}
		}
	}
	else
	{
		/*
		 * One switch, not two: each case sets BOTH which start condition
		 * this object type's own identifier scans under AND which
		 * NESTED_* token it returns, together, since they're the same
		 * per-tag decision made twice over otherwise -- a prior version
		 * of this function used two separate switches over this same
		 * desc enum, one per decision, which is exactly the duplicated-
		 * enumeration hazard this whole rewrite exists to retire
		 * (forgetting to update one of two case lists when adding a tag
		 * is a real, already-hit failure mode, see TOCBODY/TOCQBODY's
		 * own file-header comment on the bug that first exposed it).
		 * Which of TOCBODY/TOCSBODY or TOCQBODY/TOCQSBODY each case
		 * BEGIN()s into is that same per-tag decision too -- see this
		 * file's TOCSBODY/TOCQSBODY comment, above the first %%.
		 *
		 * Only these own an identifier pg_dump itself built with
		 * fmtId() and so can be double-quoted (archive_toc_object_has_
		 * ident's old list, carried over) -- everything else nested,
		 * like every top-level tag, is raw sanitize_line() text with no
		 * escaping convention at all, so a literal '"' in it (a
		 * deliberately adversarial test identifier, say) is just
		 * another character, not a quote to unescape. TOCQBODY is
		 * TOCBODY's quote-aware twin, see the file header.
		 */
		switch (desc)
		{
			case ARCHIVE_TAG_TABLE:
			case ARCHIVE_TAG_VIEW:
			case ARCHIVE_TAG_MATERIALIZED_VIEW:
			case ARCHIVE_TAG_FOREIGN_TABLE:
			case ARCHIVE_TAG_INDEX:
			case ARCHIVE_TAG_SEQUENCE:
			{
				BEGIN(TOCQBODY);
				return NESTED_RELATION_LIKE;
			}

			case ARCHIVE_TAG_COLUMN:
			{
				BEGIN(TOCQBODY);
				return NESTED_COLUMN;
			}

			case ARCHIVE_TAG_EXTENSION:
			{
				BEGIN(TOCBODY);
				return NESTED_EXTENSION;
			}

			case ARCHIVE_TAG_CONSTRAINT:
			case ARCHIVE_TAG_TRIGGER:
			case ARCHIVE_TAG_POLICY:
			case ARCHIVE_TAG_RULE:
			{
				BEGIN(TOCQSBODY);
				return NESTED_ON_RELATION;
			}

			case ARCHIVE_TAG_FUNCTION:
			case ARCHIVE_TAG_PROCEDURE:
			case ARCHIVE_TAG_AGGREGATE:
			{
				BEGIN(TOCQBODY);
				return NESTED_FUNCLIKE;
			}

			case ARCHIVE_TAG_OPERATOR:
			{
				BEGIN(TOCQBODY);
				return NESTED_OPERATOR;
			}

			case ARCHIVE_TAG_CAST:
			{
				BEGIN(TOCSBODY);
				return NESTED_CAST;
			}

			case ARCHIVE_TAG_TRANSFORM:
			{
				BEGIN(TOCSBODY);
				archive_toc_expect_transform_kw = true;
				return NESTED_TRANSFORM;
			}

			case ARCHIVE_TAG_OPERATOR_CLASS:
			case ARCHIVE_TAG_OPERATOR_FAMILY:
			{
				BEGIN(TOCSBODY);
				return NESTED_OPCLASS_FAMILY;
			}

			/*
			 * Same reasoning as the top-level switch's own default,
			 * above: every desc reaching here already came from one of
			 * this file's keyword patterns, so this is every nested
			 * object type with no further-structured shape of its own
			 * (ACL/COMMENT itself can never recurse into another ACL/
			 * COMMENT, and most top-level-only tags -- DATABASE, SCHEMA,
			 * LANGUAGE, SERVER, ... -- never actually appear nested in
			 * real pg_restore --list output either), not a fallback for
			 * unrecognized text.
			 */
			default:
			{
				BEGIN(TOCBODY);
				return NESTED_GENERIC;
			}
		}
	}
}


/*
 * archive_toc_begin_nested_tag is the mid-rule action archive_toc_
 * grammar.y calls right after shifting an ACL/COMMENT entry's own outer
 * "- " or "nspname " (see the file header): it switches back to INITIAL
 * so the next tag-keyword match is the object type nested inside the
 * remainder, and flags archive_toc_tag_token to classify it into one of
 * the NESTED_* tokens instead of a TOP_* one.
 */
void
archive_toc_begin_nested_tag(void)
{
	archive_toc_nested_mode = true;
	BEGIN(INITIAL);
}


/*
 * archive_toc_word_or_keyword_token classifies a plain (unquoted) run of
 * non-space, non-structural characters matched in TOCBODY/TOCQBODY:
 * either DOMAIN (only right after ON) or TRANSFORM's own repeated tag
 * word (only as a TRANSFORM entry's very first body word -- dumpTransform's
 * labelq always writes "TRANSFORM FOR typename LANGUAGE langname" for the
 * remainder, pg_dump.c; FOR's own pattern already bakes in the separator
 * space ahead of it, see above, so unlike CAST_KW this one needs no
 * pattern of its own), or a generic word.
 *
 * Unlike a blanket text match, both reclassifications are gated by
 * archive_toc_expect_domain/archive_toc_expect_transform_kw (see their
 * own comment, above archive_toc_tag_token): DOMAIN is not actually a
 * reserved Postgres keyword (it's unreserved) and TRANSFORM isn't a
 * keyword at all, so either can be a real, unquoted object name anywhere
 * else a WORD is expected -- a table literally named `DOMAIN` must still
 * parse as an ordinary name. Both flags are consumed (reset to false)
 * on every call, win or lose, so a stale flag from several tokens back
 * can never leak into an unrelated later match.
 *
 * isLast (true only for the $-anchored twin of this rule, see the file
 * header) distinguishes LASTWORD from plain WORD for the generic case --
 * the grammar's own owner-boundary rule, not anything this function
 * decides.
 */
static int
archive_toc_word_or_keyword_token(bool isLast)
{
	bool expectDomain = archive_toc_expect_domain;
	bool expectTransformKw = archive_toc_expect_transform_kw;

	archive_toc_expect_domain = false;
	archive_toc_expect_transform_kw = false;

	if (expectDomain && streq(yytext, "DOMAIN"))
	{
		return DOMAIN;
	}

	if (expectTransformKw && streq(yytext, "TRANSFORM"))
	{
		return TRANSFORM_KW;
	}

	archive_toc_lval.word.text = strdup(yytext);
	archive_toc_lval.word.quoted = false;
	archive_toc_lval.word.end = archive_toc_token_end;

	return isLast ? LASTWORD : WORD;
}


/*
 * archive_toc_ident_append appends to archive_toc_word, this lexer's own
 * internal accumulator for a double-quoted identifier being scanned
 * across possibly several TOCQIDENT matches (plain runs of non-quote
 * characters, and one single escaped quote per "" match) -- the same
 * role litbuf/addlit play in scan.l's own <xd> state. Never read by any
 * grammar action directly, see the file header -- only copied into
 * archive_toc_lval, once, by the rule that closes the quote.
 */
static void
archive_toc_ident_append(const char *s, int len)
{
	size_t curLen = strlen(archive_toc_word);

	if (curLen + len >= sizeof(archive_toc_word))
	{
		len = sizeof(archive_toc_word) - curLen - 1;
	}

	if (len > 0)
	{
		memcpy(archive_toc_word + curLen, s, len);
		archive_toc_word[curLen + len] = '\0';
	}
}


/*
 * archive_toc_lex_line scans exactly one line (no trailing newline
 * expected in `line` itself) ready for the caller (archive_toc_parser.c)
 * to parse with archive_toc_parse() (archive_toc_grammar.y's generated
 * yyparse(), which pulls tokens by calling archive_toc_lex() itself).
 * Frees the previous scan's buffer first, if any -- yy_scan_string
 * allocates a fresh one every call, and a TOC can have many thousands of
 * lines. BEGIN(INITIAL) and resetting archive_toc_nested_mode reset
 * state a previous, possibly malformed scan could have left dangling.
 *
 * The '\n' appended before scanning is not part of the real line (which
 * never has one by the time it reaches here, see archive_iter_toc) --
 * it's there purely so TOCBODY/TOCQBODY's $-anchored rules (see the
 * file header) have a real end-of-line to anchor against. There is no
 * explicit <<EOF>> rule: flex's own default (returning 0) already is
 * exactly what bison's generated archive_toc_parse() expects to see at
 * true end of input.
 */
static YY_BUFFER_STATE archive_toc_buffer;

void
archive_toc_lex_line(const char *line)
{
	if (archive_toc_buffer != NULL)
	{
		archive_toc__delete_buffer(archive_toc_buffer);
		archive_toc_buffer = NULL;
	}

	size_t bytes = strlen(line) + 2;
	char *withNewline = (char *) malloc(bytes);

	if (withNewline == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return;
	}

	sformat(withNewline, bytes, "%s\n", line);

	archive_toc_nested_mode = false;
	archive_toc_buffer = archive_toc__scan_string(withNewline);
	BEGIN(INITIAL);

	free(withNewline);
}
