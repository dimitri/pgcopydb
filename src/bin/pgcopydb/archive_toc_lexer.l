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
 * 2. Everything after a tag keyword is tokenized uniformly by TOCBODY
 *    (or its quote-aware twin TOCQBODY, see below) into WORD, SPACE,
 *    the handful of separator keywords, and structural punctuation.
 *    archive_toc_grammar.y's own productions -- not any string-splitting
 *    here or in archive_toc_parser.c -- decide how many WORDs (and
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

%x NAMESPACE
%x TOCBODY
%x TOCQBODY
%x TOCQIDENT

%%

<INITIAL>{

[0-9]+                            { archive_toc_lval.oid = strtoull(yytext, NULL, 10);
                                     return OID; }
";"                                { return ';'; }
" "+                               ;

"ACCESS METHOD"                   { return archive_toc_tag_token(ARCHIVE_TAG_ACCESS_METHOD); }
"ACL"                             { return archive_toc_tag_token(ARCHIVE_TAG_ACL); }
"AGGREGATE"                       { return archive_toc_tag_token(ARCHIVE_TAG_AGGREGATE); }
"ATTRDEF"                         { return archive_toc_tag_token(ARCHIVE_TAG_ATTRDEF); }
"BLOB DATA"                       { return archive_toc_tag_token(ARCHIVE_TAG_BLOB_DATA); }
"BLOB"                            { return archive_toc_tag_token(ARCHIVE_TAG_BLOB); }
"CAST"                            { return archive_toc_tag_token(ARCHIVE_TAG_CAST); }
"CHECK CONSTRAINT"                { return archive_toc_tag_token(ARCHIVE_TAG_CHECK_CONSTRAINT); }
"COLLATION"                       { return archive_toc_tag_token(ARCHIVE_TAG_COLLATION); }
"COLUMN"                          { return archive_toc_tag_token(ARCHIVE_TAG_COLUMN); }
"COMMENT"                         { return archive_toc_tag_token(ARCHIVE_TAG_COMMENT); }
"CONSTRAINT"                      { return archive_toc_tag_token(ARCHIVE_TAG_CONSTRAINT); }
"CONVERSION"                      { return archive_toc_tag_token(ARCHIVE_TAG_CONVERSION); }
"DATABASE"                        { return archive_toc_tag_token(ARCHIVE_TAG_DATABASE); }
"DEFAULT ACL"                     { return archive_toc_tag_token(ARCHIVE_TAG_DEFAULT_ACL); }
"DEFAULT"                         { return archive_toc_tag_token(ARCHIVE_TAG_DEFAULT); }
"DOMAIN"                          { return archive_toc_tag_token(ARCHIVE_TAG_DOMAIN); }
"DUMMY TYPE"                      { return archive_toc_tag_token(ARCHIVE_TAG_DUMMY_TYPE); }
"EVENT TRIGGER"                   { return archive_toc_tag_token(ARCHIVE_TAG_EVENT_TRIGGER); }
"EXTENSION"                       { return archive_toc_tag_token(ARCHIVE_TAG_EXTENSION); }
"FK CONSTRAINT"                   { return archive_toc_tag_token(ARCHIVE_TAG_FK_CONSTRAINT); }
"FOREIGN DATA WRAPPER"            { return archive_toc_tag_token(ARCHIVE_TAG_FOREIGN_DATA_WRAPPER); }
"FOREIGN SERVER"                  { return archive_toc_tag_token(ARCHIVE_TAG_FOREIGN_SERVER); }
"FOREIGN TABLE"                   { return archive_toc_tag_token(ARCHIVE_TAG_FOREIGN_TABLE); }
"FUNCTION"                        { return archive_toc_tag_token(ARCHIVE_TAG_FUNCTION); }
"INDEX ATTACH"                    { return archive_toc_tag_token(ARCHIVE_TAG_INDEX_ATTACH); }
"INDEX"                           { return archive_toc_tag_token(ARCHIVE_TAG_INDEX); }
"LANGUAGE"                        { return archive_toc_tag_token(ARCHIVE_TAG_LANGUAGE); }
"LARGE OBJECT"                    { return archive_toc_tag_token(ARCHIVE_TAG_LARGE_OBJECT); }
"MATERIALIZED VIEW DATA"          { return archive_toc_tag_token(ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW); }
"MATERIALIZED VIEW"               { return archive_toc_tag_token(ARCHIVE_TAG_MATERIALIZED_VIEW); }
"OPERATOR CLASS"                  { return archive_toc_tag_token(ARCHIVE_TAG_OPERATOR_CLASS); }
"OPERATOR FAMILY"                 { return archive_toc_tag_token(ARCHIVE_TAG_OPERATOR_FAMILY); }
"OPERATOR"                        { return archive_toc_tag_token(ARCHIVE_TAG_OPERATOR); }
"POLICY"                          { return archive_toc_tag_token(ARCHIVE_TAG_POLICY); }
"PROCEDURAL LANGUAGE"             { return archive_toc_tag_token(ARCHIVE_TAG_PROCEDURAL_LANGUAGE); }
"PROCEDURE"                       { return archive_toc_tag_token(ARCHIVE_TAG_PROCEDURE); }
"PUBLICATION TABLES IN SCHEMA"    { return archive_toc_tag_token(ARCHIVE_TAG_PUBLICATION_TABLES_IN_SCHEMA); }
"PUBLICATION TABLE"               { return archive_toc_tag_token(ARCHIVE_TAG_PUBLICATION_TABLE); }
"PUBLICATION"                     { return archive_toc_tag_token(ARCHIVE_TAG_PUBLICATION); }
"ROW SECURITY"                    { return archive_toc_tag_token(ARCHIVE_TAG_ROW_SECURITY); }
"RULE"                            { return archive_toc_tag_token(ARCHIVE_TAG_RULE); }
"SCHEMA"                          { return archive_toc_tag_token(ARCHIVE_TAG_SCHEMA); }
"SEQUENCE OWNED BY"               { return archive_toc_tag_token(ARCHIVE_TAG_SEQUENCE_OWNED_BY); }
"SEQUENCE SET"                    { return archive_toc_tag_token(ARCHIVE_TAG_SEQUENCE_SET); }
"SEQUENCE"                        { return archive_toc_tag_token(ARCHIVE_TAG_SEQUENCE); }
"SERVER"                          { return archive_toc_tag_token(ARCHIVE_TAG_SERVER); }
"SHELL TYPE"                      { return archive_toc_tag_token(ARCHIVE_TAG_SHELL_TYPE); }
"STATISTICS"                      { return archive_toc_tag_token(ARCHIVE_TAG_STATISTICS); }
"SUBSCRIPTION"                    { return archive_toc_tag_token(ARCHIVE_TAG_SUBSCRIPTION); }
"TABLE ATTACH"                    { return archive_toc_tag_token(ARCHIVE_TAG_TABLE_ATTACH); }
"TABLE DATA"                      { return archive_toc_tag_token(ARCHIVE_TAG_TABLE_DATA); }
"TABLE"                           { return archive_toc_tag_token(ARCHIVE_TAG_TABLE); }
"TEXT SEARCH CONFIGURATION"       { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_CONFIGURATION); }
"TEXT SEARCH DICTIONARY"          { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_DICTIONARY); }
"TEXT SEARCH PARSER"              { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_PARSER); }
"TEXT SEARCH TEMPLATE"            { return archive_toc_tag_token(ARCHIVE_TAG_TEXT_SEARCH_TEMPLATE); }
"TRANSFORM"                       { return archive_toc_tag_token(ARCHIVE_TAG_TRANSFORM); }
"TRIGGER"                         { return archive_toc_tag_token(ARCHIVE_TAG_TRIGGER); }
"TYPE"                            { return archive_toc_tag_token(ARCHIVE_TAG_TYPE); }
"USER MAPPING"                    { return archive_toc_tag_token(ARCHIVE_TAG_USER_MAPPING); }
"VIEW"                            { return archive_toc_tag_token(ARCHIVE_TAG_VIEW); }

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

<TOCBODY>{

" "+$                              { return NO_OWNER; }

  /*
   * Each of these six keywords is this format's own fixed separator for
   * exactly one further-structured shape (CAST's " AS ", TRANSFORM's own
   * "FOR ... LANGUAGE ...", USER MAPPING's " SERVER ", DEFAULT ACL's
   * "FOR ...", an OPERATOR CLASS/FAMILY ACL/COMMENT's " USING ", a
   * relation-owned ACL/COMMENT object's " ON " -- see pg_dump.c's own
   * dumpCast/dumpTransform/dumpUserMappings/dumpDefaultACL/dumpOpclass/
   * dumpOpfamily), always with exactly the single space pg_dump's own
   * fixed sprintf()-style formatting puts on each side. Matching ALL of
   * that (keyword and both surrounding runs of whitespace) as one token
   * means the grammar itself never has to spell out an explicit SPACE
   * around any of them (see archive_toc_grammar.y's own file header) --
   * "one or more spaces" on each side rather than hard-coding exactly
   * one still correctly handles a hand-edited file with irregular
   * spacing, at the cost of no longer being able to tell such a file
   * apart from a normal one. Flex's own longest-match rule means these
   * six patterns always win over the generic WORD rule below wherever
   * they apply, so archive_toc_word_or_keyword_token's own classifier
   * no longer needs (or has) entries for any of them -- DOMAIN is the
   * only keyword left that genuinely needs post-match recognition,
   * since it has no surrounding-whitespace convention of its own to
   * bake into a dedicated pattern.
   */
" "+"AS"" "+                       { return AS; }
" "+"FOR"" "+                      { archive_toc_lval.pos = yytext + strspn(yytext, " ");
                                     return FOR; }
" "+"LANGUAGE"" "+                 { return LANGUAGE_KW; }
" "+"SERVER"" "+                   { return SERVER_KW; }
" "+"USING"" "+                    { return USING; }
" "+"ON"" "+                       { return ON; }

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

<TOCQBODY>{

" "+$                              { return NO_OWNER; }

  /* NESTED_ON_RELATION's own " ON " separator -- see TOCBODY's own rule. */
" "+"ON"" "+                       { return ON; }

" "+                               { archive_toc_lval.pos = archive_toc_token_end;
                                     return SPACE; }

\"                                 { BEGIN(TOCQIDENT);
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

<TOCQIDENT>{

\"\"                               { archive_toc_ident_append("\"", 1); }
\"                                 { BEGIN(TOCQBODY);
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
	archive_toc_lval.tag.desc = desc;
	archive_toc_lval.tag.text = strdup(yytext);

	if (!archive_toc_nested_mode)
	{
		BEGIN(NAMESPACE);

		switch (desc)
		{
			case ARCHIVE_TAG_ACL:
			{
				return ACL;
			}

			case ARCHIVE_TAG_COMMENT:
			{
				return COMMENT;
			}

			case ARCHIVE_TAG_FUNCTION:
			case ARCHIVE_TAG_PROCEDURE:
			case ARCHIVE_TAG_AGGREGATE:
			{
				return TOP_FUNCLIKE;
			}

			case ARCHIVE_TAG_OPERATOR:
			{
				return TOP_OPERATOR;
			}

			case ARCHIVE_TAG_CAST:
			{
				return TOP_CAST;
			}

			case ARCHIVE_TAG_TRANSFORM:
			{
				return TOP_TRANSFORM;
			}

			case ARCHIVE_TAG_USER_MAPPING:
			{
				return TOP_USER_MAPPING;
			}

			case ARCHIVE_TAG_DEFAULT_ACL:
			{
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
				return TOP_RELATION;
			}

			default:
			{
				return TOP_SIMPLE;
			}
		}
	}
	else
	{
		/*
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
			case ARCHIVE_TAG_COLUMN:
			case ARCHIVE_TAG_CONSTRAINT:
			case ARCHIVE_TAG_TRIGGER:
			case ARCHIVE_TAG_POLICY:
			case ARCHIVE_TAG_RULE:
			case ARCHIVE_TAG_FUNCTION:
			case ARCHIVE_TAG_PROCEDURE:
			case ARCHIVE_TAG_AGGREGATE:
			case ARCHIVE_TAG_OPERATOR:
			{
				BEGIN(TOCQBODY);
				break;
			}

			default:
			{
				BEGIN(TOCBODY);
				break;
			}
		}

		switch (desc)
		{
			case ARCHIVE_TAG_TABLE:
			case ARCHIVE_TAG_VIEW:
			case ARCHIVE_TAG_MATERIALIZED_VIEW:
			case ARCHIVE_TAG_FOREIGN_TABLE:
			case ARCHIVE_TAG_INDEX:
			case ARCHIVE_TAG_SEQUENCE:
			{
				return NESTED_RELATION_LIKE;
			}

			case ARCHIVE_TAG_COLUMN:
			{
				return NESTED_COLUMN;
			}

			case ARCHIVE_TAG_EXTENSION:
			{
				return NESTED_EXTENSION;
			}

			case ARCHIVE_TAG_CONSTRAINT:
			case ARCHIVE_TAG_TRIGGER:
			case ARCHIVE_TAG_POLICY:
			case ARCHIVE_TAG_RULE:
			{
				return NESTED_ON_RELATION;
			}

			case ARCHIVE_TAG_FUNCTION:
			case ARCHIVE_TAG_PROCEDURE:
			case ARCHIVE_TAG_AGGREGATE:
			{
				return NESTED_FUNCLIKE;
			}

			case ARCHIVE_TAG_OPERATOR:
			{
				return NESTED_OPERATOR;
			}

			case ARCHIVE_TAG_CAST:
			{
				return NESTED_CAST;
			}

			case ARCHIVE_TAG_TRANSFORM:
			{
				return NESTED_TRANSFORM;
			}

			case ARCHIVE_TAG_OPERATOR_CLASS:
			case ARCHIVE_TAG_OPERATOR_FAMILY:
			{
				return NESTED_OPCLASS_FAMILY;
			}

			default:
			{
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
 * either one of the handful of literal keywords CAST/TRANSFORM/USER
 * MAPPING/DEFAULT ACL/an OPERATOR CLASS or FAMILY's ACL/COMMENT form/a
 * relation-owned object type's "ON relname" use as their own fixed
 * separator (AS, FOR, LANGUAGE, SERVER, USING, ON, DOMAIN), or a generic
 * word -- mirrors scan.l's own generic-word-then-classify approach: a
 * real identifier spelled exactly "AS" or "SERVER" would be misread,
 * but every one of these is a reserved SQL keyword, so an unquoted
 * identifier can never actually be exactly one of them; a literal name
 * that needed quoting for this reason would come through TOCQIDENT
 * instead, never reaching this rule at all.
 *
 * isLast (true only for the $-anchored twin of this rule, see the file
 * header) distinguishes LASTWORD from plain WORD for the generic case --
 * the grammar's own owner-boundary rule, not anything this function
 * decides.
 */
static int
archive_toc_word_or_keyword_token(bool isLast)
{
	static const struct
	{
		const char *word;
		int token;
	} keywords[] = {
		{ "DOMAIN", DOMAIN }
	};

	for (size_t i = 0; i < sizeof(keywords) / sizeof(keywords[0]); i++)
	{
		if (streq(yytext, keywords[i].word))
		{
			return keywords[i].token;
		}
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
