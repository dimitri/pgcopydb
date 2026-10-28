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
 * Earlier revisions of this file tokenized only the fixed dumpId/catalogOid/
 * objectOid/TAG prefix and handed the rest of the line to
 * archive_toc_parser.c as one or more opaque REMAINDER blobs, for hand-
 * written C functions to split with strchr()/strstr(). This revision
 * tokenizes the WHOLE line instead, one continuous token stream feeding
 * archive_toc_grammar.y, which assembles the ArchiveContentItem directly
 * from the tokens -- no opaque blob is ever handed back to C code to
 * re-split.
 *
 * Three things make that possible:
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
 * 2. Everything after a tag keyword -- a relation/function/type name, an
 *    argument list, a CAST's two type names, and so on -- is tokenized
 *    uniformly by the TOCBODY start condition below into WORD (one
 *    space-delimited run, or one double-quoted, fmtId()-unescaped
 *    identifier), a handful of literal keywords this format's own
 *    further-structured shapes use as fixed separators (AS, FOR,
 *    LANGUAGE, SERVER, USING, ON, DOMAIN), and the structural punctuation
 *    (parentheses, comma, dot) a function's own balanced argument list or
 *    a qualified name needs. archive_toc_grammar.y's own productions --
 *    not any string-splitting here or in archive_toc_parser.c -- decide
 *    how many WORDs make up one field (a multi-word, never-quoted name
 *    like FUNCTION's own tag text can run to several) and recognize a
 *    nested parenthesized group (a type's own typmod) by ordinary
 *    grammar recursion, not a hand-counted depth variable.
 *
 * 3. The one genuinely lexical fact this format encodes positionally
 *    rather than with its own keyword -- the owner field, which is simply
 *    the LAST space-delimited token on the line (or absent, marked by a
 *    lone trailing space with nothing after it) -- is resolved by flex's
 *    own end-of-buffer anchor ($): TOCBODY's word rule and its "trailing
 *    space" rule each have a $-anchored twin (LASTWORD, NO_OWNER) that
 *    only matches at the true end of the scanned buffer, which
 *    archive_toc_lex_line (below) arranges for by appending a '\n' before
 *    scanning -- flex's $ needs a real line ending to anchor against,
 *    verified empirically: yy_scan_string on a string with no trailing
 *    '\n' never triggers a $-anchored rule, no matter how the buffer
 *    actually ends. This makes the owner boundary a property the LEXER
 *    discovers as it goes, not something any caller pre-scans for.
 *
 * Every token that carries a value (TOK_OID's number, a tag keyword's
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
 * archive_toc_token_start/archive_toc_token_end bracket the text of
 * whichever token archive_toc_lex() just matched, as raw pointers into
 * flex's own stable internal scan buffer (the one yy_scan_string
 * allocates and keeps for as long as the current line's scan lasts,
 * not the caller's own copy, see archive_toc_lex_line below). YY_USER_
 * ACTION runs this before EVERY rule's own action, in every start
 * condition, with no per-rule code needed.
 *
 * archive_toc_grammar.y's own `mark` nonterminal (a zero-width mid-rule
 * placeholder) reads archive_toc_token_end at specific, fixed points in
 * a production -- right after the last token of a span it wants to
 * capture verbatim -- to slice that exact source text out, byte for
 * byte, rather than reassembling it from already-tokenized pieces (see
 * archive_toc_grammar.y's own file header on why: an already-split
 * piece has lost the original formatting -- exact spacing, a nested
 * typmod's own parentheses, ... -- a hand-rebuilt sformat() has to
 * guess back). Safe to read only from a mid-rule action positioned
 * immediately after the token it means to mark -- see `mark`'s own
 * comment for why reading it from an ordinary (end-of-rule) action
 * would not be.
 */
const char *archive_toc_token_start;
const char *archive_toc_token_end;

#define YY_USER_ACTION \
	archive_toc_token_start = yytext; \
	archive_toc_token_end = yytext + yyleng;

%}

%option prefix="archive_toc_"
%option noyywrap
%option nounput
%option noinput
%option never-interactive

%x NAMESPACE
%x TOCBODY
%x TOCQIDENT

%%

<INITIAL>{

[0-9]+                            { archive_toc_lval.oid = strtoull(yytext, NULL, 10);
                                     return TOK_OID; }
";"                                { return TOK_SEMICOLON; }
" "+                               { return TOK_SPACE; }

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

.                                  { return TOK_UNKNOWN; }

}

<NAMESPACE>{

"-"                                { BEGIN(TOCBODY); return TOK_DASH; }
[^ \n]+                            { BEGIN(TOCBODY);
                                     archive_toc_lval.word.text = strdup(yytext);
                                     archive_toc_lval.word.quoted = false;
                                     archive_toc_lval.word.end = archive_toc_token_end;
                                     return TOK_WORD; }

}

<TOCBODY>{

" "+$                              { return TOK_NO_OWNER; }
" "+                               { return TOK_SPACE; }

\"                                 { BEGIN(TOCQIDENT);
                                     archive_toc_word[0] = '\0'; }

"("                                { return TOK_LPAREN; }
")"                                { return TOK_RPAREN; }
","                                { return TOK_COMMA; }
"."                                { return TOK_DOT; }

[^ ().,\"\n]+$                     { return archive_toc_word_or_keyword_token(true); }
[^ ().,\"\n]+                      { return archive_toc_word_or_keyword_token(false); }

}

<TOCQIDENT>{

\"\"                               { archive_toc_ident_append("\"", 1); }
\"                                 { BEGIN(TOCBODY);
                                     archive_toc_lval.word.text = strdup(archive_toc_word);
                                     archive_toc_lval.word.quoted = true;
                                     archive_toc_lval.word.end = archive_toc_token_end;
                                     return TOK_WORD; }
[^\"\n]+                           { archive_toc_ident_append(yytext, yyleng); }

}

<<EOF>>                            { return TOK_EOF; }

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
 * straight to TOCBODY instead.
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
				return TOK_ACL;
			}

			case ARCHIVE_TAG_COMMENT:
			{
				return TOK_COMMENT;
			}

			case ARCHIVE_TAG_FUNCTION:
			case ARCHIVE_TAG_PROCEDURE:
			case ARCHIVE_TAG_AGGREGATE:
			{
				return TOK_TOP_FUNCLIKE;
			}

			case ARCHIVE_TAG_OPERATOR:
			{
				return TOK_TOP_OPERATOR;
			}

			case ARCHIVE_TAG_CAST:
			{
				return TOK_TOP_CAST;
			}

			case ARCHIVE_TAG_TRANSFORM:
			{
				return TOK_TOP_TRANSFORM;
			}

			case ARCHIVE_TAG_USER_MAPPING:
			{
				return TOK_TOP_USER_MAPPING;
			}

			case ARCHIVE_TAG_DEFAULT_ACL:
			{
				return TOK_TOP_DEFAULT_ACL;
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
				return TOK_TOP_RELATION;
			}

			default:
			{
				return TOK_TOP_SIMPLE;
			}
		}
	}
	else
	{
		BEGIN(TOCBODY);

		switch (desc)
		{
			case ARCHIVE_TAG_TABLE:
			case ARCHIVE_TAG_VIEW:
			case ARCHIVE_TAG_MATERIALIZED_VIEW:
			case ARCHIVE_TAG_FOREIGN_TABLE:
			case ARCHIVE_TAG_INDEX:
			case ARCHIVE_TAG_SEQUENCE:
			{
				return TOK_NESTED_RELATION_LIKE;
			}

			case ARCHIVE_TAG_COLUMN:
			{
				return TOK_NESTED_COLUMN;
			}

			case ARCHIVE_TAG_EXTENSION:
			{
				return TOK_NESTED_EXTENSION;
			}

			case ARCHIVE_TAG_CONSTRAINT:
			case ARCHIVE_TAG_TRIGGER:
			case ARCHIVE_TAG_POLICY:
			case ARCHIVE_TAG_RULE:
			{
				return TOK_NESTED_ON_RELATION;
			}

			case ARCHIVE_TAG_FUNCTION:
			case ARCHIVE_TAG_PROCEDURE:
			case ARCHIVE_TAG_AGGREGATE:
			{
				return TOK_NESTED_FUNCLIKE;
			}

			case ARCHIVE_TAG_OPERATOR:
			{
				return TOK_NESTED_OPERATOR;
			}

			case ARCHIVE_TAG_CAST:
			{
				return TOK_NESTED_CAST;
			}

			case ARCHIVE_TAG_TRANSFORM:
			{
				return TOK_NESTED_TRANSFORM;
			}

			case ARCHIVE_TAG_OPERATOR_CLASS:
			case ARCHIVE_TAG_OPERATOR_FAMILY:
			{
				return TOK_NESTED_OPCLASS_FAMILY;
			}

			default:
			{
				return TOK_NESTED_GENERIC;
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
 * non-space, non-structural characters matched in TOCBODY: either one of
 * the handful of literal keywords CAST/TRANSFORM/USER MAPPING/DEFAULT
 * ACL/an OPERATOR CLASS or FAMILY's ACL/COMMENT form/a relation-owned
 * object type's "ON relname" use as their own fixed separator (AS, FOR,
 * LANGUAGE, SERVER, USING, ON, DOMAIN), or a generic word -- mirrors the
 * previous revision's archive_toc_object_type_word_token, same rationale
 * (classify first, same risk profile as scan.l's own generic-word-then-
 * classify approach: a real identifier spelled exactly "AS" or "SERVER"
 * would be misread, but every one of these is a reserved SQL keyword, so
 * an unquoted identifier can never actually be exactly one of them; a
 * literal name that needed quoting for this reason would come through
 * TOCQIDENT instead, never reaching this rule at all).
 *
 * isLast (true only for the $-anchored twin of this rule, see the file
 * header) distinguishes TOK_LASTWORD from plain TOK_WORD for the generic
 * case -- the grammar's own owner-boundary rule, not anything this
 * function decides.
 */
static int
archive_toc_word_or_keyword_token(bool isLast)
{
	static const struct
	{
		const char *word;
		int token;
	} keywords[] = {
		{ "AS", TOK_AS },
		{ "FOR", TOK_FOR },
		{ "LANGUAGE", TOK_LANGUAGE_KW },
		{ "SERVER", TOK_SERVER_KW },
		{ "USING", TOK_USING },
		{ "ON", TOK_ON },
		{ "DOMAIN", TOK_DOMAIN }
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

	return isLast ? TOK_LASTWORD : TOK_WORD;
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
 * it's there purely so TOCBODY's $-anchored rules (see the file header)
 * have a real end-of-line to anchor against.
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
