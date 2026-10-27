%{
/*
 * src/bin/pgcopydb/archive_toc_lexer.l
 *     flex lexer for the fixed-shape prefix of a pg_restore --list line:
 *
 *       dumpId ';' catalogOid objectOid TAG remainder
 *
 * ahprintf(AH, "%d; %u %u %s %s %s %s\n", te->dumpId,
 *          te->catalogId.tableoid, te->catalogId.oid,
 *          te->desc, sanitized_schema, sanitized_name, sanitized_owner);
 * (PrintTOCSummary, pg_backup_archiver.c)
 *
 * This prefix -- up through and including the TAG keyword -- is the one
 * part of the line with a genuinely regular, context-free structure, so a
 * generated DFA is a clean fit (and, unlike the hand-maintained linear
 * strncmp scan it replaces, correctly prefers the longest match without
 * depending on the table being listed in a careful "longer tag before its
 * shorter prefix" order -- see e.g. "TABLE ATTACH"/"TABLE DATA" vs
 * "TABLE", or "SEQUENCE OWNED BY"/"SEQUENCE SET" vs "SEQUENCE").
 *
 * The remainder after the TAG is mostly NOT tokenized here: its shape
 * depends on which tag it follows (see ArchiveTocShape, pgcmd.h), which is
 * exactly the part of the format that is genuinely irregular from one tag
 * to the next, not a parsing problem a grammar disciplines away. Once a
 * TAG is recognized, the lexer switches to the REMAINDER start condition,
 * which matches the rest of the line verbatim in one token for
 * archive_toc_parser.c's shape-table-driven parser to split.
 *
 * One exception: when this lexer is re-entered to recognize the object
 * type inside an ACL or COMMENT entry's remainder (see
 * archive_toc_lex_object_type_line), and that object type is one that owns
 * an identifier of its own -- a relation, column, constraint, trigger,
 * policy or rule name -- that name can be double-quoted by pg_dump's
 * fmtId(), which genuinely is lexical work (unescaping a quoted token),
 * not a shape-splitting one. So the object type's own remainder switches
 * to the OBJTYPEBODY start condition instead, which tokenizes it properly:
 * IDENT (plain or double-quoted, mirroring scan.l's <xd> state for quoted
 * identifiers), DOT, and the two literal markers the "name ON relname"
 * shape and its "ON DOMAIN" exclusion need, ON and DOMAIN. The owner field
 * that can follow is, like everywhere else in this format, never escaped
 * or quoted, so it's still grabbed as one raw REMAINDER token (via
 * archive_toc_begin_remainder) rather than tokenized.
 *
 * A second exception within the first: FUNCTION, PROCEDURE, AGGREGATE and
 * OPERATOR also own an identifier (OBJTYPEBODY, same as above), but it's
 * immediately followed by its own balanced-parentheses argument list (an
 * operator's operand types, or a function's own argument list -- see
 * archive_toc_parser.c's naming note on why we go no further than that
 * raw text). Recognizing where that group ends genuinely is lexical work
 * too, exactly the kind flex is built for: a nested pair from a type's own
 * typmod ("numeric(10,2)") isn't the end of the group, so OBJTYPEBODY's
 * own "(" rule switches to the OBJTYPEARGS start condition, which counts
 * paren depth across possibly several matches and returns the whole group
 * (parentheses excluded) as one ARCHIVE_TOC_TOKEN_ARGS token once depth
 * returns to zero.
 */

#include <stdlib.h>
#include <string.h>

#include "parson.h"

#include "pgcmd.h"
#include "string_utils.h"
#include "archive_toc_lexer.h"

static int archive_toc_tag_token(ArchiveItemDesc desc);
static int archive_toc_object_type_word_token(void);
static bool archive_toc_object_type_has_ident(ArchiveItemDesc desc);
static void archive_toc_ident_append(const char *s, int len);
static void archive_toc_args_append(const char *s, int len);

/* used directly in the OBJTYPEBODY/OBJTYPEARGS rule actions below, so,
 * unlike archive_toc_object_type_mode, it must be declared up here in
 * the prologue rather than down in the trailing user-code section */
static int archive_toc_args_depth = 0;

%}

%option prefix="archive_toc_"
%option noyywrap
%option nounput
%option noinput
%option never-interactive

%x REMAINDER
%x OBJTYPEBODY
%x OBJTYPEQIDENT
%x OBJTYPEARGS

%%

<INITIAL>{

[0-9]+                            { archive_toc_last_oid = strtoull(yytext, NULL, 10);
                                     return ARCHIVE_TOC_TOKEN_OID; }
";"                                { return ARCHIVE_TOC_TOKEN_SEMICOLON; }
" "+                               { return ARCHIVE_TOC_TOKEN_SPACE; }
"-"                                { return ARCHIVE_TOC_TOKEN_DASH; }

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

.                                  { return ARCHIVE_TOC_TOKEN_UNKNOWN; }

}

<REMAINDER>{

.*                                 { BEGIN(INITIAL); return ARCHIVE_TOC_TOKEN_REMAINDER; }

}

<OBJTYPEBODY>{

" "+                               { return ARCHIVE_TOC_TOKEN_SPACE; }
"."                                { return ARCHIVE_TOC_TOKEN_DOT; }

\"                                 { BEGIN(OBJTYPEQIDENT);
                                     archive_toc_ident[0] = '\0';
                                     archive_toc_ident_quoted = true; }

"("                                { BEGIN(OBJTYPEARGS);
                                     archive_toc_args_depth = 1;
                                     archive_toc_args[0] = '\0'; }

[^\" .(]+                          { return archive_toc_object_type_word_token(); }

}

<OBJTYPEQIDENT>{

\"\"                               { archive_toc_ident_append("\"", 1); }
\"                                 { BEGIN(OBJTYPEBODY);
                                     return ARCHIVE_TOC_TOKEN_IDENT; }
[^\"]+                             { archive_toc_ident_append(yytext, yyleng); }

}

<OBJTYPEARGS>{

"("                                { archive_toc_args_depth++;
                                     archive_toc_args_append(yytext, yyleng); }

")"                                { archive_toc_args_depth--;
                                     if (archive_toc_args_depth == 0)
                                     {
                                         BEGIN(OBJTYPEBODY);
                                         return ARCHIVE_TOC_TOKEN_ARGS;
                                     }
                                     archive_toc_args_append(yytext, yyleng); }

[^()]+                             { archive_toc_args_append(yytext, yyleng); }

}

<<EOF>>                            { return ARCHIVE_TOC_TOKEN_EOF; }

%%

/*
 * archive_toc_tag_token switches the lexer to the REMAINDER start
 * condition (the text after a TAG is not otherwise tokenized) -- except
 * when scanning an object type (see archive_toc_lex_object_type_line)
 * that owns an identifier of its own, which switches to OBJTYPEBODY
 * instead, so that identifier can be unescaped properly. Either way,
 * returns a single ARCHIVE_TOC_TOKEN_DESC token; the specific
 * ArchiveItemDesc is left in archive_toc_last_desc for the caller to read
 * (flex's generated yylex() returns only an int, so this is the same
 * out-of-band-value idiom the grammar needs for the OID token's parsed
 * value too -- see archive_toc_last_oid in archive_toc_lexer.h).
 */
ArchiveItemDesc archive_toc_last_desc;
uint64_t archive_toc_last_oid;
char archive_toc_last_desc_text[BUFSIZE];
char archive_toc_ident[PG_NAMEDATALEN];
bool archive_toc_ident_quoted;
char archive_toc_args[BUFSIZE];

static bool archive_toc_object_type_mode = false;

static bool
archive_toc_object_type_has_ident(ArchiveItemDesc desc)
{
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
			return true;
		}

		default:
		{
			return false;
		}
	}
}


static int
archive_toc_tag_token(ArchiveItemDesc desc)
{
	archive_toc_last_desc = desc;
	strlcpy(archive_toc_last_desc_text, yytext,
			sizeof(archive_toc_last_desc_text));

	if (archive_toc_object_type_mode && archive_toc_object_type_has_ident(desc))
	{
		BEGIN(OBJTYPEBODY);
	}
	else
	{
		BEGIN(REMAINDER);
	}

	return ARCHIVE_TOC_TOKEN_DESC;
}


/*
 * archive_toc_object_type_word_token classifies a plain (unquoted) word
 * matched in OBJTYPEBODY: either one of the two literal markers the
 * "name ON relname" shape and its "ON DOMAIN" exclusion need (ON, DOMAIN),
 * or a plain identifier. Mirrors scan.l's own approach of matching a
 * generic word first and classifying it against a keyword set afterward,
 * rather than relying on flex's longest-match/rule-order tie-breaking
 * between a literal keyword rule and a generic identifier rule -- the
 * exact kind of declaration-order hazard this lexer exists to get rid of.
 */
static int
archive_toc_object_type_word_token(void)
{
	if (streq(yytext, "ON"))
	{
		return ARCHIVE_TOC_TOKEN_ON;
	}

	if (streq(yytext, "DOMAIN"))
	{
		return ARCHIVE_TOC_TOKEN_DOMAIN;
	}

	strlcpy(archive_toc_ident, yytext, sizeof(archive_toc_ident));
	archive_toc_ident_quoted = false;

	return ARCHIVE_TOC_TOKEN_IDENT;
}


/*
 * archive_toc_ident_append appends to archive_toc_ident, the accumulator
 * for a double-quoted identifier being scanned across possibly several
 * OBJTYPEQIDENT matches (plain runs of non-quote characters, and one
 * single escaped quote per "" match) -- the same role litbuf/addlit play
 * in scan.l's own <xd> state.
 */
static void
archive_toc_ident_append(const char *s, int len)
{
	size_t curLen = strlen(archive_toc_ident);

	if (curLen + len >= sizeof(archive_toc_ident))
	{
		len = sizeof(archive_toc_ident) - curLen - 1;
	}

	if (len > 0)
	{
		memcpy(archive_toc_ident + curLen, s, len);
		archive_toc_ident[curLen + len] = '\0';
	}
}


/*
 * archive_toc_args_append appends to archive_toc_args, the accumulator for
 * a FUNCTION/PROCEDURE/AGGREGATE/OPERATOR's own balanced-parentheses
 * argument list being scanned across possibly several OBJTYPEARGS matches
 * -- mirrors archive_toc_ident_append's role for a quoted identifier, one
 * level up (bytes making up the args text as a whole, not a single quoted
 * token within it; OBJTYPEARGS does not itself unescape quoting inside an
 * argument's own type name, if any -- see the file header).
 */
static void
archive_toc_args_append(const char *s, int len)
{
	size_t curLen = strlen(archive_toc_args);

	if (curLen + len >= sizeof(archive_toc_args))
	{
		len = sizeof(archive_toc_args) - curLen - 1;
	}

	if (len > 0)
	{
		memcpy(archive_toc_args + curLen, s, len);
		archive_toc_args[curLen + len] = '\0';
	}
}


/*
 * archive_toc_begin_remainder switches the lexer to the REMAINDER start
 * condition without matching a TAG first, so a caller that has just
 * finished parsing some structured prefix of the current scan buffer
 * (the object type's own identifier(s), see parseAclOrCommentRemainder in
 * archive_toc_parser.c) can grab everything left as one raw token. This
 * is how the owner field is captured: like everywhere else in this
 * format, it is never itself escaped or quoted, so there is nothing for
 * OBJTYPEBODY's identifier-aware tokenizing to do with it.
 */
void
archive_toc_begin_remainder(void)
{
	BEGIN(REMAINDER);
}


/*
 * archive_toc_lex_line and archive_toc_lex_object_type_line scan exactly
 * one line (no trailing newline expected or required) or, for the latter,
 * one object-type substring, ready for the caller to pull tokens from
 * with repeated archive_toc_lex() calls. Both free the previous scan's
 * buffer, if any -- yy_scan_string allocates a fresh one every call, and
 * a TOC can have many thousands of lines. BEGIN(INITIAL) resets the start
 * condition so a previous scan's dangling REMAINDER/OBJTYPEBODY/
 * OBJTYPEQIDENT state (there shouldn't be one, since a well-formed scan
 * always ends on its own REMAINDER token, but a malformed one could in
 * principle stop early) can never leak into the next call.
 *
 * The two differ only in archive_toc_object_type_mode, which
 * archive_toc_tag_token reads to decide whether a matched TAG owning its
 * own identifier should switch to OBJTYPEBODY (tokenized) instead of
 * REMAINDER (raw) -- see there.
 */
static YY_BUFFER_STATE archive_toc_buffer;

static void
archive_toc_lex_start(const char *line, bool objectTypeMode)
{
	if (archive_toc_buffer != NULL)
	{
		archive_toc__delete_buffer(archive_toc_buffer);
	}

	archive_toc_object_type_mode = objectTypeMode;
	archive_toc_buffer = archive_toc__scan_string(line);
	BEGIN(INITIAL);
}


void
archive_toc_lex_line(const char *line)
{
	archive_toc_lex_start(line, false);
}


void
archive_toc_lex_object_type_line(const char *line)
{
	archive_toc_lex_start(line, true);
}
