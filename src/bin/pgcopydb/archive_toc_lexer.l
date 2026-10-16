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
 * The remainder after the TAG is NOT tokenized here: its shape depends on
 * which tag it follows (see ArchiveTocShape, pgcmd.h), which is exactly
 * the part of the format that is genuinely irregular from one tag to the
 * next, not a parsing problem a grammar disciplines away. Once a TAG is
 * recognized, the lexer switches to the REMAINDER start condition, which
 * matches the rest of the line verbatim in one token for
 * archive_toc_parser.c's shape-table-driven parser to split.
 */

#include <stdlib.h>

#include "parson.h"

#include "pgcmd.h"
#include "archive_toc_lexer.h"

static int archive_toc_tag_token(ArchiveItemDesc desc);

%}

%option prefix="archive_toc_"
%option noyywrap
%option nounput
%option noinput
%option never-interactive

%x REMAINDER

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

<<EOF>>                            { return ARCHIVE_TOC_TOKEN_EOF; }

%%

/*
 * archive_toc_tag_token switches the lexer to the REMAINDER start
 * condition (the text after a TAG is never itself re-tokenized by this
 * lexer) and returns a single ARCHIVE_TOC_TOKEN_DESC token; the specific
 * ArchiveItemDesc is left in archive_toc_last_desc for the caller to read
 * (flex's generated yylex() returns only an int, so this is the same
 * out-of-band-value idiom the grammar needs for the OID token's parsed
 * value too -- see archive_toc_last_oid in archive_toc_lexer.h).
 */
ArchiveItemDesc archive_toc_last_desc;
uint64_t archive_toc_last_oid;
char archive_toc_last_desc_text[BUFSIZE];

static int
archive_toc_tag_token(ArchiveItemDesc desc)
{
	archive_toc_last_desc = desc;
	strlcpy(archive_toc_last_desc_text, yytext,
			sizeof(archive_toc_last_desc_text));
	BEGIN(REMAINDER);
	return ARCHIVE_TOC_TOKEN_DESC;
}


/*
 * archive_toc_lex_line scans exactly one line (no trailing newline
 * expected or required), ready for the caller to pull tokens from with
 * repeated archive_toc_lex() calls. Frees the previous line's scan
 * buffer, if any -- yy_scan_string allocates a fresh one every call, and
 * a TOC can have many thousands of lines. BEGIN(INITIAL) resets the start
 * condition so a previous line's dangling REMAINDER state (there
 * shouldn't be one, since each line's last token is always its own
 * REMAINDER, but a malformed line could in principle stop early) can
 * never leak into the next call.
 */
static YY_BUFFER_STATE archive_toc_buffer;

void
archive_toc_lex_line(const char *line)
{
	if (archive_toc_buffer != NULL)
	{
		archive_toc__delete_buffer(archive_toc_buffer);
	}

	archive_toc_buffer = archive_toc__scan_string(line);
	BEGIN(INITIAL);
}
