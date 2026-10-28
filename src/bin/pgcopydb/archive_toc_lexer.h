/*
 * src/bin/pgcopydb/archive_toc_lexer.h
 *     Minimal glue between the flex-generated archive_toc_lexer.l and
 *     the bison-generated archive_toc_grammar.y: the one lexer entry
 *     point archive_toc_parser.c calls, and the mid-rule action the
 *     grammar calls back into. Every token's own semantic value (a tag
 *     keyword's ArchiveItemDesc, a WORD's text, an OID's number) lives
 *     in archive_toc_lval, declared by the bison-generated
 *     archive_toc_grammar.h -- not here, and not in any plain global
 *     either (see archive_toc_lexer.l's own file header for why).
 */

#ifndef ARCHIVE_TOC_LEXER_H
#define ARCHIVE_TOC_LEXER_H

/*
 * archive_toc_lex_line feeds exactly one line (no trailing newline
 * required) to the lexer for archive_toc_parse() (archive_toc_grammar.y's
 * generated yyparse(), which pulls tokens by calling archive_toc_lex()
 * itself, declared there via %define api.prefix) to parse into an
 * ArchiveContentItem.
 *
 * archive_toc_begin_nested_tag is a mid-rule action the grammar calls
 * right after shifting an ACL/COMMENT entry's own outer namespace field,
 * switching the lexer back to recognizing a TAG keyword -- this time as
 * the object type nested inside the remainder, not the line's own outer
 * tag -- see archive_toc_lexer.l's own file header for the full
 * rationale.
 */
void archive_toc_lex_line(const char *line);
void archive_toc_begin_nested_tag(void);

/*
 * The end of whichever token archive_toc_lex() just matched -- see
 * archive_toc_lexer.l's own file header on YY_USER_ACTION, and archive_
 * toc_grammar.y's `mark` nonterminal, the only thing that reads this
 * (and only from specific, fixed mid-rule points, never from an
 * ordinary end-of-rule action).
 */
extern const char *archive_toc_token_end;

#endif /* ARCHIVE_TOC_LEXER_H */
