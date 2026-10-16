/*
 * src/bin/pgcopydb/archive_toc_lexer.h
 *     Token types returned by the flex-generated archive_toc_lexer.l,
 *     and the out-of-band values that go with ARCHIVE_TOC_TOKEN_OID and
 *     ARCHIVE_TOC_TOKEN_DESC (flex's yylex() only returns a plain int).
 */

#ifndef ARCHIVE_TOC_LEXER_H
#define ARCHIVE_TOC_LEXER_H

#include <stdint.h>

#include "pgcmd.h"

typedef enum
{
	ARCHIVE_TOC_TOKEN_EOF = 0,
	ARCHIVE_TOC_TOKEN_UNKNOWN,
	ARCHIVE_TOC_TOKEN_OID,
	ARCHIVE_TOC_TOKEN_SEMICOLON,
	ARCHIVE_TOC_TOKEN_SPACE,
	ARCHIVE_TOC_TOKEN_DASH,
	ARCHIVE_TOC_TOKEN_DESC,
	ARCHIVE_TOC_TOKEN_REMAINDER
} ArchiveTocTokenType;

/* set by the ARCHIVE_TOC_TOKEN_OID rule's action */
extern uint64_t archive_toc_last_oid;

/*
 * The text of the token just returned by archive_toc_lex() (flex's own
 * yytext, renamed by %option prefix). Valid only until the next
 * archive_toc_lex() or archive_toc_lex_line() call -- callers that need
 * to keep it (as archive_toc_parser.c does, for ARCHIVE_TOC_TOKEN_REMAINDER)
 * must copy it out before pulling the next token.
 */
extern char *archive_toc_text;

/* set by archive_toc_tag_token() for ARCHIVE_TOC_TOKEN_DESC */
extern ArchiveItemDesc archive_toc_last_desc;

/* the literal tag text that was matched, e.g. "FOREIGN DATA WRAPPER" */
extern char archive_toc_last_desc_text[BUFSIZE];

/*
 * archive_toc_lex_line feeds exactly one line (no trailing newline
 * required) to the lexer, resetting its start condition first so every
 * call begins in INITIAL regardless of how the previous line ended. Only
 * this function and archive_toc_lex() (the real flex-generated token-pull
 * function, declared here since callers pull tokens from outside the
 * .l file) are part of this module's public interface; the underlying
 * flex buffer-handling functions are used only inside archive_toc_lexer.l
 * itself, where their real prototypes are already in scope.
 */
void archive_toc_lex_line(const char *line);

/* flex-generated */
int archive_toc_lex(void);

#endif /* ARCHIVE_TOC_LEXER_H */
