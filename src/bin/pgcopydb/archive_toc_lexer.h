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
	ARCHIVE_TOC_TOKEN_REMAINDER,

	/* OBJTYPEBODY only, see archive_toc_lex_object_type_line */
	ARCHIVE_TOC_TOKEN_DOT,
	ARCHIVE_TOC_TOKEN_ON,
	ARCHIVE_TOC_TOKEN_DOMAIN,
	ARCHIVE_TOC_TOKEN_IDENT,

	/*
	 * OBJTYPEARGS only: the whole text between a FUNCTION/PROCEDURE/
	 * AGGREGATE/OPERATOR's own balanced parentheses, matched as one
	 * token by counting nested parens (e.g. a type's own typmod,
	 * "numeric(10,2)") rather than stopping at the first ")".
	 */
	ARCHIVE_TOC_TOKEN_ARGS,

	/*
	 * CASTBODY/TRANSFORMBODY/USERMAPPINGBODY/DEFAULTACLBODY/OPCLASSBODY
	 * only, see archive_toc_lex_cast_body and its siblings below: the
	 * literal keywords each of those five shapes uses as its own fixed
	 * separator, and WORD, a generic one-word-at-a-time chunk of
	 * whatever text sits between them (a type name, a role name, a
	 * server name, ...) for the caller to accumulate across possibly
	 * several matches -- the same accumulate-until-the-next-keyword
	 * idiom OBJTYPEARGS already uses for a balanced-parens group, one
	 * level simpler since there is no nesting to track here, only a
	 * fixed keyword to stop at.
	 */
	ARCHIVE_TOC_TOKEN_AS,
	ARCHIVE_TOC_TOKEN_FOR,
	ARCHIVE_TOC_TOKEN_LANGUAGE_KW,
	ARCHIVE_TOC_TOKEN_SERVER_KW,
	ARCHIVE_TOC_TOKEN_USING,
	ARCHIVE_TOC_TOKEN_RPAREN,
	ARCHIVE_TOC_TOKEN_WORD
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
 * The unescaped value of the identifier just returned as
 * ARCHIVE_TOC_TOKEN_IDENT (OBJTYPEBODY only) -- set either directly, for a
 * plain identifier, or accumulated across possibly several OBJTYPEQIDENT
 * matches, for a double-quoted one. Same validity rule as archive_toc_text:
 * valid only until the next archive_toc_lex() call.
 */
extern char archive_toc_ident[PG_NAMEDATALEN];

/*
 * Whether the identifier just returned as ARCHIVE_TOC_TOKEN_IDENT was
 * double-quoted in the original line (pg_dump's own fmtId() decided it
 * needed to be) or matched as a plain word. archive_toc_ident itself is
 * always the unescaped value either way; a caller that needs to
 * reconstruct the original, faithfully-quoted spelling (see
 * useListRemainder in pgcmd.h) uses this to decide whether to re-quote
 * it. Same validity rule as archive_toc_ident: valid only until the next
 * archive_toc_lex() call.
 */
extern bool archive_toc_ident_quoted;

/*
 * The text just returned as ARCHIVE_TOC_TOKEN_ARGS (OBJTYPEARGS only):
 * everything between a FUNCTION/PROCEDURE/AGGREGATE/OPERATOR's own
 * balanced parentheses, exclusive, accumulated across possibly several
 * OBJTYPEARGS matches the same way archive_toc_ident is. Same validity
 * rule: valid only until the next archive_toc_lex() call. Sized BUFSIZE,
 * not PG_NAMEDATALEN: an argument list isn't a single identifier.
 */
extern char archive_toc_args[BUFSIZE];

/*
 * The text just returned as ARCHIVE_TOC_TOKEN_WORD (CASTBODY/TRANSFORMBODY/
 * USERMAPPINGBODY/DEFAULTACLBODY/OPCLASSBODY only): one space-delimited
 * word, not yet accumulated with any neighboring WORD the caller may also
 * need (see the entry points below). Sized BUFSIZE, not PG_NAMEDATALEN:
 * unlike archive_toc_ident (always a single Postgres identifier), this can
 * be one word of an arbitrary type-name expression.
 */
extern char archive_toc_word[BUFSIZE];

/*
 * archive_toc_lex_line feeds exactly one line (no trailing newline
 * required) to the lexer, resetting its start condition first so every
 * call begins in INITIAL regardless of how the previous line ended.
 *
 * archive_toc_lex_object_type_line is the same, for the substring starting
 * right at the object type's own keyword text (see
 * parseAclOrCommentRemainder, archive_toc_parser.c): it additionally arms
 * OBJTYPEBODY tokenizing for whichever object type owns an identifier of
 * its own (see archive_toc_object_type_has_ident in the .l file).
 *
 * archive_toc_begin_remainder switches to the REMAINDER start condition
 * without matching a TAG first, letting a caller that just finished
 * parsing the object type's own identifier(s) grab everything left (the
 * owner field, never itself quoted or escaped) as one raw token.
 *
 * These, and archive_toc_lex() (the real flex-generated token-pull
 * function, declared here since callers pull tokens from outside the .l
 * file), are this module's public interface; the underlying flex buffer-
 * handling functions are used only inside archive_toc_lexer.l itself,
 * where their real prototypes are already in scope.
 */
void archive_toc_lex_line(const char *line);
void archive_toc_lex_object_type_line(const char *line);
void archive_toc_begin_remainder(void);

/*
 * Phase 4: one entry point per further-structured shape, each arming the
 * matching start condition directly (no TAG keyword to match first --
 * the caller already knows which shape it's dealing with, see
 * archive_toc_parser.c's applyTopLevelSignatureExtras and
 * parseAclOrCommentRemainder). Text returned via ARCHIVE_TOC_TOKEN_WORD is
 * left in archive_toc_word, below -- a one-word-at-a-time chunk of
 * whatever text sits between two keywords (a type name, a role name, a
 * server name, ...), for the caller to accumulate across possibly several
 * WORD tokens in a row (a multi-word type name, e.g. "double precision")
 * until the next keyword token ends the run.
 *
 *   archive_toc_lex_cast_body:           "(" sourcetype " AS " targettype ")"
 *   archive_toc_lex_transform_body:      "FOR " type " LANGUAGE " language
 *   archive_toc_lex_user_mapping_body:   user " SERVER " server
 *   archive_toc_lex_default_acl_body:    "FOR " objecttype
 *   archive_toc_lex_opclass_body:        name " USING " amname
 *   archive_toc_lex_function_signature:  name "(" args ")" -- its own
 *     FUNCSIGBODY/OBJTYPEARGS pair, almost but not quite OBJTYPEBODY's
 *     own (see the .l file): this name is never double-quoted, even when
 *     it has an embedded space (format_function_signature's own
 *     honor_quotes=false in pg_dump.c), so unlike OBJTYPEBODY's own
 *     identifier rule, FUNCSIGBODY's doesn't stop at a space.
 */
void archive_toc_lex_cast_body(const char *text);
void archive_toc_lex_transform_body(const char *text);
void archive_toc_lex_user_mapping_body(const char *text);
void archive_toc_lex_default_acl_body(const char *text);
void archive_toc_lex_opclass_body(const char *text);
void archive_toc_lex_function_signature(const char *text);

/* flex-generated */
int archive_toc_lex(void);

#endif /* ARCHIVE_TOC_LEXER_H */
