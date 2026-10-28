/*
 * src/bin/pgcopydb/archive_toc_parser.c
 *     Entry point that feeds one pg_restore --list line to the flex
 *     lexer (archive_toc_lexer.l) and bison grammar (archive_toc_
 *     grammar.y), which together tokenize and parse it directly into an
 *     ArchiveContentItem -- there is no C-level string splitting left in
 *     this file; see archive_toc_grammar.y's own file header for why,
 *     and for the shape dispatch this used to do by hand with a table
 *     and a switch.
 */

#include <string.h>

#include "parson.h"

#include "pgcmd.h"
#include "archive_toc_lexer.h"
#include "archive_toc_grammar.h"
#include "log.h"

/*
 * archive_toc_parse_line parses one pg_restore --list line (no trailing
 * newline needed) into item.
 *
 * Always returns true: a line this grammar can't structure (a shape it
 * doesn't recognize, or genuinely malformed input -- see archive_toc_
 * error in archive_toc_grammar.y) is not fatal, the same "we don't
 * support this one yet" convention the previous hand-written parser
 * followed throughout. archive_iter_toc_next (pgcmd.c) already warns
 * when item->desc comes back ARCHIVE_TAG_UNKNOWN, which a failed parse
 * leaves it as (archive_toc_lex_line/archive_toc_parse never touch
 * item->desc themselves until a production actually matches), so that
 * one warning path covers both the previous malformed-prefix case and
 * this one.
 */
bool
archive_toc_parse_line(const char *line, ArchiveContentItem *item)
{
	*item = (ArchiveContentItem) {
		0
	};

	archive_toc_lex_line(line);

	if (archive_toc_parse(item) != 0)
	{
		log_debug("archive_toc_parser: could not parse: %s", line);
	}

	return true;
}
