/*
 * src/bin/pgcopydb/archive_toc_parser.c
 *     Entry point that feeds one pg_restore --list line to the flex
 *     lexer (archive_toc_lexer.l) and bison grammar (archive_toc_
 *     grammar.y), which together tokenize and parse it directly into an
 *     ArchiveContentItem -- there is no C-level string splitting left in
 *     this file; see archive_toc_grammar.y's own file header for the
 *     full dispatch.
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
 * error in archive_toc_grammar.y) is not fatal, so callers can keep
 * going. archive_iter_toc_next (pgcmd.c) already warns when item->desc
 * comes back ARCHIVE_TAG_UNKNOWN, which a failed parse leaves it as
 * (archive_toc_lex_line/archive_toc_parse never touch item->desc
 * themselves until a production actually matches), so that one
 * warning path covers both a malformed line and an unrecognized one.
 *
 * This is a deliberate, long-standing project policy, not something
 * new here: see https://github.com/dimitri/pgcopydb/issues/123, where
 * the original hand-rolled parser already chose WARN over ERROR for
 * exactly this reason -- "there is no impact at all in the behavior of
 * pgcopydb (hence WARN, not ERROR), but it's enough of a problem that
 * someone might want to report it, giving me a chance to go ahead and
 * fix the parser." This rewrite generalizes that same policy from one
 * specific case (an ACL/COMMENT entry the old parser couldn't split)
 * to every shape this grammar doesn't yet structure.
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
