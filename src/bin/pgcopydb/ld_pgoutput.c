/*
 * src/bin/pgcopydb/ld_pgoutput.c
 *   pgoutput logical decoding plugin support for pgcopydb.
 *
 * pgoutput uses a binary wire protocol (proto_version=1) with typed messages.
 * All integers are big-endian (network byte order).
 *
 * Message layout for proto_version=1 (no streaming, no xid in DML):
 *
 *  BEGIN:    'B' u64(final_lsn) u64(commit_time) u32(xid)
 *  COMMIT:   'C' u8(flags) u64(commit_lsn) u64(end_lsn) u64(commit_time)
 *  RELATION: 'R' u32(relOid) cstr(nspname) cstr(relname) u8(replident)
 *                u16(natts) per-col[u8(flags) cstr(name) u32(typeOid) i32(typmod)]
 *  INSERT:   'I' u32(relOid) 'N' tuple
 *  UPDATE:   'U' u32(relOid) [('K'|'O') old_tuple] 'N' new_tuple
 *  DELETE:   'D' u32(relOid) ('K'|'O') old_tuple
 *  TRUNCATE: 'T' u32(nrelids) u8(flags) nrelids*u32(relOid)
 *  TYPE:     'Y' ...  (filtered out)
 *  ORIGIN:   'O' ...  (filtered out)
 *
 * Tuple: u16(ncols) per-col[u8(status) if-'t': i32(len) + len bytes]
 *   status: 'n'=null, 'u'=unchanged TOAST, 't'=text value
 */

#include <errno.h>
#include <inttypes.h>
#include <string.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "access/xlogdefs.h"

#include "schema.h"     /* pulls in sqlite3.h */

#include "catalog.h"
#include "copydb.h"
#include "ld_pgoutput.h"
#include "ld_stream.h"
#include "log.h"
#include "pgsql.h"
#include "string_utils.h"

/*
 * Replica identity flag from proto.c (not exported in a header we can use).
 */
#define PGOUT_IS_REPLICA_IDENTITY 0x01


/* ----------
 * Big-endian binary reader helpers.
 * All bounds-check against bufLen; log_error + return 0/NULL on overflow.
 * ----------
 */
static uint8_t
pgout_u8(const char *buf, int *pos, int bufLen)
{
	if (*pos + 1 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading u8 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}
	return (uint8_t) buf[(*pos)++];
}


static int16_t
pgout_i16(const char *buf, int *pos, int bufLen)
{
	if (*pos + 2 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading i16 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}
	uint16_t v = ((uint16_t) (uint8_t) buf[*pos] << 8) |
				 (uint16_t) (uint8_t) buf[*pos + 1];
	*pos += 2;
	return (int16_t) v;
}


static uint32_t
pgout_u32(const char *buf, int *pos, int bufLen)
{
	if (*pos + 4 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading u32 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}
	uint32_t v = ((uint32_t) (uint8_t) buf[*pos] << 24) |
				 ((uint32_t) (uint8_t) buf[*pos + 1] << 16) |
				 ((uint32_t) (uint8_t) buf[*pos + 2] << 8) |
				 (uint32_t) (uint8_t) buf[*pos + 3];
	*pos += 4;
	return v;
}


static int32_t
pgout_i32(const char *buf, int *pos, int bufLen)
{
	return (int32_t) pgout_u32(buf, pos, bufLen);
}


static uint64_t
pgout_u64(const char *buf, int *pos, int bufLen)
{
	if (*pos + 8 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading u64 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}
	uint64_t hi = pgout_u32(buf, pos, bufLen);
	uint64_t lo = pgout_u32(buf, pos, bufLen);
	return (hi << 32) | lo;
}


/*
 * pgout_cstr reads a null-terminated C-string from the buffer.
 * Returns a pointer into buf[] (not a copy). On overflow returns NULL.
 */
static const char *
pgout_cstr(const char *buf, int *pos, int bufLen)
{
	int start = *pos;

	while (*pos < bufLen && buf[*pos] != '\0')
	{
		(*pos)++;
	}

	if (*pos >= bufLen)
	{
		log_error("pgoutput: unterminated string at pos %d (len %d)",
				  start, bufLen);
		return NULL;
	}

	const char *s = buf + start;
	(*pos)++;               /* consume the NUL */
	return s;
}


/* ----------
 * Relation cache management.
 * ----------
 */

/*
 * pgoutput_cache_relation parses a RELATION ('R') message starting at *pos
 * and inserts/updates the relation in ctx->pgoutputRelationCache.
 *
 * On return *pos is advanced past the full message.
 */
static bool
pgoutput_cache_relation(StreamContext *privateContext,
						const char *buf, int bufLen, int pos)
{
	/* pos is already past the 'R' type byte */
	uint32_t relOid = pgout_u32(buf, &pos, bufLen);

	const char *nspname = pgout_cstr(buf, &pos, bufLen);
	if (nspname == NULL)
	{
		return false;
	}

	/* empty namespace means pg_catalog */
	if (nspname[0] == '\0')
	{
		nspname = "pg_catalog";
	}

	const char *relname = pgout_cstr(buf, &pos, bufLen);
	if (relname == NULL)
	{
		return false;
	}

	uint8_t replident = pgout_u8(buf, &pos, bufLen);

	int16_t natts = pgout_i16(buf, &pos, bufLen);

	/* check if already cached */
	PgoutputRelationCache *rel = NULL;
	HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);
	if (rel != NULL)
	{
		/* already cached; skip re-parsing (schema changes not supported) */
		log_debug("pgoutput: relation %u already cached, skipping R message",
				  relOid);
		return true;
	}

	rel = (PgoutputRelationCache *) calloc(1, sizeof(PgoutputRelationCache));
	if (rel == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	rel->relOid = relOid;
	strlcpy(rel->nspname, nspname, sizeof(rel->nspname));
	strlcpy(rel->relname, relname, sizeof(rel->relname));
	rel->replicaIdentity = (char) replident;
	rel->natts = natts;

	for (int i = 0; i < natts; i++)
	{
		uint8_t flags = pgout_u8(buf, &pos, bufLen);
		const char *attname = pgout_cstr(buf, &pos, bufLen);
		if (attname == NULL)
		{
			free(rel);
			return false;
		}
		uint32_t typeOID = pgout_u32(buf, &pos, bufLen);
		pgout_i32(buf, &pos, bufLen);  /* typmod — not used */

		PgoutputAttrCache *attr =
			(PgoutputAttrCache *) calloc(1, sizeof(PgoutputAttrCache));
		if (attr == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			free(rel);
			return false;
		}

		attr->colIndex = i;
		strlcpy(attr->attname, attname, sizeof(attr->attname));
		attr->typeOID = typeOID;
		attr->isReplicaIdentity = (flags & PGOUT_IS_REPLICA_IDENTITY) != 0;

		HASH_ADD_INT(rel->attrs, colIndex, attr);
	}

	HASH_ADD_INT(privateContext->pgoutputRelationCache, relOid, rel);

	log_debug("pgoutput: cached relation %u %s.%s replident=%c natts=%d",
			  relOid, rel->nspname, rel->relname, rel->replicaIdentity, natts);

	return true;
}


/* ----------
 * Tuple decoder.
 * ----------
 */

/*
 * decode_tuple reads a pgoutput binary tuple from buf at *pos.
 *
 * Allocates *cols_out (calloc, caller owns) and sets *ncols_out to the
 * tuple width as reported in the wire format.  The 'value' field of each
 * 't'-status column is malloc'd; caller owns via free_pgoutput_message.
 */
static bool
decode_tuple(const char *buf, int bufLen, int *pos,
			 PgoutputRelationCache *rel,
			 PgoutputColumn **cols_out, int *ncols_out)
{
	int16_t ncols = pgout_i16(buf, pos, bufLen);

	if (ncols < 0)
	{
		log_error("pgoutput: negative column count %d in tuple", ncols);
		return false;
	}

	*ncols_out = ncols;

	if (ncols == 0)
	{
		*cols_out = NULL;
		return true;
	}

	PgoutputColumn *cols =
		(PgoutputColumn *) calloc(ncols, sizeof(PgoutputColumn));
	if (cols == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	for (int i = 0; i < ncols; i++)
	{
		uint8_t status = pgout_u8(buf, pos, bufLen);
		cols[i].status = (char) status;

		/* get column name from relation cache */
		if (rel != NULL)
		{
			PgoutputAttrCache *attr = NULL;
			HASH_FIND_INT(rel->attrs, &i, attr);
			if (attr != NULL)
			{
				strlcpy(cols[i].name, attr->attname, sizeof(cols[i].name));
			}
		}

		if (status == 't')
		{
			int32_t len = pgout_i32(buf, pos, bufLen);
			if (len < 0)
			{
				log_error("pgoutput: negative column value length %d", len);
				free(cols);
				return false;
			}
			if (*pos + len > bufLen)
			{
				log_error("pgoutput: buffer underflow reading column value "
						  "(need %d bytes at pos %d, bufLen %d)",
						  len, *pos, bufLen);
				free(cols);
				return false;
			}
			cols[i].value = strndup(buf + *pos, len);
			if (cols[i].value == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				free(cols);
				return false;
			}
			*pos += len;
		}
		else if (status == 'b')
		{
			/* binary column (proto >= 14): read and discard */
			int32_t len = pgout_i32(buf, pos, bufLen);
			if (len >= 0 && *pos + len <= bufLen)
			{
				*pos += len;
			}
			cols[i].status = 'u';   /* treat as unchanged for our purposes */
		}

		/* 'n' and 'u' have no payload */
	}

	*cols_out = cols;
	return true;
}


/* ----------
 * Public API: parsePgoutputMessageActionAndXid
 * ----------
 */

/*
 * parsePgoutputMessageActionAndXid reads the first byte of the pgoutput
 * binary message in context->buffer and sets metadata->action, metadata->xid,
 * and metadata->filterOut.
 *
 * This is the equivalent of parseTestDecodingMessageActionAndXid for pgoutput.
 * Called from parseMessageActionAndXid in ld_stream.c.
 */
bool
parsePgoutputMessageActionAndXid(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	const char *buf = context->buffer;
	int bufLen = context->bufferLen;

	if (bufLen < 1)
	{
		log_error("pgoutput: empty message (bufLen=%d)", bufLen);
		return false;
	}

	char msgtype = buf[0];
	int pos = 1;

	switch (msgtype)
	{
		case 'B':               /* BEGIN */
		{
			if (bufLen < 21)    /* 1 + 8 + 8 + 4 */
			{
				log_error("pgoutput: BEGIN message too short (%d bytes)", bufLen);
				return false;
			}
			pgout_u64(buf, &pos, bufLen);   /* final_lsn */
			pgout_u64(buf, &pos, bufLen);   /* commit_time */
			uint32_t xid = pgout_u32(buf, &pos, bufLen);

			metadata->action = STREAM_ACTION_BEGIN;
			metadata->xid = xid;
			privateContext->currentXid = xid;
			break;
		}

		case 'C':               /* COMMIT */
		{
			metadata->action = STREAM_ACTION_COMMIT;
			metadata->xid = privateContext->currentXid;

			/* don't clear currentXid here; preparePgoutputMessage needs it */
			break;
		}

		case 'R':               /* RELATION — cache it, filter out */
		{
			if (!pgoutput_cache_relation(privateContext, buf, bufLen, pos))
			{
				log_error("pgoutput: failed to cache RELATION message");
				return false;
			}
			metadata->filterOut = true;
			break;
		}

		case 'Y':               /* TYPE — filter out */
		case 'O':               /* ORIGIN — filter out */
		{
			metadata->filterOut = true;
			break;
		}

		case 'I':               /* INSERT */
		case 'U':               /* UPDATE */
		case 'D':               /* DELETE */
		case 'T':               /* TRUNCATE */
		{
			if (bufLen < 5)
			{
				log_error("pgoutput: DML message too short (%d bytes)", bufLen);
				return false;
			}
			uint32_t relOid = pgout_u32(buf, &pos, bufLen);

			/* look up relation to check if it's in pgcopydb schema */
			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel != NULL && strcmp(rel->nspname, "pgcopydb") == 0)
			{
				log_debug("pgoutput: filtering out %c message for pgcopydb.%s",
						  msgtype, rel->relname);
				metadata->filterOut = true;
				break;
			}

			switch (msgtype)
			{
				case 'I':
				{
					metadata->action = STREAM_ACTION_INSERT;
					break;
				}

				case 'U':
				{
					metadata->action = STREAM_ACTION_UPDATE;
					break;
				}

				case 'D':
				{
					metadata->action = STREAM_ACTION_DELETE;
					break;
				}

				case 'T':
				{
					metadata->action = STREAM_ACTION_TRUNCATE;
					break;
				}
			}

			metadata->xid = privateContext->currentXid;
			break;
		}

		default:
		{
			log_debug("pgoutput: unknown message type '%c' (0x%02x), filtering out",
					  msgtype, (unsigned char) msgtype);
			metadata->filterOut = true;
			break;
		}
	}

	return true;
}


/* ----------
 * Public API: preparePgoutputMessage
 * ----------
 */

/*
 * preparePgoutputMessage fully decodes the binary pgoutput message into
 * privateContext->pgoutputMsg.  metadata->jsonBuffer is left NULL — pgoutput
 * does not use the text JSON path.
 *
 * Called from prepareMessageJSONbuffer dispatch in ld_stream.c (after
 * parsePgoutputMessageActionAndXid has already set metadata->action etc.).
 */
bool
preparePgoutputMessage(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);
	PgoutputMessage *msg = &(privateContext->pgoutputMsg);

	const char *buf = context->buffer;
	int bufLen = context->bufferLen;

	/* Reset the message struct */
	free_pgoutput_message(msg);
	memset(msg, 0, sizeof(PgoutputMessage));

	if (bufLen < 1)
	{
		return true;            /* filtered-out messages are fine */
	}
	char msgtype = buf[0];
	int pos = 1;

	msg->action = msgtype;
	msg->xid = metadata->xid;
	msg->lsn = metadata->lsn;

	switch (msgtype)
	{
		case 'B':
		{
			/* already parsed in ActionAndXid; nothing extra to store */
			pgout_u64(buf, &pos, bufLen);   /* final_lsn */
			pgout_u64(buf, &pos, bufLen);   /* commit_time */
			pgout_u32(buf, &pos, bufLen);   /* xid */
			break;
		}

		case 'C':
		{
			pgout_u8(buf, &pos, bufLen);    /* flags */
			msg->lsn = pgout_u64(buf, &pos, bufLen);    /* commit_lsn */
			pgout_u64(buf, &pos, bufLen);   /* end_lsn */
			pgout_u64(buf, &pos, bufLen);   /* commit_time */

			/* clear the XID tracking after COMMIT is decoded */
			privateContext->currentXid = 0;
			break;
		}

		case 'I':
		{
			uint32_t relOid = pgout_u32(buf, &pos, bufLen);
			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel == NULL)
			{
				log_error("pgoutput: INSERT for uncached relOid %u", relOid);
				return false;
			}

			strlcpy(msg->nspname, rel->nspname, sizeof(msg->nspname));
			strlcpy(msg->relname, rel->relname, sizeof(msg->relname));

			uint8_t marker = pgout_u8(buf, &pos, bufLen);
			if (marker != 'N')
			{
				log_error("pgoutput: INSERT expected 'N' marker, got '%c'",
						  marker);
				return false;
			}

			if (!decode_tuple(buf, bufLen, &pos, rel,
							  &msg->new_cols, &msg->ncols_new))
			{
				return false;
			}

			msg->oldType = 0;
			break;
		}

		case 'U':
		{
			uint32_t relOid = pgout_u32(buf, &pos, bufLen);
			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel == NULL)
			{
				log_error("pgoutput: UPDATE for uncached relOid %u", relOid);
				return false;
			}

			strlcpy(msg->nspname, rel->nspname, sizeof(msg->nspname));
			strlcpy(msg->relname, rel->relname, sizeof(msg->relname));

			uint8_t next = pgout_u8(buf, &pos, bufLen);

			if (next == 'K' || next == 'O')
			{
				msg->oldType = (char) next;
				if (!decode_tuple(buf, bufLen, &pos, rel,
								  &msg->old_cols, &msg->ncols_old))
				{
					return false;
				}

				/* read the 'N' marker for the new tuple */
				uint8_t n_marker = pgout_u8(buf, &pos, bufLen);
				if (n_marker != 'N')
				{
					log_error("pgoutput: UPDATE expected 'N' after old tuple, "
							  "got '%c'", n_marker);
					return false;
				}
			}
			else if (next == 'N')
			{
				/*
				 * No old tuple sent: this happens when REPLICA IDENTITY is
				 * DEFAULT and the key columns did not change.  We need to
				 * synthesize an old-key tuple by extracting the replica
				 * identity columns from the new tuple.
				 *
				 * Read the new tuple first; then build old from it.
				 */
				msg->oldType = 'K';
			}
			else
			{
				log_error("pgoutput: UPDATE unexpected marker '%c'", next);
				return false;
			}

			if (!decode_tuple(buf, bufLen, &pos, rel,
							  &msg->new_cols, &msg->ncols_new))
			{
				return false;
			}

			/*
			 * Synthesize old-key tuple from new tuple when no old tuple was
			 * sent (key unchanged, REPLICA IDENTITY DEFAULT).
			 */
			if (next == 'N')
			{
				/* count replica identity columns */
				int nkey = 0;
				for (int i = 0; i < msg->ncols_new; i++)
				{
					PgoutputAttrCache *attr = NULL;
					HASH_FIND_INT(rel->attrs, &i, attr);
					if (attr != NULL && attr->isReplicaIdentity)
					{
						nkey++;
					}
				}

				if (nkey == 0)
				{
					log_error("pgoutput: UPDATE without old tuple and no "
							  "replica identity columns for %s.%s",
							  rel->nspname, rel->relname);
					return false;
				}

				msg->ncols_old = nkey;
				msg->old_cols = (PgoutputColumn *) calloc(
					nkey, sizeof(PgoutputColumn));
				if (msg->old_cols == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				int ki = 0;
				for (int i = 0; i < msg->ncols_new && ki < nkey; i++)
				{
					PgoutputAttrCache *attr = NULL;
					HASH_FIND_INT(rel->attrs, &i, attr);
					if (attr == NULL || !attr->isReplicaIdentity)
					{
						continue;
					}

					msg->old_cols[ki] = msg->new_cols[i];

					/* deep-copy the value string so both tuples own their data */
					if (msg->new_cols[i].status == 't' &&
						msg->new_cols[i].value != NULL)
					{
						msg->old_cols[ki].value = strdup(msg->new_cols[i].value);
						if (msg->old_cols[ki].value == NULL)
						{
							log_error(ALLOCATION_FAILED_ERROR);
							return false;
						}
					}
					ki++;
				}
			}
			break;
		}

		case 'D':
		{
			uint32_t relOid = pgout_u32(buf, &pos, bufLen);
			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel == NULL)
			{
				log_error("pgoutput: DELETE for uncached relOid %u", relOid);
				return false;
			}

			strlcpy(msg->nspname, rel->nspname, sizeof(msg->nspname));
			strlcpy(msg->relname, rel->relname, sizeof(msg->relname));

			uint8_t marker = pgout_u8(buf, &pos, bufLen);
			if (marker != 'K' && marker != 'O')
			{
				log_error("pgoutput: DELETE expected 'K' or 'O', got '%c'",
						  marker);
				return false;
			}

			msg->oldType = (char) marker;
			if (!decode_tuple(buf, bufLen, &pos, rel,
							  &msg->old_cols, &msg->ncols_old))
			{
				return false;
			}
			break;
		}

		case 'T':
		{
			uint32_t nrelids = pgout_u32(buf, &pos, bufLen);
			pgout_u8(buf, &pos, bufLen);    /* flags (cascade, restart_seqs) */

			for (uint32_t i = 0; i < nrelids; i++)
			{
				uint32_t relOid = pgout_u32(buf, &pos, bufLen);
				if (i == 0)
				{
					PgoutputRelationCache *rel = NULL;
					HASH_FIND_INT(privateContext->pgoutputRelationCache,
								  &relOid, rel);
					if (rel != NULL)
					{
						strlcpy(msg->nspname, rel->nspname,
								sizeof(msg->nspname));
						strlcpy(msg->relname, rel->relname,
								sizeof(msg->relname));
					}
				}
			}
			break;
		}

		default:
		{
			/* filtered-out type — nothing to decode */
			break;
		}
	}

	return true;
}


/* ----------
 * Public API: parsePgoutputMessage (transform step)
 * ----------
 */

/*
 * quote_and_dup returns a newly-allocated properly double-quoted SQL
 * identifier string.  Internal double-quote characters are doubled.
 * The caller is responsible for freeing the returned string.
 *
 * Examples:
 *   public       → "public"
 *   Sp1eCial .Char → "Sp1eCial .Char"
 *   s"1          → "s""1"
 *   ABCDE...(63) → "ABCDE...(63)"  (output is 65 bytes, no PG_NAMEDATALEN limit)
 */
static char *
quote_and_dup(const char *name)
{
	if (name == NULL)
	{
		return strdup("");
	}

	size_t len = strlen(name);

	/* Worst case: every char is '"' (doubled) + 2 outer quotes + NUL */
	char *result = malloc(2 * len + 3);
	if (!result)
	{
		return NULL;
	}

	result[0] = '"';
	size_t pos = 1;
	for (size_t i = 0; i < len; i++)
	{
		if (name[i] == '"')
		{
			result[pos++] = '"'; /* escape internal double-quote */
		}
		result[pos++] = name[i];
	}
	result[pos++] = '"';
	result[pos] = '\0';
	return result;
}


/*
 * Fetch-function context for iterating pgoutput_col rows.
 */
typedef struct PgoutputColContext
{
	int colCount;
	int colCap;
	char (*sections)[2];     /* 'N', 'K', 'O' */
	int *positions;
	char (*names)[PG_NAMEDATALEN];
	char (*statuses)[2];
	char **values;
} PgoutputColContext;


/*
 * fill_tuple populates a LogicalMessageTuple from the subset of columns in
 * ctx that match the given section ('N', 'K', or 'O').
 *
 * 'u' (unchanged TOAST) columns are skipped in ALL sections.
 *
 * For the 'K' (key-only) section, pgoutput fills non-key column positions with
 * status='n' as placeholders — they carry no data and must be skipped so that
 * only the actual key columns appear in the generated WHERE clause.
 *
 * For 'N' (new tuple) and 'O' (full old tuple) sections, 'n' means the column
 * is genuinely NULL and should render as IS NULL in the generated SQL.
 *
 * Column names are double-quoted so that schema/table/column identifiers with
 * mixed case, spaces, or special characters survive the SQL round-trip.
 */
static bool
fill_tuple(LogicalMessageTuple *tuple, PgoutputColContext *ctx, char section)
{
	/*
	 * Count matching columns.
	 *
	 * Always skip 'u' (unchanged TOAST) in every section.
	 *
	 * For section 'K' (key-only tuple, REPLICA IDENTITY DEFAULT): also skip
	 * 'n' columns.  pgoutput fills non-key positions with status='n' as
	 * placeholders; they carry no information and must NOT appear in the WHERE
	 * clause.  For section 'O' (full old tuple) and 'N' (new tuple), 'n'
	 * means the column is genuinely NULL and should render as IS NULL / = NULL.
	 */
	bool key_section = (section == 'K');
	int count = 0;
	for (int i = 0; i < ctx->colCount; i++)
	{
		if (ctx->sections[i][0] != section)
		{
			continue;
		}
		if (ctx->statuses[i][0] == 'u')
		{
			continue;
		}
		if (key_section && ctx->statuses[i][0] == 'n')
		{
			continue;
		}
		count++;
	}

	if (!AllocateLogicalMessageTuple(tuple, count))
	{
		return false;
	}

	if (count == 0)
	{
		return true;
	}

	LogicalMessageValues *vals = &(tuple->values.array[0]);
	int j = 0;

	for (int i = 0; i < ctx->colCount && j < count; i++)
	{
		if (ctx->sections[i][0] != section)
		{
			continue;
		}
		if (ctx->statuses[i][0] == 'u')
		{
			continue;
		}
		if (key_section && ctx->statuses[i][0] == 'n')
		{
			continue;
		}

		LogicalMessageAttribute *attr = &(tuple->attributes.array[j]);
		LogicalMessageValue *val = &(vals->array[j]);

		/* double-quote the column name for safe SQL embedding */
		attr->attname = quote_and_dup(ctx->names[i]);
		if (attr->attname == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}
		val->oid = TEXTOID;

		if (ctx->statuses[i][0] == 'n')
		{
			val->isNull = true;
		}
		else
		{
			val->isNull = false;
			val->val.str = ctx->values[i] != NULL
						   ? strdup(ctx->values[i]) : strdup("");
		}

		j++;
	}

	return true;
}


/*
 * parsePgoutputMessage is the transform-step counterpart to
 * preparePgoutputMessage.  It queries pgoutput_col for the given output row
 * and builds a LogicalTransactionStatement in privateContext->stmt.
 *
 * Called from the DML dispatch switch in ld_transform.c.
 */
bool
parsePgoutputMessage(StreamContext *privateContext,
					 DatabaseCatalog *outputDB,
					 int64_t outputId)
{
	LogicalTransactionStatement *stmt = privateContext->stmt;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	sqlite3 *db = outputDB->db;
	if (db == NULL)
	{
		log_error("BUG: parsePgoutputMessage: outputDB is NULL");
		return false;
	}

	/* Read all pgoutput_col rows for this output_id */
	const char *sql =
		"select section, pos, name, status, value "
		"  from pgoutput_col "
		" where output_id = $1 "
		" order by section, pos";

	SQLiteQuery query = { 0 };
	query.errorOnZeroRows = false;

	if (!catalog_sql_prepare(db, sql, &query))
	{
		return false;
	}

	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "output_id", outputId, NULL }
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		return false;
	}

	/* We don't know the count upfront, so use a dynamic array */
	int colCap = 64;
	int colCount = 0;
	char (*sections)[2] = (char (*)[2])calloc(colCap, 2);
	int *positions = (int *) calloc(colCap, sizeof(int));
	char (*names)[PG_NAMEDATALEN] = (char (*)[PG_NAMEDATALEN])
									calloc(colCap, PG_NAMEDATALEN);
	char (*statuses)[2] = (char (*)[2])calloc(colCap, 2);
	char **values = (char **) calloc(colCap, sizeof(char *));

	if (!sections || !positions || !names || !statuses || !values)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		(void) catalog_sql_finalize(&query);
		return false;
	}

	int rc;
	while ((rc = catalog_sql_step(&query)) == SQLITE_ROW)
	{
		if (colCount >= colCap)
		{
			colCap *= 2;
			sections = realloc(sections, colCap * 2);
			positions = realloc(positions, colCap * sizeof(int));
			names = realloc(names, colCap * PG_NAMEDATALEN);
			statuses = realloc(statuses, colCap * 2);
			values = realloc(values, colCap * sizeof(char *));
			if (!sections || !positions || !names || !statuses || !values)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				(void) catalog_sql_finalize(&query);
				return false;
			}
		}

		const char *sec = (char *) sqlite3_column_text(query.ppStmt, 0);
		sections[colCount][0] = sec ? sec[0] : 'N';
		sections[colCount][1] = '\0';

		positions[colCount] = sqlite3_column_int(query.ppStmt, 1);

		const char *nm = (char *) sqlite3_column_text(query.ppStmt, 2);
		if (nm)
		{
			strlcpy(names[colCount], nm, PG_NAMEDATALEN);
		}

		const char *st = (char *) sqlite3_column_text(query.ppStmt, 3);
		statuses[colCount][0] = st ? st[0] : 't';
		statuses[colCount][1] = '\0';

		const char *val = (char *) sqlite3_column_text(query.ppStmt, 4);
		values[colCount] = val ? strdup(val) : NULL;

		colCount++;
	}

	if (rc != SQLITE_DONE)
	{
		log_error("pgoutput: error iterating pgoutput_col for output_id %lld: %s",
				  (long long) outputId, sqlite3_errmsg(db));
		(void) catalog_sql_finalize(&query);
		return false;
	}

	(void) catalog_sql_finalize(&query);

	/* Build the context struct */
	PgoutputColContext ctx = {
		.colCount = colCount,
		.colCap = colCap,
		.sections = sections,
		.positions = positions,
		.names = names,
		.statuses = statuses,
		.values = values
	};

	/* Get nspname / relname from the output row (already in metadata via
	 * stream_transform_prepare_message reading the output table). We read
	 * it from the calling code's ReplayDBOutputMessage but that's not
	 * accessible here.  Instead we'll read it again directly. */

	/*
	 * Set the relation on stmt using the nspname/relname from output table.
	 * The caller (stream_transform_prepare_message via parseMessage) already
	 * read these from ReplayDBOutputMessage output->nspname / output->relname
	 * and stored them in metadata through a small extension below.
	 * For now, read them from the output row directly.
	 */
	const char *nspsql =
		"select nspname, relname, old_type from output where id = $1";

	SQLiteQuery nsq = { 0 };
	nsq.errorOnZeroRows = true;

	char nspname[PG_NAMEDATALEN] = { 0 };
	char relname[PG_NAMEDATALEN] = { 0 };
	char old_type = 0;

	if (catalog_sql_prepare(db, nspsql, &nsq))
	{
		BindParam nsp_params[1] = {
			{ BIND_PARAMETER_TYPE_INT64, "id", outputId, NULL }
		};

		if (catalog_sql_bind(&nsq, nsp_params, 1))
		{
			if (catalog_sql_step(&nsq) == SQLITE_ROW)
			{
				const char *ns = (char *) sqlite3_column_text(nsq.ppStmt, 0);
				const char *rn = (char *) sqlite3_column_text(nsq.ppStmt, 1);
				const char *ot = (char *) sqlite3_column_text(nsq.ppStmt, 2);
				if (ns)
				{
					strlcpy(nspname, ns, sizeof(nspname));
				}
				if (rn)
				{
					strlcpy(relname, rn, sizeof(relname));
				}
				if (ot)
				{
					old_type = ot[0];
				}
			}
		}
		(void) catalog_sql_finalize(&nsq);
	}

	StreamAction action = (StreamAction) metadata->action;

	switch (action)
	{
		case STREAM_ACTION_INSERT:
		{
			stmt->stmt.insert.table.nspname = quote_and_dup(nspname);
			stmt->stmt.insert.table.relname = quote_and_dup(relname);

			stmt->stmt.insert.new.count = 1;
			stmt->stmt.insert.new.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));
			if (stmt->stmt.insert.new.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}
			if (!fill_tuple(&stmt->stmt.insert.new.array[0], &ctx, 'N'))
			{
				return false;
			}
			break;
		}

		case STREAM_ACTION_UPDATE:
		{
			stmt->stmt.update.table.nspname = quote_and_dup(nspname);
			stmt->stmt.update.table.relname = quote_and_dup(relname);

			stmt->stmt.update.old.count = 1;
			stmt->stmt.update.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));
			stmt->stmt.update.new.count = 1;
			stmt->stmt.update.new.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));
			if (!stmt->stmt.update.old.array || !stmt->stmt.update.new.array)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			/* old tuple: section = old_type ('K' or 'O') */
			char old_sec = (old_type == 'O') ? 'O' : 'K';
			if (!fill_tuple(&stmt->stmt.update.old.array[0], &ctx, old_sec))
			{
				return false;
			}
			if (!fill_tuple(&stmt->stmt.update.new.array[0], &ctx, 'N'))
			{
				return false;
			}
			break;
		}

		case STREAM_ACTION_DELETE:
		{
			stmt->stmt.delete.table.nspname = quote_and_dup(nspname);
			stmt->stmt.delete.table.relname = quote_and_dup(relname);

			stmt->stmt.delete.old.count = 1;
			stmt->stmt.delete.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));
			if (stmt->stmt.delete.old.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			char old_sec = (old_type == 'O') ? 'O' : 'K';
			if (!fill_tuple(&stmt->stmt.delete.old.array[0], &ctx, old_sec))
			{
				return false;
			}
			break;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			stmt->stmt.truncate.table.nspname = quote_and_dup(nspname);
			stmt->stmt.truncate.table.relname = quote_and_dup(relname);
			break;
		}

		default:
		{
			log_error("BUG: parsePgoutputMessage called with action %c",
					  metadata->action);
			return false;
		}
	}

	/* free the temporary column arrays */
	for (int i = 0; i < colCount; i++)
	{
		free(values[i]);
	}
	free(sections);
	free(positions);
	free(names);
	free(statuses);
	free(values);

	return true;
}


/* ----------
 * Public API: free_pgoutput_message
 * ----------
 */
void
free_pgoutput_message(PgoutputMessage *msg)
{
	if (msg == NULL)
	{
		return;
	}

	if (msg->old_cols != NULL)
	{
		for (int i = 0; i < msg->ncols_old; i++)
		{
			free(msg->old_cols[i].value);
		}
		free(msg->old_cols);
		msg->old_cols = NULL;
	}

	if (msg->new_cols != NULL)
	{
		for (int i = 0; i < msg->ncols_new; i++)
		{
			free(msg->new_cols[i].value);
		}
		free(msg->new_cols);
		msg->new_cols = NULL;
	}

	msg->ncols_old = 0;
	msg->ncols_new = 0;
}
