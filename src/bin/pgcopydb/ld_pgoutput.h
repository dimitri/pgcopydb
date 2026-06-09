/*
 * src/bin/pgcopydb/ld_pgoutput.h
 *   pgoutput logical decoding plugin support for pgcopydb.
 *
 * pgoutput is built into PostgreSQL core (v10+) and uses a binary wire
 * protocol with typed messages (B/C/R/I/U/D/T/Y/O).  Unlike test_decoding
 * and wal2json, which produce text, pgoutput sends raw binary that must be
 * parsed here.
 *
 * This header is included by ld_stream.h to embed the relation cache and
 * the current decoded message directly into StreamContext.  It must NOT
 * include ld_stream.h to avoid a circular dependency.
 */

#ifndef LD_PGOUTPUT_H
#define LD_PGOUTPUT_H

#include <stdbool.h>
#include <stdint.h>

#include "pgsql.h"
#include "uthash.h"


/*
 * Per-column entry stored in the relation definition cache built from
 * pgoutput 'R' (RELATION) messages.
 */
typedef struct PgoutputAttrCache
{
	int colIndex;                   /* hash key: zero-based position */
	char attname[PG_NAMEDATALEN];
	uint32_t typeOID;
	bool isReplicaIdentity;         /* true if flags bit 0x01 is set */
	UT_hash_handle hh;
} PgoutputAttrCache;


/*
 * Per-relation entry built from pgoutput 'R' (RELATION) messages.
 * Keyed by relOid for fast lookup on every DML message.
 */
typedef struct PgoutputRelationCache
{
	uint32_t relOid;                /* hash key */
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char replicaIdentity;           /* 'd', 'i', 'f', 'n' */
	int natts;
	PgoutputAttrCache *attrs;       /* colIndex → attr, NULL-terminated uthash */
	UT_hash_handle hh;
} PgoutputRelationCache;


/*
 * One column decoded from a pgoutput binary tuple.
 *
 * status values match pgoutput wire protocol:
 *   't' — text value present (value is malloc'd, caller owns it)
 *   'n' — NULL
 *   'u' — column not sent (unchanged TOAST value)
 */
typedef struct PgoutputColumn
{
	char name[PG_NAMEDATALEN];
	char status;
	char *value;                    /* malloc'd; non-NULL only when status='t' */
} PgoutputColumn;


/*
 * One fully-decoded pgoutput DML or transaction-control message.
 * Stored in StreamContext.pgoutputMsg during the receive step.
 *
 * action uses the StreamAction char codes ('B','C','I','U','D','T') rather
 * than the enum to avoid a circular include with ld_stream.h.
 */
typedef struct PgoutputMessage
{
	char action;                    /* 'B','C','I','U','D','T' */
	uint32_t xid;
	uint64_t lsn;

	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];

	char oldType;                   /* 'K'=key-only, 'O'=full-old, 0=absent */
	int ncols_old;
	PgoutputColumn *old_cols;       /* malloc'd array; NULL when oldType==0 */

	int ncols_new;
	PgoutputColumn *new_cols;       /* malloc'd array; NULL for DELETE */
} PgoutputMessage;


/* Forward declarations to avoid circular includes */
struct StreamContext;
struct DatabaseCatalog;


bool parsePgoutputMessageActionAndXid(LogicalStreamContext *context);
bool preparePgoutputMessage(LogicalStreamContext *context);
bool parsePgoutputMessage(struct StreamContext *context,
						  struct DatabaseCatalog *outputDB,
						  int64_t outputId);
void free_pgoutput_message(PgoutputMessage *msg);


#endif  /* LD_PGOUTPUT_H */
