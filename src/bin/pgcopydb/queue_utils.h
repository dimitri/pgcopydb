/*
 * src/bin/pgcopydb/queue_utils.h
 *   Utility functions for inter-process queueing
 */

#ifndef QUEUE_UTILS_H
#define QUEUE_UTILS_H

#include <stdbool.h>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include "postgres.h"

typedef struct Queue
{
	char *name;
	int qId;
	pid_t owner;
} Queue;


/*
 * Message types that we send on the queue. The only messages we send are Oid
 * from either table (to drive a vacuum analyze job) or an index oid (to drive
 * a CREATE INDEX job).
 */
typedef enum
{
	QMSG_TYPE_UNKNOWN = 0,
	QMSG_TYPE_TABLEOID,         /* table oid */
	QMSG_TYPE_TABLEPOID,        /* table oid, table partition number */
	QMSG_TYPE_INDEXOID,         /* index oid */
	QMSG_TYPE_STREAM_TRANSFORM, /* lsn position for transform process */
	QMSG_TYPE_BLOBOID,          /* large object oid */
	QMSG_TYPE_STOP
} QMessageType;

typedef struct QMessage
{
	long type;
	union
	{
		uint32_t oid;
		uint64_t lsn;

		/* table parts (support for COPY partitionning) */
		struct tp
		{
			uint32_t oid;
			uint32_t part;
		} tp;
	} data;
} QMessage;

bool queue_create(Queue *queue, char *name);
bool queue_unlink(Queue *queue);

bool queue_send(Queue *queue, QMessage *msg);
bool queue_receive(Queue *queue, QMessage *msg);

/* see struct msqid_ds in msgctl(2) */
typedef struct QueueStats
{
	uint64_t msg_cbytes;    /* number of bytes in use on the queue */
	uint64_t msg_qnum;      /* number of msgs in the queue */
	pid_t msg_lspid;        /* pid of last msgsnd() */
	pid_t msg_lrpid;        /* pid of last msgrcv() */
} QueueStats;

bool queue_stats(Queue *queue, QueueStats *stats);

#endif /* QUEUE_UTILS_H */
