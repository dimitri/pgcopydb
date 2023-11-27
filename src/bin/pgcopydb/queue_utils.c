/*
 * src/bin/pgcopydb/queue_utils.c
 *   Utility functions for inter-process queueing
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <unistd.h>

#include "copydb.h"
#include "defaults.h"
#include "log.h"
#include "queue_utils.h"
#include "signals.h"


/*
 * queue_create creates a new message queue.
 */
bool
queue_create(Queue *queue, char *name)
{
	queue->name = name;
	queue->owner = getpid();
	queue->qId = msgget(IPC_PRIVATE, 0600);

	if (queue->qId < 0)
	{
		log_fatal("Failed to create message queue: %m");
		return false;
	}

	/* register the queue to the System V resources clean-up array */
	if (!copydb_register_sysv_queue(&system_res_array, queue))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("Created message %s queue %d (cleanup with `ipcrm -q %d`)",
			  queue->name,
			  queue->qId,
			  queue->qId);

	return true;
}


/*
 * queue_unlink removes an existing message queue.
 */
bool
queue_unlink(Queue *queue)
{
	log_debug("iprm -q %d (%s)", queue->qId, queue->name);

	if (msgctl(queue->qId, IPC_RMID, NULL) != 0)
	{
		log_error("Failed to delete %s message queue %d: %m",
				  queue->name,
				  queue->qId);
		return false;
	}

	/* mark the queue as unlinked to the System V resources clean-up array */
	if (!copydb_unlink_sysv_queue(&system_res_array, queue))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * queue_send sends a message on the queue.
 */
bool
queue_send(Queue *queue, QMessage *msg)
{
	int errStatus;
	bool firstLoop = true;

	do {
		if (firstLoop)
		{
			firstLoop = false;
		}
		else
		{
			pg_usleep(10 * 1000); /* 10 ms */
		}

		errStatus = msgsnd(queue->qId, msg, sizeof(msg->data), IPC_NOWAIT);
	} while (errStatus < 0 && (errno == EINTR || errno == EAGAIN));

	if (errStatus < 0)
	{
		log_error("Failed to send a message to %s queue (%d) "
				  "with type %ld: %m",
				  queue->name,
				  queue->qId,
				  msg->type);
		return false;
	}

	return true;
}


/*
 * queue_receive receives a message from the queue.
 */
bool
queue_receive(Queue *queue, QMessage *msg)
{
	QMessage *buf = (QMessage *) calloc(1, sizeof(QMessage));

	int errStatus;
	bool firstLoop = true;

	do {
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			free(buf);
			return false;
		}

		if (firstLoop)
		{
			firstLoop = false;
		}
		else
		{
			pg_usleep(10 * 1000); /* 10 ms */
		}

		errStatus = msgrcv(queue->qId, buf, sizeof(buf->data), 0, IPC_NOWAIT);
	} while (errStatus < 0 && (errno == EINTR || errno == ENOMSG));

	if (errStatus < 0)
	{
		log_error("Failed to receive a message from %s queue (%d): %m",
				  queue->name,
				  queue->qId);
		free(buf);
		return false;
	}

	*msg = *buf;
	free(buf);
	return true;
}


/*
 * queue_stats retrieves statistics from the queue.
 */
bool
queue_stats(Queue *queue, QueueStats *stats)
{
	struct msqid_ds ds = { 0 };

	if (msgctl(queue->qId, IPC_STAT, &ds) != 0)
	{
		log_error("Failed to get stats for %s message queue %d: %m",
				  queue->name,
				  queue->qId);
		return false;
	}

	stats->msg_cbytes = ds.msg_cbytes;
	stats->msg_qnum = ds.msg_qnum;
	stats->msg_lspid = ds.msg_lspid;
	stats->msg_lrpid = ds.msg_lrpid;

	return true;
}
