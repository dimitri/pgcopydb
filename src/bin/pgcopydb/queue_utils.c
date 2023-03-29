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

	log_debug("Created message %s queue %d (cleanup with ipcrm -q)",
			  queue->name,
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
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
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

		errStatus = msgsnd(queue->qId, msg, sizeof(QMessage), IPC_NOWAIT);
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
	int errStatus;
	bool firstLoop = true;

	do {
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
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

		errStatus = msgrcv(queue->qId, msg, sizeof(QMessage), 0, IPC_NOWAIT);
	} while (errStatus < 0 && (errno == EINTR || errno == ENOMSG));

	if (errStatus < 0)
	{
		log_error("Failed to receive a message from %s queue (%d): %m",
				  queue->name,
				  queue->qId);
		return false;
	}

	return true;
}
