/*
 * src/bin/pgcopydb/lock_utils.h
 *   Utility functions for inter-process locking
 */

#ifndef LOCK_UTILS_H
#define LOCK_UTILS_H

#include <stdbool.h>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>

typedef struct Semaphore
{
	int semId;
	int initValue;
	pid_t owner;
} Semaphore;


bool semaphore_init(Semaphore *semaphore);
bool semaphore_finish(Semaphore *semaphore);

bool semaphore_create(Semaphore *semaphore);
bool semaphore_open(Semaphore *semaphore);
bool semaphore_unlink(Semaphore *semaphore);

bool semaphore_cleanup(const char *pidfile);

bool semaphore_lock(Semaphore *semaphore);
bool semaphore_unlock(Semaphore *semaphore);

void semaphore_log_lock_function(void *udata, int mode);

#endif /* LOCK_UTILS_H */
