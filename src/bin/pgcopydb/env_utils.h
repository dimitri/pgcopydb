/*
 * src/bin/pgcopydb/env_utils.h
 *   Utility functions for interacting with environment settings.
 */

#ifndef ENV_UTILS_H
#define ENV_UTILS_H

#include "postgres_fe.h"

bool env_found_empty(const char *name);
bool env_exists(const char *name);
bool get_env_copy(const char *name, char *outbuffer, int maxLength);
bool get_env_copy_with_fallback(const char *name, char *result, int maxLength,
								const char *fallback);
bool get_env_dup(const char *name, char **result);
bool get_env_dup_with_fallback(const char *name, char **result, const char *fallback);
bool get_env_pgdata(char *pgdata);
void get_env_pgdata_or_exit(char *pgdata);
#endif /* ENV_UTILS_H */
