/*
 * src/bin/pgcopydb/config.h
 *     Configuration data structure and function definitions
 *
 * Licensed under the PostgreSQL License.
 *
 */

#ifndef CONFIG_H
#define CONFIG_H

#include <limits.h>
#include <stdbool.h>

#include "cli_common.h"
#include "config.h"
#include "defaults.h"


void config_init(CopyDBOptions *config);
bool config_read_file(CopyDBOptions *config, const char *filename);
bool config_write_file(CopyDBOptions *config, const char *filename);
bool config_write(FILE *stream, CopyDBOptions *config);
bool config_to_json(CopyDBOptions *config, JSON_Value *js);
void config_log_settings(CopyDBOptions *config);

bool config_get_setting(CopyDBOptions *config,
						const char *filename,
						const char *path,
						char *value, size_t size);

bool config_set_setting(CopyDBOptions *config,
						const char *filename,
						const char *path,
						char *value);

bool config_merge_options(CopyDBOptions *config, CopyDBOptions *options,
						  const char *filename);

#endif /* CONFIG_H */
