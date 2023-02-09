/*
 * src/bin/pgcopydb/cli_root.h
 *     Implementation of a CLI which lets you run individual keeper routines
 *     directly
 */

#ifndef CLI_ROOT_H
#define CLI_ROOT_H

#include "commandline.h"
#include "lock_utils.h"

extern char pgcopydb_argv0[];
extern char pgcopydb_program[];
extern int pgconnect_timeout;
extern int logLevel;

extern Semaphore log_semaphore;

extern char *ps_buffer;
extern size_t ps_buffer_size;
extern size_t last_status_len;

extern CommandLine help;
extern CommandLine version;

extern CommandLine root;
extern CommandLine *root_subcommands[];

extern CommandLine root_with_debug;
extern CommandLine *root_subcommands_with_debug[];

int root_options(int argc, char **argv);

/* cli_clone_follow.c */
void cli_clone(int argc, char **argv);
void cli_follow(int argc, char **argv);

/* cli_copy.h */
extern CommandLine copy__db_command;
extern CommandLine clone_command;
extern CommandLine fork_command;
extern CommandLine follow_command;
extern CommandLine copy_commands;

/* cli_create.c */
extern CommandLine create_snapshot_command;
extern CommandLine create_commands;
extern CommandLine drop_commands;

/* cli_dump.h */
extern CommandLine dump_commands;

/* cli_ping.h */
extern CommandLine ping_command;

/* cli_restore.h */
extern CommandLine restore_commands;

/* cli_list.h */
extern CommandLine list_commands;

/* cli_stream.h */
extern CommandLine stream_commands;

/* cli_sentinel.h */
extern CommandLine sentinel_commands;

#endif  /* CLI_ROOT_H */
