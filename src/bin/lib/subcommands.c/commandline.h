/*
 * commandline.h
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#ifndef COMMANDLINE_H
#define COMMANDLINE_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

typedef int (*command_getopt)(int argc, char **argv);
typedef void (*command_run)(int argc, char **argv);

typedef struct CommandLine
{
	const char *name;
	const char *shortDescription;
	const char *usageSuffix;
	const char *help;

	command_getopt getopt;
	command_run run;

	struct CommandLine **subcommands;
	char *breadcrumb;
} CommandLine;

extern CommandLine *current_command;

#define make_command_set(name, desc, usage, help, getopt, set) \
	{ name, desc, usage, help, getopt, NULL, set, NULL }

#define make_command(name, desc, usage, help, getopt, run) \
	{ name, desc, usage, help, getopt, run, NULL, NULL }

bool commandline_run(CommandLine *command, int argc, char **argv);
void commandline_help(FILE *stream);
void commandline_print_usage(CommandLine *command, FILE *stream);
void commandline_print_subcommands(CommandLine *command, FILE *stream);
void commandline_print_command_tree(CommandLine *command, FILE *stream);
void commandline_add_breadcrumb(CommandLine *command, CommandLine *subcommand);

#define streq(a, b) (a != NULL && b != NULL && strcmp(a, b) == 0)

#endif  /* COMMANDLINE_H */
