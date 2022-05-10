/*
 * src/bin/pg_autoctl/ini_implementation.c
 *     The file containing library code used to parse files with .INI syntax
 *
 *     The main reason this is in a separate file is so you can exclude a file
 *     during static analysis. This way we exclude vendored in library code,
 *     but not our code using it.
 *
 * Licensed under the PostgreSQL License.
 */
#define INI_IMPLEMENTATION
#include "ini.h"
