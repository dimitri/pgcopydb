/*
 * src/bin/pgcopydb/ini_file.h
 *     Functions to parse a configuration file using the .INI syntax.
 *
 * Licensed under the PostgreSQL License.
 *
 */

#ifndef INI_FILE_H
#define INI_FILE_H

#include <stdbool.h>
#include <stdio.h>

#include "parson.h"

#define INI_STRING_T 1          /* char *target */
#define INI_STRBUF_T 2          /* char target[size] */
#define INI_INT_T 3             /* int target */
#define INI_BOOL_T 4            /* bool target */
#define INI_END_T 5

/*
 * IniOption represent a key/value as written in the INI format:
 *
 * [section]
 * name = "values"
 * int  = 10
 *
 * The IniOption structure is used both for specifying what we expect to read
 * in the INI file: required, strdefault, and intdefault, and what has been
 * actually read from it: strval/intval.
 *
 * Given the previous contents and this structure as input:
 *
 * {
 *   {INI_STRING_T, "section", "name", true, "default", -1, -1, &str, NULL},
 *   {INI_INT_T,    "section", "int",  true, NULL, 1, -1, NULL, &int},
 *   {INI_END_T, NULL, NULL, false, NULL, -1, -1, NULL, NULL}
 * }
 *
 * Then after reading the ini file with `read_ini_file' then *str = "values"
 * and *int = 10.
 */
typedef struct IniOption
{
	int type;
	const char *section;
	const char *name;
	const char *optName;        /* command line option name */
	bool required;
	bool compat;               /* compatibility: read but don't write */
	char *strDefault;          /* default value when type is string */
	int intDefault;            /* default value when type is int */
	int boolDefault;           /* default value when type is bool */
	int strBufferSize;         /* size of the BUFFER when INI_STRBUF_T */
	char **strValue;           /* pointer to a string pointer (typically malloc-ed) */
	char *strBufValue;         /* pointer to a string buffer (on the stack) */
	int *intValue;             /* pointer to an integer */
	bool *boolValue;           /* pointer to a boolean */
} IniOption;

#define make_int_option(section, name, optName, required, value) \
	{ INI_INT_T, section, name, optName, required, false, \
	  NULL, -1, false, -1, NULL, NULL, value, NULL }

#define make_int_option_default(section, name, optName, \
								required, value, default) \
	{ INI_INT_T, section, name, optName, required, false, \
	  NULL, default, false, -1, NULL, NULL, value, NULL }

#define make_bool_option(section, name, optName, required, value) \
	{ INI_BOOL_T, section, name, optName, required, false, \
	  NULL, -1, -1, NULL, NULL, -1, value }

#define make_bool_option_default(section, name, optName, \
								 required, value, default) \
	{ INI_BOOL_T, section, name, optName, required, false, \
	  NULL, -1, default, -1, NULL, NULL, -1, value }

#define make_string_option(section, name, optName, required, value) \
	{ INI_STRING_T, section, name, optName, required, false, \
	  NULL, -1, false, -1, value, NULL, NULL, NULL }

#define make_string_option_default(section, name, optName, required, \
								   value, default) \
	{ INI_STRING_T, section, name, optName, required, false, \
	  default, -1, false, -1, value, NULL, NULL, NULL, NULL }

#define make_strbuf_option(section, name, optName, required, size, value) \
	{ INI_STRBUF_T, section, name, optName, required, false, \
	  NULL, -1, false, size, NULL, value, NULL, NULL }

#define make_strbuf_compat_option(section, name, size, value) \
	{ INI_STRBUF_T, section, name, NULL, false, true, \
	  NULL, -1, false, size, NULL, value, NULL, NULL }

#define make_strbuf_option_default(section, name, optName, required, \
								   size, value, default) \
	{ INI_STRBUF_T, section, name, optName, required, false, \
	  default, -1, false, size, NULL, value, NULL, NULL }

#define INI_OPTION_LAST \
	{ INI_END_T, NULL, NULL, NULL, false, false, \
	  NULL, -1, false, -1, NULL, NULL, NULL, NULL }

bool read_ini_file(const char *filename, IniOption *opts);
bool parse_ini_buffer(const char *filename,
					  char *fileContents,
					  IniOption *optionList);
bool ini_validate_options(IniOption *optionList);
bool ini_set_option_value(IniOption *option, const char *value);
bool ini_option_to_string(IniOption *option, char *dest, size_t size);
bool write_ini_to_stream(FILE *stream, IniOption *optionList);
bool ini_to_json(JSON_Object *jsRoot, IniOption *optionList);
IniOption * lookup_ini_option(IniOption *optionList,
							  const char *section, const char *name);
IniOption * lookup_ini_path_value(IniOption *optionList, const char *path);
bool ini_merge(IniOption *dstOptionList, IniOption *overrideOptionList);

bool ini_set_option(IniOption *optionList, const char *path, char *value);

bool ini_get_setting(const char *filename, IniOption *optionList,
					 const char *path, char *value, size_t size);
bool ini_set_setting(const char *filename, IniOption *optionList,
					 const char *path, char *value);


#endif /* INI_FILE_H */
