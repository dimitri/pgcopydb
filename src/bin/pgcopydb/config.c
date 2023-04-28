/*
 * src/bin/pgcopydb/config.c
 *     Configuration functions for pgcopydb.
 *
 * Licensed under the PostgreSQL License.
 *
 */

#include <inttypes.h>
#include <stdbool.h>
#include <unistd.h>

#include "config.h"
#include "defaults.h"
#include "ini_file.h"
#include "log.h"
#include "parsing_utils.h"

#define OPTION_PGCOPYDB_DIR(config) \
	make_strbuf_option("pgcopydb", "dir", "dir", true, MAXCONNINFO, \
					   config->dir)

#define OPTION_PGCOPYDB_SOURCE(config) \
	make_strbuf_option("pgcopydb", "source", "source", true, MAXCONNINFO, \
					   config->source_pguri)

#define OPTION_PGCOPYDB_TARGET(config) \
	make_strbuf_option("pgcopydb", "target", "source", true, MAXCONNINFO, \
					   config->target_pguri)

#define OPTION_PGCOPYDB_TABLE_JOBS(config) \
	make_int_option_default("pgcopydb", "table-jobs", "table-jobs", \
							true, &(config->tableJobs), \
							DEFAULT_TABLE_JOBS)

#define OPTION_PGCOPYDB_INDEX_JOBS(config) \
	make_int_option_default("pgcopydb", "index-jobs", "index-jobs", \
							true, &(config->indexJobs), \
							DEFAULT_INDEX_JOBS)

#define SET_INI_OPTIONS_ARRAY(config) \
	{ \
		OPTION_PGCOPYDB_DIR(config), \
		OPTION_PGCOPYDB_SOURCE(config), \
		OPTION_PGCOPYDB_TARGET(config), \
		OPTION_PGCOPYDB_TABLE_JOBS(config), \
		OPTION_PGCOPYDB_INDEX_JOBS(config), \
		INI_OPTION_LAST \
	}


/*
 * config_init initializes a CopyDBOptions with the default values.
 */
void
config_init(CopyDBOptions *config)
{
	IniOption options[] = SET_INI_OPTIONS_ARRAY(config);

	log_trace("config_init");

	if (!ini_validate_options(options))
	{
		log_error("Please review your setup options per above messages");
		exit(EXIT_CODE_BAD_CONFIG);
	}
}


/*
 * config_read_file overrides values in given CopyDBOptions with whatever
 * values are read from given configuration filename.
 */
bool
config_read_file(CopyDBOptions *config, const char *filename)
{
	IniOption options[] = SET_INI_OPTIONS_ARRAY(config);

	log_debug("Reading configuration from %s", filename);

	if (!read_ini_file(filename, options))
	{
		log_error("Failed to parse configuration file \"%s\"", filename);
		return false;
	}

	return true;
}


/*
 * config_write_file writes the current values in given CopyDBOptions to
 * filename.
 */
bool
config_write_file(CopyDBOptions *config, const char *filename)
{
	log_trace("config_write_file \"%s\"", filename);

	FILE *fileStream = fopen_with_umask(filename, "w", FOPEN_FLAGS_W, 0644);
	if (fileStream == NULL)
	{
		/* errors have already been logged */
		return false;
	}

	bool success = config_write(fileStream, config);

	if (fclose(fileStream) == EOF)
	{
		log_error("Failed to write file \"%s\"", filename);
		return false;
	}

	return success;
}


/*
 * config_write write the current config to given STREAM.
 */
bool
config_write(FILE *stream, CopyDBOptions *config)
{
	IniOption options[] = SET_INI_OPTIONS_ARRAY(config);

	return write_ini_to_stream(stream, options);
}


/*
 * config_to_json populates given jsRoot object with the INI configuration
 * sections as JSON objects, and the options as keys to those objects.
 */
bool
config_to_json(CopyDBOptions *config, JSON_Value *js)
{
	JSON_Object *jsRoot = json_value_get_object(js);
	IniOption options[] = SET_INI_OPTIONS_ARRAY(config);

	return ini_to_json(jsRoot, options);
}


/*
 * config_log_settings outputs a DEBUG line per each config parameter in the
 * given CopyDBOptions.
 */
void
config_log_settings(CopyDBOptions *config)
{
	log_debug("pgcopydb.dir: %s", config->dir);
	log_debug("pgcopydb.source_pguri: %s", config->source_pguri);
	log_debug("pgcopydb.target_pguri: %s", config->target_pguri);

	log_debug("pgcopydb.table-jobs: %d", config->tableJobs);
	log_debug("pgcopydb.index-jobs: %d", config->indexJobs);
}


/*
 * config_get_setting returns the current value of the given option "path"
 * (thats a section.option string). The value is returned in the pre-allocated
 * value buffer of size size.
 */
bool
config_get_setting(CopyDBOptions *config,
				   const char *filename,
				   const char *path,
				   char *value, size_t size)
{
	IniOption options[] = SET_INI_OPTIONS_ARRAY(config);

	return ini_get_setting(filename, options, path, value, size);
}


/*
 * config_set_setting sets the setting identified by "path" (section.option) to
 * the given value. The value is passed in as a string, which is going to be
 * parsed if necessary.
 */
bool
config_set_setting(CopyDBOptions *config,
				   const char *filename,
				   const char *path,
				   char *value)
{
	IniOption options[] = SET_INI_OPTIONS_ARRAY(config);

	log_trace("config_set_setting: %s = %s", path, value);

	if (!ini_set_setting(filename, options, path, value))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * config_merge_options merges any option setup in options into config. Its
 * main use is to override configuration file settings with command line
 * options.
 */
bool
config_merge_options(CopyDBOptions *config, CopyDBOptions *options,
					 const char *filename)
{
	IniOption configOptions[] = SET_INI_OPTIONS_ARRAY(config);
	IniOption optionsOptions[] = SET_INI_OPTIONS_ARRAY(options);

	log_trace("config_merge_options");

	if (ini_merge(configOptions, optionsOptions))
	{
		return config_write_file(config, filename);
	}

	return false;
}
