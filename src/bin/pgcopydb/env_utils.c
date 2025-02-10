/*
 * src/bin/pgcopydb/env_utils.c
 *   Utility functions for interacting with environment settings.
 */

#include <stdlib.h>
#include <string.h>

#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "parsing_utils.h"
#include "string_utils.h"
#include "log.h"
#include "pqexpbuffer.h"

static bool get_env_using_parser(EnvParser *parser);
static bool get_env_value_using_parser(char *envValue, EnvParser *parser);
static bool process_env_line(void *ctx, char *line);

/*
 * env_exists returns true if the passed environment variable exists in the
 * environment, otherwise it returns false.
 */
bool
env_exists(const char *name)
{
	if (name == NULL || strlen(name) == 0)
	{
		log_error("Failed to get environment setting. "
				  "NULL or empty variable name is provided");
		return false;
	}

	/*
	 * Explanation of IGNORE-BANNED
	 * getenv is safe here because we never provide null argument,
	 * and only check if it returns NULL.
	 */
	return getenv(name) != NULL; /* IGNORE-BANNED */
}


/*
 * get_env_copy_with_fallback copies the environment variable with "name" into
 * the result buffer. It returns false when it fails. If the environment
 * variable is not set the fallback string will be written in the buffer.
 * Except when fallback is NULL, in that case an error is returned.
 */
bool
get_env_copy_with_fallback(const char *name, char *result, int maxLength,
						   const char *fallback)
{
	if (name == NULL || strlen(name) == 0)
	{
		log_error("Failed to get environment setting. "
				  "NULL or empty variable name is provided");
		return false;
	}

	if (result == NULL)
	{
		log_error("Failed to get environment setting. "
				  "Tried to store in NULL pointer");
		return false;
	}

	/*
	 * Explanation of IGNORE-BANNED
	 * getenv is safe here because we never provide null argument,
	 * and copy out the result immediately.
	 */
	const char *envvalue = getenv(name); /* IGNORE-BANNED */
	if (envvalue == NULL)
	{
		envvalue = fallback;
		if (envvalue == NULL)
		{
			log_error("Failed to get value for environment variable '%s', "
					  "which is unset", name);
			return false;
		}
	}

	size_t actualLength = strlcpy(result, envvalue, maxLength);

	/* uses >= to make sure the nullbyte fits */
	if (actualLength >= maxLength)
	{
		log_error("Failed to copy value stored in %s environment setting, "
				  "which is %zu long. pgcopydb only supports %d bytes for "
				  "this environment setting",
				  name,
				  actualLength,
				  maxLength - 1);
		return false;
	}
	return true;
}


/*
 * get_env_dup_with_fallback copies the environment variable with "name" into
 * the result buffer using strdup. It returns false when it fails. If the environment
 * variable is not set the fallback string will be written in the buffer.
 * Except when fallback is NULL, in that case an error is returned.
 */
bool
get_env_dup_with_fallback(const char *name, char **result, const char *fallback)
{
	if (name == NULL || strlen(name) == 0)
	{
		log_error("Failed to get environment setting. "
				  "NULL or empty variable name is provided");
		return false;
	}

	/*
	 * Explanation of IGNORE-BANNED
	 * getenv is safe here because we never provide null argument,
	 * and copy out the result immediately.
	 */
	const char *envvalue = getenv(name); /* IGNORE-BANNED */
	if (envvalue == NULL)
	{
		envvalue = fallback;
		if (envvalue == NULL)
		{
			log_error("Failed to get value for environment variable '%s', "
					  "which is unset", name);
			return false;
		}
	}
	*result = strdup(envvalue);
	if (*result == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	return true;
}


/*
 * get_env_dup copies the environment variable with "name" into the result
 * buffer using strdup. It returns false when it fails. The environment variable not
 * existing is also considered a failure.
 */
bool
get_env_dup(const char *name, char **result)
{
	return get_env_dup_with_fallback(name, result, NULL);
}


/*
 * get_env_copy copies the environment variable with "name" into tho result
 * buffer. It returns false when it fails. The environment variable not
 * existing is also considered a failure.
 */
bool
get_env_copy(const char *name, char *result, int maxLength)
{
	return get_env_copy_with_fallback(name, result, maxLength, NULL);
}


/*
 * process_env_line parses and processes a single line from .env file
 */
static bool
process_env_line(void *ctx, char *line)
{
	EnvParserArray *parsers = (EnvParserArray *) ctx;
	if (line[0] == '#')
	{
		return true;
	}

	/* split the line into key and value */
	const char *key = line;
	char *value = strchr(line, '=');
	if (value == NULL)
	{
		return true;
	}
	*value = 0;
	value++;
	if (*value == '\n')
	{
		return true;
	}

	/* remove the newline character from the value */
	value[strcspn(value, "\n ")] = 0;

	for (int i = 0; i < parsers->count; i++)
	{
		if (strcmp(key, parsers->array[i].envname) == 0)
		{
			return get_env_value_using_parser(value, &parsers->array[i]);
		}
	}

	return true;
}


/*
 * get_env_using_parsers_from_file reads the environment variables from
 * XDG_CONFIG_HOME/pgcopydb/.env file and uses the parsers to parse them.
 */
bool
get_env_using_parsers_from_file(EnvParserArray *parsers)
{
	char envFilePath[BUFSIZE];
	if (env_exists("XDG_CONFIG_HOME"))
	{
		char *configHome;
		get_env_dup("XDG_CONFIG_HOME", &configHome);
		sformat(envFilePath, sizeof(envFilePath), "%s/pgcopydb/.env", configHome);
	}
	else if (env_exists("HOME"))
	{
		char *homeDir;
		get_env_dup("HOME", &homeDir);
		sformat(envFilePath, sizeof(envFilePath), "%s/.config/pgcopydb/.env", homeDir);
	}
	else
	{
		log_info("No config home path found");
		return true;
	}

	if (!file_exists(envFilePath))
	{
		log_info("No %s file found", envFilePath);
		return true;
	}

	if (!file_iter_lines(envFilePath, BUFSIZE, parsers, process_env_line))
	{
		return false;
	}

	return true;
}


/*
 * get_env_using_parsers iterates over the parsers array and calls
 * get_env_using_parser for each parser.
 */
bool
get_env_using_parsers(EnvParserArray *parsers)
{
	int errors = 0;

	for (int i = 0; i < parsers->count; i++)
	{
		if (!get_env_using_parser(&parsers->array[i]))
		{
			++errors;
		}
	}

	return errors == 0;
}


/*
 * get_env_value_using_parser processes a single string according to
 * the given parser.
 */
static bool
get_env_value_using_parser(char *envValue, EnvParser *parser)
{
	switch (parser->type)
	{
		case ENV_TYPE_INT:
		{
			if (!stringToInt(envValue, (int *) parser->target) ||
				(parser->lowerBounded &&
				 *((int *) parser->target) < parser->minValue) ||
				(parser->upperBounded &&
				 *((int *) parser->target) > parser->maxValue))
			{
				PQExpBuffer errorMessage = createPQExpBuffer();

				appendPQExpBuffer(errorMessage,
								  "Failed to parse \"%s\": \"%s\", "
								  "expected an integer",
								  parser->envname,
								  envValue);

				if (parser->lowerBounded)
				{
					appendPQExpBuffer(errorMessage,
									  " >= %d",
									  parser->minValue);
				}

				if (parser->lowerBounded && parser->upperBounded)
				{
					appendPQExpBuffer(errorMessage, " and");
				}

				if (parser->upperBounded)
				{
					appendPQExpBuffer(errorMessage,
									  " <= %d",
									  parser->maxValue);
				}

				log_fatal("%s", errorMessage->data);

				destroyPQExpBuffer(errorMessage);
				return false;
			}
			break;
		}

		case ENV_TYPE_BOOL:
		{
			if (!parse_bool(envValue, (bool *) parser->target))
			{
				log_fatal("Failed to parse \"%s\": \"%s\", "
						  "expected a boolean (on/off)",
						  parser->envname, envValue);
				return false;
			}
			break;
		}

		case ENV_TYPE_STRING:
		{
			size_t actualLength = strlcpy((char *) parser->target,
										  envValue,
										  parser->targetSize);

			/* uses >= to make sure the nullbyte fits */
			if (actualLength >= parser->targetSize)
			{
				log_fatal("Failed to copy value stored in %s environment setting, "
						  "which is %zu long. pgcopydb only supports %d bytes for "
						  "this environment setting",
						  parser->envname,
						  actualLength,
						  parser->targetSize - 1);
				return false;
			}
			break;
		}

		case ENV_TYPE_STR_PTR:
		{
			/* Duplicate the new value and assign it to the target */
			*(char **) (parser->target) = strdup(envValue);
			if (*(char **) (parser->target) == NULL)
			{
				log_fatal("Failed to allocate memory for environment setting %s",
						  parser->envname);
				return false;
			}
			break;
		}
	}
	return true;
}


/*
 * get_env_using_parser checks if the environment variable exists and if it does
 * it parses it and stores it in the target. If the environment variable is not
 * set, the target is not modified. If the environment variable is set but the
 * parsing fails, an error is logged and the errors counter is incremented.
 */
static bool
get_env_using_parser(EnvParser *parser)
{
	if (!env_exists(parser->envname))
	{
		return true;
	}

	switch (parser->type)
	{
		case ENV_TYPE_INT:
		case ENV_TYPE_BOOL:
		{
			char envValue[BUFSIZE] = { 0 };
			if (!get_env_copy(parser->envname, envValue, sizeof(envValue)))
			{
				/* errors have already been logged */
				return false;
			}
			else if (!get_env_value_using_parser(envValue, parser))
			{
				/* errors have already been logged */
				return false;
			}
			break;
		}

		case ENV_TYPE_STRING:
		{
			if (!get_env_copy(parser->envname,
							  parser->target,
							  parser->targetSize))
			{
				/* errors have already been logged */
				return false;
			}
			break;
		}

		case ENV_TYPE_STR_PTR:
		{
			char *envValue = NULL;
			if (!get_env_dup(parser->envname, &envValue))
			{
				/* errors have already been logged */
				return false;
			}
			else
			{
				*(char **) parser->target = envValue;
			}
			break;
		}

		default:
		{
			log_fatal("Unknown parser type %d", parser->type);
			return false;
			break;
		}
	}

	return true;
}
