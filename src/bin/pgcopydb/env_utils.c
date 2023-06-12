/*
 * src/bin/pgcopydb/env_utils.c
 *   Utility functions for interacting with environment settings.
 */

#include <stdlib.h>
#include <string.h>

#include "defaults.h"
#include "env_utils.h"
#include "log.h"


/*
 * env_found_empty returns true if the passed environment variable is the empty
 * string. It returns false when the environment variable is not set or if it
 * set but is something else than the empty string.
 */
bool
env_found_empty(const char *name)
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
	 * and only check the value it's length.
	 */
	char *envvalue = getenv(name); /* IGNORE-BANNED */
	return envvalue != NULL && strlen(envvalue) == 0;
}


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
				  "which is %lu long. pgcopydb only supports %lu bytes for "
				  "this environment setting",
				  name,
				  (unsigned long) actualLength,
				  (unsigned long) maxLength - 1);
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
 * get_env_dup copies the environmennt variable with "name" into the result
 * buffer using strdup. It returns false when it fails. The environment variable not
 * existing is also considered a failure.
 */
bool
get_env_dup(const char *name, char **result)
{
	return get_env_dup_with_fallback(name, result, NULL);
}


/*
 * get_env_copy copies the environmennt variable with "name" into tho result
 * buffer. It returns false when it fails. The environment variable not
 * existing is also considered a failure.
 */
bool
get_env_copy(const char *name, char *result, int maxLength)
{
	return get_env_copy_with_fallback(name, result, maxLength, NULL);
}


/*
 * get_env_pgdata checks for environment value PGDATA
 * and copy its value into provided buffer.
 *
 * function returns true on successful run. returns false
 * if it can't find PGDATA or its value is larger than
 * the provided buffer
 */
bool
get_env_pgdata(char *pgdata)
{
	return get_env_copy("PGDATA", pgdata, MAXPGPATH) > 0;
}


/*
 * get_env_pgdata_or_exit does the same as get_env_pgdata. Instead of
 * returning false in case of error it exits the process and shows a FATAL log
 * message.
 */
void
get_env_pgdata_or_exit(char *pgdata)
{
	if (get_env_pgdata(pgdata))
	{
		return;
	}
	log_fatal("Failed to set PGDATA either from the environment "
			  "or from --pgdata");
	exit(EXIT_CODE_BAD_ARGS);
}
