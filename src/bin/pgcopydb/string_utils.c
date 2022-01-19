/*
 * src/bin/pgcopydb/string_utils.c
 *   Implementations of utility functions for string handling
 */

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <float.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "postgres_fe.h"
#include "pqexpbuffer.h"

#include "defaults.h"
#include "file_utils.h"
#include "log.h"
#include "string_utils.h"

/*
 * intToString converts an int to an IntString, which contains a decimal string
 * representation of the integer.
 */
IntString
intToString(int64_t number)
{
	IntString intString;

	intString.intValue = number;

	sformat(intString.strValue, INTSTRING_MAX_DIGITS, "%" PRId64, number);

	return intString;
}


/*
 * converts given string to 64 bit integer value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToInt(const char *str, int *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;

	long long int n = strtoll(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n < INT_MIN || n > INT_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to 64 bit integer value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToInt64(const char *str, int64_t *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;

	long long int n = strtoll(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n < INT64_MIN || n > INT64_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to 64 bit unsigned integer value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToUInt(const char *str, unsigned int *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;
	unsigned long long n = strtoull(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n > UINT_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to 64 bit unsigned integer value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToUInt64(const char *str, uint64_t *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;
	unsigned long long n = strtoull(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n > UINT64_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to short value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToShort(const char *str, short *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;

	long long int n = strtoll(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n < SHRT_MIN || n > SHRT_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to unsigned short value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToUShort(const char *str, unsigned short *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;
	unsigned long long n = strtoull(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n > USHRT_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to 32 bit integer value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToInt32(const char *str, int32_t *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;

	long long int n = strtoll(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n < INT32_MIN || n > INT32_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to 32 bit unsigned int value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToUInt32(const char *str, uint32_t *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;
	unsigned long long n = strtoull(str, &endptr, 10);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n > UINT32_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * converts given string to a double precision float value.
 * returns 0 upon failure and sets error flag
 */
bool
stringToDouble(const char *str, double *number)
{
	char *endptr;

	if (str == NULL)
	{
		return false;
	}

	if (number == NULL)
	{
		return false;
	}

	errno = 0;
	double n = strtod(str, &endptr);

	if (str == endptr)
	{
		return false;
	}
	else if (errno != 0)
	{
		return false;
	}
	else if (*endptr != '\0')
	{
		return false;
	}
	else if (n > DBL_MAX)
	{
		return false;
	}

	*number = n;

	return true;
}


/*
 * IntervalToString prepares a string buffer to represent a given interval
 * value given as a double precision float number.
 */
bool
IntervalToString(uint64_t millisecs, char *buffer, size_t size)
{
	double seconds = millisecs / 1000.0;

	if (millisecs < 1000)
	{
		sformat(buffer, size, "%3lldms", (long long) millisecs);
	}
	else if (seconds < 10.0)
	{
		int s = (int) seconds;
		uint64_t ms = millisecs - (1000 * s);

		sformat(buffer, size, "%2ds%03lld", s, (long long) ms);
	}
	else if (seconds < 60.0)
	{
		int s = (int) seconds;

		sformat(buffer, size, "%2ds", s);
	}
	else if (seconds < (60.0 * 60.0))
	{
		int mins = (int) (seconds / 60.0);
		int secs = (int) (seconds - (mins * 60.0));

		sformat(buffer, size, "%2dm%02ds", mins, secs);
	}
	else if (seconds < (24.0 * 60.0 * 60.0))
	{
		int hours = (int) (seconds / (60.0 * 60.0));
		int mins = (int) ((seconds - (hours * 60.0 * 60.0)) / 60.0);

		sformat(buffer, size, "%2dh%02dm", hours, mins);
	}
	else
	{
		long days = (long) (seconds / (24.0 * 60.0 * 60.0));
		long hours =
			(long) ((seconds - (days * 24.0 * 60.0 * 60.0)) / (60.0 * 60.0));

		sformat(buffer, size, "%2ldd%02ldh", days, hours);
	}

	return true;
}


/*
 * splitLines prepares a multi-line error message in a way that calling code
 * can loop around one line at a time and call log_error() or log_warn() on
 * individual lines.
 */
int
splitLines(char *errorMessage, char **linesArray, int size)
{
	int lineNumber = 0;
	char *currentLine = errorMessage;

	if (errorMessage == NULL)
	{
		return 0;
	}

	do {
		char *newLinePtr = strchr(currentLine, '\n');

		if (newLinePtr == NULL)
		{
			if (strlen(currentLine) > 0)
			{
				linesArray[lineNumber++] = currentLine;
			}

			currentLine = NULL;
		}
		else
		{
			*newLinePtr = '\0';

			linesArray[lineNumber++] = currentLine;

			currentLine = ++newLinePtr;
		}
	} while (currentLine != NULL && *currentLine != '\0' && lineNumber < size);

	return lineNumber;
}


/*
 * processBufferCallback is a function callback to use with the subcommands.c
 * library when we want to output a command's output as it's running, such as
 * when running a pg_basebackup command.
 */
void
processBufferCallback(const char *buffer, bool error)
{
	char *outLines[BUFSIZE] = { 0 };
	int lineCount = splitLines((char *) buffer, outLines, BUFSIZE);
	int lineNumber = 0;

	int logLevel = error ? LOG_ERROR : LOG_INFO;

	for (lineNumber = 0; lineNumber < lineCount; lineNumber++)
	{
		if (strneq(outLines[lineNumber], ""))
		{
			log_level(logLevel, "%s", outLines[lineNumber]);
		}
	}
}
