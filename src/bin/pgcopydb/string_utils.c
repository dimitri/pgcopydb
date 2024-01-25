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
#include "parsing_utils.h"
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
 * converts given hexadecimal string to 32 bit unsigned int value.
 * returns 0 upon failure and sets error flag
 */
bool
hexStringToUInt32(const char *str, uint32_t *number)
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
	unsigned long long n = strtoull(str, &endptr, 16);

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
 * countLines returns how many line separators (\n) are found in the given
 * string.
 */
uint64_t
countLines(char *buffer)
{
	uint64_t lineNumber = 0;
	char *currentLine = buffer;

	if (buffer == NULL)
	{
		return 0;
	}

	do {
		char *newLinePtr = strchr(currentLine, '\n');

		if (newLinePtr == NULL)
		{
			/* strlen(currentLine) > 0 */
			if (*currentLine != '\0')
			{
				++lineNumber;
			}
			currentLine = NULL;
		}
		else
		{
			++lineNumber;
			currentLine = ++newLinePtr;
		}
	} while (currentLine != NULL && *currentLine != '\0');

	return lineNumber;
}


/*
 * splitLines splits a C-string "buffer" into a lines array. The buffer is
 * modified: \n are replaced with \0. Then the lines array is just an array of
 * pointers into the first character of each line found in the buffer memory
 * area.
 */
bool
splitLines(LinesBuffer *lbuf, char *buffer)
{
	lbuf->buffer = buffer;
	lbuf->count = countLines(lbuf->buffer);

	if (lbuf->buffer == NULL || lbuf->count == 0)
	{
		/* make sure we have a NULL lines in that case */
		lbuf->lines = NULL;
		return true;
	}

	lbuf->lines = (char **) calloc(lbuf->count, sizeof(char *));

	if (lbuf->lines == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	uint64_t lineNumber = 0;
	char *currentLine = lbuf->buffer;

	do {
		char *newLinePtr = strchr(currentLine, '\n');

		if (newLinePtr == NULL)
		{
			/* strlen(currentLine) > 0 */
			if (*currentLine != '\0')
			{
				lbuf->lines[lineNumber++] = currentLine;
			}

			currentLine = NULL;
		}
		else
		{
			*newLinePtr = '\0';

			lbuf->lines[lineNumber++] = currentLine;

			currentLine = ++newLinePtr;
		}
	} while (currentLine != NULL &&
			 *currentLine != '\0' &&
			 lineNumber < lbuf->count);

	return true;
}


/*
 * processBufferCallback is a function callback to use with the subcommands.c
 * library when we want to output a command's output as it's running, such as
 * when running a pg_basebackup command.
 */
void
processBufferCallback(const char *buffer, bool error)
{
	const char *warningPattern = "^(pg_dump: warning:|pg_restore: warning:)";
	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, (char *) buffer))
	{
		/* errors have already been logged */
		return;
	}

	for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
	{
		char *line = lbuf.lines[lineNumber];

		if (strneq(line, ""))
		{
			char *match = regexp_first_match(line, warningPattern);
			int logLevel = match != NULL ? LOG_WARN : (error ? LOG_ERROR : LOG_INFO);
			log_level(logLevel, "%s", line);
		}
	}
}


/*
 * pretty_print_bytes pretty prints bytes in a human readable form. Given
 * 17179869184 it places the string "16 GB" in the given buffer.
 */
void
pretty_print_bytes(char *buffer, size_t size, uint64_t bytes)
{
	const char *suffixes[7] = {
		"B",                    /* Bytes */
		"kB",                   /* Kilo */
		"MB",                   /* Mega */
		"GB",                   /* Giga */
		"TB",                   /* Tera */
		"PB",                   /* Peta */
		"EB"                    /* Exa */
	};

	uint sIndex = 0;
	long double count = bytes;

	while (count >= 10240 && sIndex < 7)
	{
		sIndex++;
		count /= 1024;
	}

	/* forget about having more precision, Postgres wants integers here */
	sformat(buffer, size, "%d %s", (int) count, suffixes[sIndex]);
}


/*
 * pretty_print_bytes_per_second pretty prints bytes transmitted per second in
 * a human readable form. Given 17179869184 it places the string
 * "17 GB/s" in the given buffer.
 *
 * Unlike pretty_print_bytes function that uses powers of 2, this function uses
 * powers of 10. So 1 GBit/s is 1,000,000,000 bits per second.
 */
void
pretty_print_bytes_per_second(char *buffer, size_t size, uint64_t bytes,
							  uint64_t durationMs)
{
	/* avoid division by zero */
	if (durationMs == 0)
	{
		sformat(buffer, size, "0 B/s");
		return;
	}

	const char *suffixes[7] = {
		"Bit/s",                    /* Bits per second */
		"kBit/s",                   /* Kilobits per second */
		"MBit/s",                   /* Megabits per second */
		"GBit/s",                   /* Gigabits per second */
		"TBit/s",                   /* Terabits per second */
		"PBit/s",                   /* Petabits per second */
		"EBit/s"                    /* Exabits per second */
	};

	uint sIndex = 0;
	long double count = ((long double) bytes) * 1000 * 8 / durationMs;

	while (count >= 10000 && sIndex < 7)
	{
		sIndex++;
		count /= 1000;
	}

	/* forget about having more precision, Postgres wants integers here */
	sformat(buffer, size, "%d %s", (int) count, suffixes[sIndex]);
}


/*
 * pretty_print_bytes pretty prints bytes in a human readable form. Given
 * 17179869184 it places the string "17 billion" in the given buffer.
 */
void
pretty_print_count(char *buffer, size_t size, uint64_t number)
{
	const char *suffixes[7] = {
		"",                     /* units */
		"thousands",            /* 10^3 */
		"million",              /* 10^6 */
		"billion",              /* 10^9 */
		"trillion",             /* 10^12 */
		"quadrillion",          /* 10^15 */
		"quintillion"           /* 10^18 */
	};

	if (number < 1000)
	{
		sformat(buffer, size, "%lld", (long long) number);
	}
	else if (number < (1000 * 1000))
	{
		int t = number / 1000;
		int u = number - (t * 1000);

		sformat(buffer, size, "%d %d", t, u);
	}
	else
	{
		uint sIndex = 0;
		long double count = number;

		/* issue 1234 million rather than 1 billion or 1.23 billion */
		while (count >= 10000 && sIndex < 7)
		{
			sIndex++;
			count /= 1000;
		}

		sformat(buffer, size, "%d %s", (int) count, suffixes[sIndex]);
	}
}
