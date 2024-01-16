/*
 * src/bin/pgcopydb/string_utils.h
 *   Utility functions for string handling
 */
#ifndef STRING_UTILS_H
#define STRING_UTILS_H

#include <stdbool.h>

#define IS_EMPTY_STRING_BUFFER(strbuf) (strbuf[0] == '\0')

#define NULL_AS_EMPTY_STRING(str) (str == NULL ? "" : str)

#define streq(a, b) (a != NULL && b != NULL && strcmp(a, b) == 0)

#define strneq(x, y) \
	((x != NULL) && (y != NULL) && (strcmp(x, y) != 0))

/* maximum decimal int64 length with minus and NUL */
#define INTSTRING_MAX_DIGITS 21
typedef struct IntString
{
	int64_t intValue;
	char strValue[INTSTRING_MAX_DIGITS];
} IntString;

IntString intToString(int64_t number);

bool stringToInt(const char *str, int *number);
bool stringToUInt(const char *str, unsigned int *number);

bool stringToInt64(const char *str, int64_t *number);
bool stringToUInt64(const char *str, uint64_t *number);

bool stringToShort(const char *str, short *number);
bool stringToUShort(const char *str, unsigned short *number);

bool stringToInt32(const char *str, int32_t *number);
bool stringToUInt32(const char *str, uint32_t *number);

bool stringToDouble(const char *str, double *number);

bool hexStringToUInt32(const char *str, uint32_t *number);

bool IntervalToString(uint64_t millisecs, char *buffer, size_t size);

typedef struct LinesBuffer
{
	bool ownsBuffer;
	char *buffer;
	uint64_t count;
	char **lines;                     /* malloc'ed area */
} LinesBuffer;

uint64_t countLines(char *buffer);
bool splitLines(LinesBuffer *lbuf, char *buffer, bool ownsBuffer);
void FreeLinesBuffer(LinesBuffer *lbuf);

void processBufferCallback(const char *buffer, bool error);

void pretty_print_bytes(char *buffer, size_t size, uint64_t bytes);
void pretty_print_bytes_per_second(char *buffer, size_t size, uint64_t bytes,
								   uint64_t durationMs);
void pretty_print_count(char *buffer, size_t size, uint64_t count);

#endif /* STRING_UTILS_h */
