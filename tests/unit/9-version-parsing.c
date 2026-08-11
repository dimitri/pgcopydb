/*
 * Unit test for version parsing in pgcopydb (parsing_utils.c)
 * Tests parsing of version strings:
 *   - 16.3
 *   - 18.1
 *   - 16.14.0
 *   - 18.3.10
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "parsing_utils.h"

char pgcopydb_argv0[1024] = "9-version-parsing";
char *pgcopydb_cmdline = "9-version-parsing";
char ps_buffer[1024] = { 0 };
size_t ps_buffer_size = 1024;
size_t last_status_len = 0;
void *system_res_array = NULL;
void *log_semaphore = NULL;



int
main(int argc, char **argv)
{
	struct TestCase {
		const char *input;
		int expected_version;
	} test_cases[] = {
		{ "16.3", 1603 },
		{ "18.1", 1801 },
		{ "16.14.0", 1614 },
		{ "18.3.10", 1803 },
		{ NULL, 0 }
	};

	int failed = 0;
	int count = 0;

	printf("=== Running Version Parsing Unit Tests ===\n");

	for (int i = 0; test_cases[i].input != NULL; i++)
	{
		count++;
		int parsed_ver = 0;
		char parsed_str[64] = { 0 };

		bool ok = parse_dotted_version_string(test_cases[i].input, &parsed_ver);
		if (!ok)
		{
			printf("[FAIL] parse_dotted_version_string(\"%s\") returned false\n",
				   test_cases[i].input);
			failed++;
			continue;
		}

		if (parsed_ver != test_cases[i].expected_version)
		{
			printf("[FAIL] parse_dotted_version_string(\"%s\") = %d, expected %d\n",
				   test_cases[i].input, parsed_ver, test_cases[i].expected_version);
			failed++;
			continue;
		}

		bool ok_num = parse_version_number(test_cases[i].input, parsed_str, sizeof(parsed_str), &parsed_ver);
		if (!ok_num)
		{
			printf("[FAIL] parse_version_number(\"%s\") returned false\n",
				   test_cases[i].input);
			failed++;
			continue;
		}

		printf("[PASS] Version \"%s\" -> parsed number: %d, parsed string: \"%s\"\n",
			   test_cases[i].input, parsed_ver, parsed_str);
	}

	if (failed > 0)
	{
		printf("\nFAILURE: %d out of %d test(s) failed.\n", failed, count);
		return 1;
	}

	printf("\nSUCCESS: All %d version parsing tests passed successfully!\n", count);
	return 0;
}
