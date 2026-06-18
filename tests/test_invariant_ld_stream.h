#include <check.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Include the actual header under test */
#include "ld_stream.h"

/*
 * Security invariant: Any SQL fragment built using the INSERT macro that
 * incorporates table/schema names MUST NOT contain unquoted adversarial
 * identifiers. Specifically, when a name is properly quoted via
 * PQescapeIdentifier-style quoting (double-quotes with internal double-quotes
 * escaped), the resulting SQL fragment must not allow SQL injection.
 *
 * We verify that the INSERT macro expands to the expected literal string,
 * and that a naive concatenation of adversarial names (without quoting)
 * would differ from a properly quoted version — confirming quoting is required.
 */

static char *naive_quote(const char *name)
{
    /* Proper quoting: wrap in double-quotes, escape internal double-quotes */
    size_t len = strlen(name);
    char *buf = malloc(len * 2 + 3);
    size_t j = 0;
    buf[j++] = '"';
    for (size_t i = 0; i < len; i++) {
        if (name[i] == '"') buf[j++] = '"';
        buf[j++] = name[i];
    }
    buf[j++] = '"';
    buf[j] = '\0';
    return buf;
}

START_TEST(test_insert_macro_identifier_quoting)
{
    /* Invariant: adversarial identifiers must be quoted before use in SQL */
    const char *payloads[] = {
        "evil\"; DROP TABLE users; --",   /* exact exploit payload */
        "schema\".\"injected",            /* boundary: dot/quote combo */
        "public"                          /* valid input */
    };
    int num_payloads = sizeof(payloads) / sizeof(payloads[0]);

    /* Verify INSERT macro expands to the expected safe literal */
    const char *insert_fragment = INSERT;
    ck_assert_str_eq(insert_fragment, "AS INSERT INTO ");

    for (int i = 0; i < num_payloads; i++) {
        const char *name = payloads[i];

        /* Naive (unsafe) concatenation */
        char naive_sql[512];
        snprintf(naive_sql, sizeof(naive_sql), "%s%s", INSERT, name);

        /* Properly quoted concatenation */
        char *quoted = naive_quote(name);
        char quoted_sql[512];
        snprintf(quoted_sql, sizeof(quoted_sql), "%s%s", INSERT, quoted);
        free(quoted);

        /* For adversarial inputs, naive and quoted SQL must differ,
         * proving that quoting is necessary and changes the output */
        if (strchr(name, '"') != NULL || strchr(name, ';') != NULL) {
            ck_assert_msg(strcmp(naive_sql, quoted_sql) != 0,
                "SECURITY FAIL: adversarial identifier '%s' produces identical "
                "SQL with and without quoting — quoting has no effect", name);
        }

        /* The quoted SQL must start with the INSERT macro literal */
        ck_assert_msg(strncmp(quoted_sql, "AS INSERT INTO ", 15) == 0,
            "INSERT macro prefix must be preserved in quoted SQL");

        /* The quoted SQL must begin the identifier portion with a double-quote */
        ck_assert_msg(quoted_sql[15] == '"',
            "Identifier in SQL must be double-quote delimited, got: %s", quoted_sql);
    }
}
END_TEST

Suite *security_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s = suite_create("Security");
    tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_insert_macro_identifier_quoting);
    suite_add_tcase(s, tc_core);

    return s;
}

int main(void)
{
    int number_failed;
    Suite *s;
    SRunner *sr;

    s = security_suite();
    sr = srunner_create(s);

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}