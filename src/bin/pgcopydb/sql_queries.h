/*
 * GENERATED FILE - do not edit.
 * Source: src/bin/pgcopydb/sql/
 * Run 'make gen-sql' after modifying .sql files.
 */

#ifndef PGCOPYDB_SQL_QUERIES_H
#define PGCOPYDB_SQL_QUERIES_H

#include <stdbool.h>

/*////////////////////////////////////////////////////////////// */

/* list_source_depend                                           */
/*////////////////////////////////////////////////////////////// */

bool pgcopydb_sql_list_source_depend(int filter,
									 const char **sql);

/*////////////////////////////////////////////////////////////// */

/* list_source_indexes                                          */
/*////////////////////////////////////////////////////////////// */

bool pgcopydb_sql_list_source_indexes(int filter,
									  const char **sql);

/*////////////////////////////////////////////////////////////// */

/* list_source_sequences                                        */
/*////////////////////////////////////////////////////////////// */

bool pgcopydb_sql_list_source_sequences(int filter,
										const char **sql);

/*////////////////////////////////////////////////////////////// */

/* list_source_table_size                                       */
/*////////////////////////////////////////////////////////////// */

bool pgcopydb_sql_list_source_table_size(int filter,
										 const char **sql);

/*////////////////////////////////////////////////////////////// */

/* list_source_tables                                           */
/*////////////////////////////////////////////////////////////// */

bool pgcopydb_sql_list_source_tables(int pg_version,
									 int filter,
									 const char **sql);

#endif  /* PGCOPYDB_SQL_QUERIES_H */
