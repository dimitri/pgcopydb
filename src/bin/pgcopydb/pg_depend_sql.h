/*
 * src/bin/pgcopydb/pg_depend_sql.h
 *	 SQL queries to discover the source database schema
 */

#ifndef PG_DEPEND_SQL_H
#define PG_DEPEND_SQL_H

/*
 * This is a very long query and will have to be repeated several times in the
 * source code, so we keep it as a #define here.
 *
 * See:
 *
 *  https://www.postgresql.org/message-id/flat/AANLkTimFbgNB9wpbkULSxOdzPRAUgj8TjxYVfWmgfaaE%40mail.gmail.com#af887127a3c3b5a3e24758764484c050
 *  https://github.com/gluefinance/pov/blob/master/sql/schema/pov/views/pg_depend_remapped.sql
 */

#define PG_DEPEND_SQL \
	"WITH RECURSIVE " \
	"edges AS ( " \
	"SELECT " \
	"    ARRAY[refclassid::int, refobjid::int, refobjsubid] AS from_obj, " \
	"    ARRAY[classid::int, objid::int, objsubid] AS to_obj, " \
	"    deptype " \
	"FROM pg_catalog.pg_depend " \
	"UNION " \
	"SELECT " \
	"    ARRAY[refclassid::int, refobjid::int, 0] AS from_obj, " \
	"    ARRAY[refclassid::int, refobjid::int, refobjsubid] AS to_obj, " \
	"    deptype " \
	"FROM pg_catalog.pg_depend WHERE refobjsubid > 0 " \
	"UNION " \
	"SELECT " \
	"    ARRAY[classid::int, objid::int, 0] AS from_obj, " \
	"    ARRAY[classid::int, objid::int, objsubid] AS to_obj, " \
	"    deptype " \
	"FROM pg_catalog.pg_depend WHERE objsubid > 0 " \
	"), " \
	"objects_with_internal_objects AS ( " \
	"SELECT from_obj AS obj FROM edges WHERE deptype = 'i' " \
	"EXCEPT " \
	"SELECT to_obj FROM edges WHERE deptype = 'i' " \
	"), " \
	"objects_without_internal_objects AS ( " \
	"SELECT from_obj AS obj FROM edges WHERE deptype IN ('n','a') " \
	"UNION " \
	"SELECT to_obj AS obj FROM edges WHERE deptype IN ('n','a') " \
	"EXCEPT " \
	"SELECT obj FROM objects_with_internal_objects " \
	"), " \
	"find_internal_recursively AS ( " \
	"SELECT " \
	"    objects_with_internal_objects.obj AS normal_obj, " \
	"    objects_with_internal_objects.obj AS internal_obj " \
	"FROM objects_with_internal_objects " \
	"UNION ALL " \
	"SELECT " \
	"    find_internal_recursively.normal_obj, " \
	"    edges.to_obj " \
	"FROM find_internal_recursively " \
	"JOIN edges ON (edges.deptype = 'i' AND edges.from_obj = find_internal_recursively.internal_obj) " \
	"), " \
	"remap_edges AS ( " \
	"SELECT " \
	"COALESCE(remap_from.normal_obj,edges.from_obj) AS from_obj, " \
	"COALESCE(remap_to.normal_obj,edges.to_obj) AS to_obj, " \
	"edges.deptype " \
	"FROM edges " \
	"LEFT JOIN find_internal_recursively AS remap_from ON (edges.from_obj = remap_from.internal_obj) " \
	"LEFT JOIN find_internal_recursively AS remap_to   ON (edges.to_obj   = remap_to.internal_obj) " \
	"WHERE edges.deptype IN ('n','a') " \
	"), " \
	"unconcat AS ( " \
	"SELECT " \
	"    from_obj[1]::oid AS refclassid, " \
	"    from_obj[2]::oid AS refobjid, " \
	"    from_obj[3]::integer AS refobjsubid, " \
	"    to_obj[1]::oid AS classid, " \
	"    to_obj[2]::oid AS objid, " \
	"    to_obj[3]::integer AS objsubid, " \
	"    deptype " \
	"FROM remap_edges " \
	") "


#endif  /* PG_DEPEND_SQL_H */
