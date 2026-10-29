#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fixture_transform_from_sql);
PG_FUNCTION_INFO_V1(fixture_transform_to_sql);

Datum
fixture_transform_from_sql(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}


Datum
fixture_transform_to_sql(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(0);
}
