
#include "iterator.h"
#include "log.h"

bool
for_each(Iterator *iter, void *context, IterCallback *callback)
{
	if (!iter->init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!iter->next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		if (!iter->has_next(iter))
		{
			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, iter->data))
		{
			log_error("Failed to iterate over list of tables, "
					  "see above for details");
			return false;
		}
	}

	return iter->finish(iter);
}
