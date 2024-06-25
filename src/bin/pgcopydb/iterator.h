#ifndef ITERATOR_H
#define ITERATOR_H

#include <stdbool.h>

struct Iterator;

/* Function pointer to initialize the iterator */
typedef bool (IterInit)(struct Iterator *);

/* Define the iterator structure */
typedef struct Iterator
{
	void *data;                         /* Pointer to the iterator-specific data */
	IterInit *init;   /* Function pointer to initialize the iterator */
	bool (*has_next)(struct Iterator *);   /* Function pointer to get the next element */
	bool (*next)(struct Iterator *);   /* Function pointer to get the next element */
	bool (*finish)(struct Iterator *); /* Function pointer to destroy the iterator */
} Iterator;

/* Define the callback function */
typedef bool (IterCallback)(void *context, void *item);

/* Iterate over the iterator and call the callback function for each item. */
bool for_each(Iterator *iter, void *context, IterCallback *callback);


#endif /* ITERATOR_H */
