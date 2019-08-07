#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "dict.h"
#include "xxhash.h"

#define GROWTH_FACTOR (2)
#define MAX_LOAD_FACTOR (0.80)

//struct elt elt_list[INITIAL_SIZE];
//struct elt *pelt_list[INITIAL_SIZE];

//struct dict *d=NULL;
//struct elt *elt_list=NULL;
//int *ht_table=NULL;

// Totoal size = sizeof(dict) + sizeof(struct elt *)*INITIAL_SIZE + sizeof(struct elt)*INITIAL_SIZE
// memory region three parts. 1) dict ( sizeof(dict) ) 2) pelt_list[] ( sizeof(struct elt *)*INITIAL_SIZE ) 3) elt_list ( sizeof(struct elt)*INITIAL_SIZE )

void Init_Pointers(Dict d, struct elt ** p_elt_list, int ** p_ht_table)
{
//    ht_table = (int *)((void *)d + sizeof(struct dict));
//	  elt_list = (struct elt *)((void *)d + sizeof(struct dict) + sizeof(int)*(d->size));
    *p_ht_table = (int *)((void *)d + sizeof(struct dict));
	  *p_elt_list = (struct elt *)((void *)d + sizeof(struct dict) + sizeof(int)*(d->size));
}

void DictCreate(Dict d, int nSize, struct elt ** p_elt_list, int ** p_ht_table)
{
  int i;

	if(d == NULL)	{
		printf("d = NULL.\nThe memory for hash table is not allocated.\nQuit\n");
		exit(1);
	}

  if(nSize) {
	  if(nSize) {
	    d->size = nSize;
	  }
	  else {
	    d->size = INITIAL_SIZE;
	  }
	  d->n = 0;
  }
  Init_Pointers(d, p_elt_list, p_ht_table);

//    ht_table = (int *)((void *)d + sizeof(dict));
//	elt_list = (struct elt *)((void *)d + sizeof(struct dict) + sizeof(int)*INITIAL_SIZE);

  if(nSize) for(i = 0; i < d->size; i++) (*p_ht_table)[i] = -1;
}

/*
static void grow(Dict d)
{
    Dict d2;            // new dictionary we'll create 
    struct dict swap;   // temporary structure for brain transplant
    int i;
    struct elt *e;

    d2 = internalDictCreate(d->size * GROWTH_FACTOR);

    for(i = 0; i < d->size; i++) {
        for(e = d->table[i]; e != 0; e = e->next) {
            //  note: this recopies everything 
            //  a more efficient implementation would
            // patch out the strdups inside DictInsert
            // to avoid this problem 
            DictInsert(d2, e->key, e->value);
        }
    }

    // the hideous part
    // We'll swap the guts of d and d2
    // then call DictDestroy on d2
    swap = *d;
    *d = *d2;
    *d2 = swap;

    DictDestroy(d2);
}
*/

// insert a new key-value pair into an existing dictionary 
void DictInsert(Dict d, const char *key, const int value, struct elt ** p_elt_list, int ** p_ht_table)
{
    struct elt *e;
    unsigned long long h;

    assert(key);

	  e = &( (*p_elt_list)[d->n]);
	  strcpy(e->key, key);
    e->value = value;

    h = XXH64(key, strlen(key), 0) % d->size;

    e->next = (*p_ht_table)[h];
    (*p_ht_table)[h] = d->n;
    d->n++;

    /* grow table if there is not enough room */
    if(d->n >= (d->size * MAX_LOAD_FACTOR) ) {
		printf("Hash table is FULL.\nQuit.\n");
		exit(1);
//        grow(d);
    }
}

/* return the most recently inserted value associated with a key */
/* or 0 if no matching key is present */
int DictSearch(Dict d, const char *key, struct elt ** p_elt_list, int ** p_ht_table)
{
	int idx;
  struct elt *e;

  if(d->n == 0) return (-1);

	idx = (*p_ht_table)[XXH64(key, strlen(key), 0) % d->size];
	if(idx == -1)	{
		return (-1);
	}
	
	e = &( (*p_elt_list)[idx] );
    while(1) {
        if(!strcmp(e->key, key)) {
            return e->value;
        }
		else	{
			idx = e->next;
			if(idx == -1)	{	// end
				return (-1);
			}
			e = &( (*p_elt_list)[idx] );
		}
    }

    return -1;
}

/*
// delete the most recently inserted record with the given key 
// if there is no such record, has no effect 
void DictDelete(Dict d, const char *key)
{
    struct elt **prev;          // what to change when elt is deleted
    struct elt *e;              // what to delete

    for(prev = &(d->table[XXH64(key, strlen(key), 0) % d->size]); 
        *prev != 0; 
        prev = &((*prev)->next)) {
        if(!strcmp((*prev)->key, key)) {
            e = *prev;
            *prev = e->next;

            return;
        }
    }
}
*/

