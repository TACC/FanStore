#define MAX_NAME_LEN	(96)
#define INITIAL_SIZE (1024*1024*2)

struct elt {
    int next;	// 
    int value;
    char key[MAX_NAME_LEN];
};

struct dict {
    int size;           /* size of the pointer table */
    int n;              /* number of elements stored */
};

typedef struct dict *Dict;


//void DictCreate(Dict, int nSize);
//void DictInsert(Dict, const char *key, const int value);

/* return the most recently inserted value associated with a key */
/* or 0 if no matching key is present */
//int DictSearch(Dict, const char *key);

/* delete the most recently inserted record with the given key */
/* if there is no such record, has no effect */
//void DictDelete(Dict, const char *key);

void DictCreate(Dict d, int nSize, struct elt ** p_elt_list, int ** p_ht_table);
void DictInsert(Dict d, const char *key, const int value, struct elt ** p_elt_list, int ** p_ht_table);
int DictSearch(Dict d, const char *key, struct elt ** p_elt_list, int ** p_ht_table);

