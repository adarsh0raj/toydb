#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>

#define VARCHAR 1
#define INT     2
#define LONG    3

typedef char byte;

typedef struct {
    char *name;
    int  type;  // one of VARCHAR, INT, LONG
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc **columns; // array of column descriptors
} Schema;

typedef struct {
    Schema *schema;

    // the following are used for the current row
    int numColumns;
    byte **columns; // array of column values   

    // the following are used for the current page
    int numSlots;
    int slotCountOffset;
    int slotCountSize;
    int slotSize;
    int slotOffset;
    int slotSizeOffset;
    int slotSizeSize;
    int freeSpaceOffset;
    int freeSpaceSize;
    int pageSize;
    int pageNum;
    byte *pageBuf;

    //add file descriptors
    int fd;
    
} Table ;

typedef int RecId;

int
Table_Open(char *fname, Schema *schema, bool overwrite, Table **table);

int
Table_Insert(Table *t, byte *record, int len, RecId *rid);

int
Table_Get(Table *t, RecId rid, byte *record, int maxlen);

void
Table_Close(Table *);

typedef void (*ReadFunc)(void *callbackObj, RecId rid, byte *row, int len);

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn);

#endif
