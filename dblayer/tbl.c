
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int  getLen(int slot, byte *pageBuf) {

    //unimplemented

    int len = 0;
    len = len | (pageBuf[slot] & 0xFF);
    len = len << 8;
    len = len | (pageBuf[slot + 1] & 0xFF);
    len = len << 8;
    len = len | (pageBuf[slot + 2] & 0xFF);
    return len;
}

int  getNumSlots(byte *pageBuf){

    //unimplemented

    int numSlots = 0;
    numSlots = numSlots | (pageBuf[0] & 0xFF);
    numSlots = numSlots << 8;
    numSlots = numSlots | (pageBuf[1] & 0xFF);
    return numSlots;
}

void setNumSlots(byte *pageBuf, int nslots) {

    //unimplemented

    pageBuf[0] = (byte) (nslots >> 8);
    pageBuf[1] = (byte) (nslots & 0xFF);
}

int  getNthSlotOffset(int slot, char* pageBuf) {

    //unimplemented

    int offset = 0;
    offset = offset | (pageBuf[slot + SLOT_COUNT_OFFSET] & 0xFF);
    offset = offset << 8;
    offset = offset | (pageBuf[slot + SLOT_COUNT_OFFSET + 1] & 0xFF);
    offset = offset << 8;
    offset = offset | (pageBuf[slot + SLOT_COUNT_OFFSET + 2] & 0xFF);
    return offset;
}


/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{   
    // Initialize PF, create PF file,
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage. 

    //unimplemented

    PF_Init();

    if(overwrite) {
        checkerr(PF_DestroyFile(dbname));
    }

    checkerr(PF_CreateFile(dbname));

    Table *table = (Table *) malloc(sizeof(Table));
    table->schema = schema;

    int fd = PF_OpenFile(dbname);
    table->fd = fd;
    *ptable = table;

    return 0;
}

void
Table_Close(Table *tbl) {

    // Unfix any dirty pages, close file.
    int pageNum = -1, slot = -1;
    
    while(PF_GetNextPage(tbl->fd, &pageNum, &slot) != PFE_EOF) {
        PF_UnfixPage(tbl->fd, pageNum, FALSE);
    }
    
    PF_CloseFile(tbl->fd);
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.

    //unimplemented

    int fd = tbl->fd;
    int slot = 0;
    int nslots = 0;

    // Allocate a new page if there is not enough space
    if(len > PF_PAGE_SIZE - SLOT_COUNT_OFFSET) {
        checkerr(PF_AllocPage(fd, &slot, (byte *) malloc(PF_PAGE_SIZE)));
        nslots = 1;
    }

    // Get the page
    byte *pageBuf = (byte *) malloc(PF_PAGE_SIZE);
    checkerr(PF_GetThisPage(fd, slot, pageBuf));

    // Get the number of slots
    nslots = getNumSlots(pageBuf);

}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    //unimplemented

    // Get the page
    byte *pageBuf = (byte *) malloc(PF_PAGE_SIZE);
    checkerr(PF_GetThisPage(tbl->fd, slot, pageBuf));

    // Get the number of slots
    int nslots = getNumSlots(pageBuf);

    //get slot offset of record
    int slotOffset = getNthSlotOffset(slot, pageBuf);

    //memcpy bytes to record
    memcpy(record, pageBuf + slotOffset, maxlen);

    //unfix page
    checkerr(PF_UnfixPage(tbl->fd, slot, FALSE));

    //return size of record
    return maxlen;

    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    // return len; // return size of record

}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    // UNIMPLEMENTED;

    // Iterate over all pages, and for each page, iterate over all
    // slots, and for each slot, call callbackfn.
    for(int i = 0; i < tbl->fd; i++) {

        byte *pageBuf = (byte *) malloc(PF_PAGE_SIZE);
        checkerr(PF_GetThisPage(tbl->fd, i, pageBuf));
        int nslots = getNumSlots(pageBuf);
        for(int j = 0; j < nslots; j++) {
            int slotOffset = getNthSlotOffset(j, pageBuf);
            int len = getLen(j, pageBuf);
            byte *record = (byte *) malloc(len);
            memcpy(record, pageBuf + slotOffset, len);
            callbackfn(callbackObj, i, record, len);
        }
        PF_UnfixPage(tbl->fd, i, 0);
    }



    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}


