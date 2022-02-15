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

    int nslots = DecodeShort(pageBuf);

    if(slot == 0) {
        return PF_PAGE_SIZE - getNthSlotOffset(slot, pageBuf);
    }
    else if(slot < nslots) {
        return getNthSlotOffset(slot-1, pageBuf) - getNthSlotOffset(slot, pageBuf);
    }

    else if(slot == nslots)
    {
        return DecodeShort(pageBuf + SLOT_COUNT_OFFSET) - SLOT_COUNT_OFFSET * 2 - (nslots+1) * 2 + 1;
    }

    else
        return -1;
}

int  getNumSlots(byte *pageBuf){

    //unimplemented
    return DecodeShort(pageBuf);
}

void setNumSlots(byte *pageBuf, int nslots) {

    //unimplemented
    EncodeShort(nslots, pageBuf);

    //set pointer of latest slot record as the free space pointer
    memcpy(pageBuf + SLOT_COUNT_OFFSET + (nslots) * 2, pageBuf + SLOT_COUNT_OFFSET, 2);
}

int  getNthSlotOffset(int slot, char* pageBuf) {

    //unimplemented
    if(slot > getNumSlots(pageBuf)) {
        return -1;
    }

    return DecodeShort(pageBuf + SLOT_COUNT_OFFSET*2 + slot*2);
}

int setNthSlotOffset(int slot, int offset, char* pageBuf, int len) {

    //unimplemented
    if(slot > getNumSlots(pageBuf)) {
        return -1;
    }

    EncodeShort(DecodeShort(pageBuf + SLOT_COUNT_OFFSET) - len, pageBuf + SLOT_COUNT_OFFSET*2 + slot*2);

    return 0;
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

    PF_CreateFile(dbname);

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
    int pageNum = -1;
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    
    while(PF_GetNextPage(tbl->fd, &pageNum, &pageBuf) != PFE_EOF) {
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

    int pageNum = -1;
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    int slot = 0;

    // Allocate a new page if there is not enough space
    if(len > getLen(getNumSlots(pageBuf), pageBuf)) {
        checkerr(PF_AllocPage(tbl->fd, &pageNum, &pageBuf));
        setNumSlots(pageBuf, getNumSlots(pageBuf) + 1);
        setNthSlotOffset(getNumSlots(pageBuf), pageBuf, pageNum, len);
        slot = getNumSlots(pageBuf) - 1;
    }
    else {
        // Find the first free slot on the page
        while(getNthSlotOffset(slot, pageBuf) != 0) {
            slot++;
        }
    }
    // copy record in free space
    memcpy(pageBuf + getNthSlotOffset(slot, pageBuf), record, len);

    // Update slot and free space index information on top of page.
    // setNthSlotOffset(slot, pageBuf, pageNum);
    // setLen(slot, pageBuf, len);

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

    //get the page 
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    checkerr(PF_GetThisPage(tbl->fd, pageNum, &pageBuf));

    //get the slot offset of record
    int slotOffset = getNthSlotOffset(slot, pageBuf);

    //get the length of record
    int len = getLen(slot, pageBuf);

    //copy the record
    memcpy(record, pageBuf + slotOffset, len);

    //unfix the page
    PF_UnfixPage(tbl->fd, pageNum, FALSE);

    return len;


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

    int pageNum = -1;
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    RecId rid;

    while(PF_GetNextPage(tbl->fd, &pageNum, &pageBuf) != PFE_EOF) {
        int slot = 0;
        while(getNthSlotOffset(slot, pageBuf) != -1) {
            rid = (pageNum << 16) | slot;
            callbackfn(callbackObj, rid, (byte *) (pageBuf + getNthSlotOffset(slot, pageBuf)), getLen(slot, pageBuf));
            slot++;
        }
        PF_UnfixPage(tbl->fd, pageNum, FALSE);
    }

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}


