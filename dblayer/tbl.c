#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int  getNumSlots(byte *pageBuf){

    // Number of slots in the page
    return DecodeShort(pageBuf);
}

int  getNthSlotOffset(int slot, char* pageBuf) {

    // Offset of the nth slot
    if(slot > getNumSlots(pageBuf)) {
        return -1;
    }

    return DecodeShort(pageBuf + SLOT_COUNT_OFFSET*2 + slot*2);
}

int  getLen(int slot, byte *pageBuf) {

    int nslots = DecodeShort(pageBuf);

    // Length of the nth slot calculated using slot offsets in the page
    if(slot == 0) {
        return PF_PAGE_SIZE - getNthSlotOffset(slot, pageBuf);
    }
    else if(slot < nslots) {
        return getNthSlotOffset(slot-1, pageBuf) - getNthSlotOffset(slot, pageBuf);
    }

    /* This is for a special case when we are enquiring the available free space in the page
    for inserting a new record. In this case, we return the available free space (minus the space
    required for the slot header) */
    else if(slot == nslots)
    {
        return DecodeShort(pageBuf + SLOT_COUNT_OFFSET) - SLOT_COUNT_OFFSET * 2 - (nslots+1) * 2 + 1;
    }

    else
        return -1;
}

void setNumSlots(byte *pageBuf, int nslots) {

    // Set the number of slots in the page header

    EncodeShort(nslots, pageBuf);
}

int setNthSlotOffset(int slot, char* pageBuf, int len) {

    // Set the offset of the nth slot
    
    if(slot > getNumSlots(pageBuf)) {
        return -1;
    }

    int nslots = slot + 1;

    //update page header
    setNumSlots(pageBuf, nslots);
    
    //encode slot header
    EncodeShort(DecodeShort(pageBuf + SLOT_COUNT_OFFSET) - len, pageBuf + SLOT_COUNT_OFFSET*2 + slot*2);

    //update free space pointer
    EncodeShort(DecodeShort(pageBuf + SLOT_COUNT_OFFSET) - len - 1, pageBuf + SLOT_COUNT_OFFSET);

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
    // Initialize PF
    PF_Init();

    // If overwrite is true, delete the file if it exists.
    if(overwrite) {
        PF_DestroyFile(dbname);
    }
    
    // Create the File
    PF_CreateFile(dbname);

    // Open the File
    int file_fd = PF_OpenFile(dbname);

    // Allocate a table structure and intialize it
    Table *table = (Table *) malloc(sizeof(Table));
    table->schema = schema;

    table->fd = file_fd;
    table->n_pages = 0;
    table->dirty_pages = (int *) malloc(8 * sizeof(int));
    table->n_dirty = 0;
    table->dirty_array_size = 8;
    table->top_page = -1;
    table->curr_page_buff = NULL;

    // Set the table pointer to the newly allocated table
    *ptable = table;

    return 0;
}

void
Table_Close(Table *tbl) {

    // Loop through all dirty pages and unfix them

    for(int i=0; i<tbl->n_dirty; i++){ 
        PF_UnfixPage(tbl->fd, tbl->dirty_pages[i], TRUE);
    }
    
    // Set number of dirty pages to zero and close the file.
    tbl->n_dirty = 0;
    PF_CloseFile(tbl->fd);
}

int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {

    // Initialize pageNum and Page Buffer 
    int pageNum = -1;
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    int slot = 0;

    // If the table is empty, allocate a new page
    if(tbl->n_pages == 0)
    {
        //Allocate and set up new page
        checkerr(PF_AllocPage(tbl->fd, &pageNum, &pageBuf));

        // Update the table variables accordingly
        tbl->n_pages++;
        tbl->top_page = pageNum;
        tbl->n_dirty = 1;
        tbl->dirty_pages[0] = pageNum;
        tbl->curr_page_buff = pageBuf;

        // Encode Page Header
        EncodeShort((short) 0, pageBuf);
        EncodeShort((short) 4095, pageBuf+2);

        // Set Record Header
        setNthSlotOffset(0, pageBuf, len);

        // Copy record into page
        memcpy(pageBuf + getNthSlotOffset(slot, pageBuf), record, len);
    }

    // If table is not empty
    else
    { 
        // Get the top page
        pageBuf = tbl->curr_page_buff;
        pageNum = tbl->top_page;

        // Allocate a new page if there is not enough space
        if(len > getLen(getNumSlots(pageBuf), pageBuf)) {

            // allocate page and update n_pages variable
            checkerr(PF_AllocPage(tbl->fd, &pageNum, &pageBuf));
            tbl->n_pages++;

            // Page Header
            EncodeShort((short) 0, pageBuf);
            EncodeShort((short) 4095, pageBuf+2);
            
            // Record Header
            setNthSlotOffset(0, pageBuf, len);
        }
        // Enough space in page 
        else {

            // Find the first free slot on the page and set offset
            slot = getNumSlots(pageBuf);
            setNthSlotOffset(slot, pageBuf, len);
        }

        // copy record in free space
        memcpy(pageBuf + getNthSlotOffset(slot, pageBuf), record, len);

    }

    if(tbl->n_dirty > 0 || tbl->dirty_pages[tbl->n_dirty-1] != pageNum)
    {
        //code for adding a dirty page number to the array
        if(tbl->n_dirty == tbl->dirty_array_size)
        {
            tbl->dirty_array_size = tbl->dirty_array_size * 2;
            tbl->dirty_pages = (int *) realloc(tbl->dirty_pages, tbl->dirty_array_size * sizeof(int));
        }

        tbl->dirty_pages[tbl->n_dirty] = pageNum;
        tbl->n_dirty = tbl->n_dirty + 1;
    }

    // Set the rid
    *rid = (pageNum << 16) | slot;

    // Set the table variables
    tbl->curr_page_buff = pageBuf;
    tbl->top_page = pageNum;

    return 0;
}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int pageNum = rid >> 16;
    int slot = rid & 0xFFFF;

    //get the page 
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    PF_GetThisPage(tbl->fd, pageNum, &pageBuf);

    //get the length of record
    int len = getLen(slot, pageBuf);

    //copy the record
    memcpy(record, pageBuf + getNthSlotOffset(slot, pageBuf), ((len < maxlen) ? len : maxlen));

    //return the length of record
    return ((len < maxlen) ? len : maxlen);
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {
    
    // Initialize pageNum and Page Buffer 
    int pageNum = -1;
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    RecId rid;

    // For each page obtained using PF_GetNextPage
    //    for each slot in that page,
    //          get rid, and initialize record structure and copy from page buffer
    //          call callbackfn(callbackObj, rid, record, recordLen) for each record

    while(PF_GetNextPage(tbl->fd, &pageNum, &pageBuf) != PFE_EOF) {
        int nslots = getNumSlots(pageBuf);
        for (int slot=0; slot< nslots; slot++) {
            rid = (pageNum << 16) | slot;
            int recordLen = getLen(slot, pageBuf);
            byte* record = (byte *) malloc(recordLen * sizeof(byte));
            memcpy(record, pageBuf + getNthSlotOffset(slot, pageBuf), recordLen);
            callbackfn(callbackObj, rid, record, recordLen);
        }
    }
}