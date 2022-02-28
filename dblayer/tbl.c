#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int n_tables = 0;
bool file_exists = 0, file_opened = 0, pf_inited = 0;
int file_fd = 0;

int  getNumSlots(byte *pageBuf){

    //unimplemented
    return DecodeShort(pageBuf);
}

int  getNthSlotOffset(int slot, char* pageBuf) {

    //unimplemented
    if(slot > getNumSlots(pageBuf)) {
        return -1;
    }

    return DecodeShort(pageBuf + SLOT_COUNT_OFFSET*2 + slot*2);
}

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

void setNumSlots(byte *pageBuf, int nslots) {

    //unimplemented
    EncodeShort(nslots, pageBuf);
}

int setNthSlotOffset(int slot, char* pageBuf, int len) {

    if(slot > getNumSlots(pageBuf)) {
        return -1;
    }

    int nslots = slot + 1;

    setNumSlots(pageBuf, nslots);
    
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
    // Initialize PF, create PF file,
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage. 

    //unimplemented

    if(pf_inited == 0)
    {
        PF_Init();
        pf_inited = 1;
    }

    if(overwrite) {
        PF_DestroyFile(dbname);
    }

    if(file_exists == 0)
    {
        checkerr(PF_CreateFile(dbname));
        file_exists = 1;
    }

    Table *table = (Table *) malloc(sizeof(Table));
    table->schema = schema;

    if(file_opened == 0)
    {
        file_fd = PF_OpenFile(dbname);
        file_opened = 1;
    }

    table->fd = file_fd;
    table->n_pages = 0;
    table->dirty_pages = (int *) malloc(8 * sizeof(int));
    table->n_dirty = 0;
    table->dirty_array_size = 8;
    table->top_page = -1;
    table->curr_page_buff = NULL;

    ptable[n_tables] = table;
    n_tables = n_tables + 1;

    return 0;
}

void
Table_Close(Table *tbl) {

    // Unfix any dirty pages, close file.

    for(int i=0; i<tbl->n_dirty; i++){ 
        PF_UnfixPage(tbl->fd, tbl->dirty_pages[i], TRUE);
    }
    
    tbl->n_dirty = 0;
    PF_CloseFile(tbl->fd);
    file_opened = 0;
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

    if(tbl->n_pages == 0)
    {
        //Allocate and set up new page
        checkerr(PF_AllocPage(tbl->fd, &pageNum, &pageBuf));
        tbl->n_pages++;
        tbl->top_page = pageNum;
        tbl->n_dirty = 1;
        tbl->dirty_pages[0] = pageNum;
        tbl->curr_page_buff = pageBuf;

        printf("Hello\n");

        //page header
        EncodeShort((short) 0, pageBuf);
        EncodeShort((short) 4095, pageBuf+2);

        //record header
        setNthSlotOffset(0, pageBuf, len);

        memcpy(pageBuf + getNthSlotOffset(slot, pageBuf), record, len);
    }

    else
    {
        pageBuf = tbl->curr_page_buff;
        pageNum = tbl->top_page;

        // Allocate a new page if there is not enough space
        if(len > getLen(getNumSlots(pageBuf), pageBuf)) {
            checkerr(PF_AllocPage(tbl->fd, &pageNum, &pageBuf));
            tbl->n_pages++;
            printf("Hi\n");
            EncodeShort((short) 0, pageBuf);
            EncodeShort((short) 4095, pageBuf+2);

            setNthSlotOffset(0, pageBuf, len);
        }
        else {
            // Find the first free slot on the page
            while(getNthSlotOffset(slot, pageBuf) != -1) {
                slot = getNumSlots(pageBuf);
            }

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

    *rid = (pageNum << 16) | slot;

    tbl->curr_page_buff = pageBuf;
    tbl->top_page = pageNum;

    return 0;

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
    int pageNum = rid >> 16;
    int slot = rid & 0xFFFF;

    //unimplemented

    //get the page 
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    checkerr(PF_GetThisPage(tbl->fd, pageNum, &pageBuf));

    //get the slot offset of record
    int slotOffset = getNthSlotOffset(slot, pageBuf);

    //get the length of record
    int len = getLen(slot, pageBuf);

    //copy the record
    memcpy(record, pageBuf + slotOffset, ((len < maxlen) ? len : maxlen));

    //unfix the page
    // PF_UnfixPage(tbl->fd, pageNum, FALSE);

    return ((len < maxlen) ? len : maxlen);


    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    // return len; // return size of record

}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    // Iterate over all pages, and for each page, iterate over all
    // slots, and for each slot, call callbackfn.

    int pageNum = -1;
    char *pageBuf = (char *) malloc(PF_PAGE_SIZE);
    RecId rid;

    while(PF_GetNextPage(tbl->fd, &pageNum, &pageBuf) != PFE_EOF) {
        int slot = 0;
        while(getNthSlotOffset(slot, pageBuf) != -1) {
            rid = (pageNum << 16) | slot;
            int recordLen = getLen(slot, pageBuf);
            byte* record = (byte *) malloc(recordLen * sizeof(byte));
            memcpy(record, pageBuf + getNthSlotOffset(slot, pageBuf), recordLen);
            callbackfn(callbackObj, rid, record, recordLen);
            slot++;
        }
    }

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}