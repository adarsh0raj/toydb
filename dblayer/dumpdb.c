#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

#define MAX_PAGE_SIZE 4096

void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;
    char *str = malloc(len + 1);

    // decode the row
    int i;
    for (i = 0; i < schema->numColumns; i++) {
        switch (schema->columns[i]->type) {
            case VARCHAR:
                cursor += DecodeCString(cursor, str, len - (cursor - row));
                break;
            case INT:
                printf("%d", DecodeInt(cursor));
                cursor += sizeof(int);
                break;
            case LONG:
                printf("%ld", DecodeLong(cursor));
                cursor += sizeof(long);
                break;
            default:
                printf("Error: Unknown type %d\n", schema->columns[i]->type);
                exit(1);
        }
        if (i < schema->numColumns - 1) {
            printf(",");
        }
    }
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
	 
void
index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value) {
    /*
    Open index ...
    while (true) {
	find next entry in index
	fetch rid from table
        printRow(...)
    }
    close index ...
    */

    byte *valueBytes = malloc(sizeof(int));
    EncodeInt(value, valueBytes);
    int scanDesc = AM_OpenIndexScan(indexFD,'i',4, op, valueBytes);

    while(true) {
        //find next entry in index
        int index_entry = AM_FindNextEntry(scanDesc);
        if (index_entry == AME_EOF) {
            break;
        }

        //fetch rid from table
        RecId rid = AM_GetRecId(indexFD, index_entry);

        // tableGet record from rid
        byte *record = malloc(MAX_PAGE_SIZE);
        int len = Table_Get(tbl, rid, record, MAX_PAGE_SIZE);


        //printRow(...)
        printRow(schema, rid, record, len);
        printf("\n");
    }    

    AM_CloseIndexScan(scanDesc);
}

int
main(int argc, char **argv) {
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    Table *tbl;

    // open table
    Table_Open(DB_NAME, schema, false, &tbl);

    if (argc == 2 && *(argv[1]) == 's') {
	    // invoke Table_Scan with printRow, which will be invoked for each row in the table.
        // invoke sequential scan using printRow function
        Table_Scan(tbl, schema, printRow);

    } else {
        // index scan by default
        int indexFD = PF_OpenFile(INDEX_NAME);
        checkerr(indexFD);

        // Ask for populations less than 100000, then more than 100000. Together they should
        // yield the complete database.
        index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
        index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
    }
    Table_Close(tbl);
}
