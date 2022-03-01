#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

void
printRow(void *callbackObj, RecId rid, byte *row, int len) {

    // get schema as a callback object and initialise a cursor to the row
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;

    // string to hold the field value (varchar case)
    char *str = malloc(len + 1);

    // decode the row
    int i;
    for (i = 0; i < schema->numColumns; i++) {
        switch (schema->columns[i]->type) {

            // Case 1 varchar field
            case VARCHAR:
                cursor += DecodeCString(cursor, str, len - (cursor - row)) + 2;
                printf("%s", str);        // print the string value
                break;

            // Case 2 int field
            case INT:
                printf("%d", DecodeInt(cursor));
                cursor += sizeof(int);
                break;
            
            // Case 3 long field
            case LONG:
                printf("%lld", DecodeLong(cursor));
                cursor += sizeof(long);
                break;
            default:
                printf("Error: Unknown type %d\n", schema->columns[i]->type);
                exit(1);
        }
        // separated by a comma to ensure csv style
        if (i < schema->numColumns - 1) {
            printf(",");
        }
    }
    printf("\n");
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define MAX_PAGE_SIZE 4000
	 
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

    // encode the given value into a byte array
    byte *valueBytes = malloc(sizeof(int));
    EncodeInt(value, valueBytes);

    // open the index and set a variable to its return scan descriptor
    int scanDesc = AM_OpenIndexScan(indexFD,'i',4, op, valueBytes);

    // loop throught the index
    while(true) {

        //find next entry in index
        int index_entry = AM_FindNextEntry(scanDesc);

        // check for error
        if (index_entry == AME_EOF) {
            break;
        }

        //fetch rid from table
        RecId rid = index_entry;

        // tableGet record from rid
        byte *record = malloc(MAX_PAGE_SIZE);
        int len = Table_Get(tbl, rid, record, MAX_PAGE_SIZE);

        // print the row
        printRow(schema, rid, record, len);
    }    

    // Close the index
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

        // to ensure working of ./dumpdb i <condition> <value>
        if(argc == 4) {
            if(stricmp("LESS_THAN_EQUAL", argv[2]) == 0) {
                index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, atoi(argv[3]));
            } 
            else if(stricmp("GREATER_THAN_EQUAL", argv[2]) == 0) {
                index_scan(tbl, schema, indexFD, GREATER_THAN_EQUAL, atoi(argv[3]));
            } 
            else if(stricmp("EQUAL", argv[2]) == 0) {
                index_scan(tbl, schema, indexFD, EQUAL, atoi(argv[3]));
            } 
            else if(stricmp("LESS_THAN", argv[2]) == 0) {
                index_scan(tbl, schema, indexFD, LESS_THAN, atoi(argv[3]));
            } 
            else if(stricmp("GREATER_THAN", argv[2]) == 0) {
                index_scan(tbl, schema, indexFD, GREATER_THAN, atoi(argv[3]));
            } 
            else {
                printf("Error: Unknown operator %s\n", argv[2]);
                exit(1);
            }
        } 
        // default ./dumpdb i case 
        else {
            // Ask for populations less than 100000, then more than 100000. 
            // Change the attributes in below function to change default behaviour
            index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
            index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
        }
    }
    // Close the table
    Table_Close(tbl);
}
