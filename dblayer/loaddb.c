#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#include "tbl.h"
#include "util.h"

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

#define MAX_PAGE_SIZE 4000


#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define CSV_NAME "data.csv"


/*
Takes a schema, and an array of strings (fields), and uses the functionality
in codec.c to convert strings into compact binary representations
 */
int
encode(Schema *sch, char **fields, byte *record, int spaceLeft) {

    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record

    int i;
    int totalBytes = 0;

    for (i = 0; i < sch->numColumns; i++) {
        switch (sch->columns[i]->type) {
            case VARCHAR:
                totalBytes += EncodeCString(fields[i], record + totalBytes, spaceLeft - totalBytes);
                break;
            case INT:
                totalBytes += EncodeInt(atoi(fields[i]), record + totalBytes);
                break;
            case LONG:
                totalBytes += EncodeLong(atol(fields[i]), record + totalBytes);
                break;
            default:
                printf("Error: Unknown type %d\n", sch->columns[i]->type);
                exit(1);
        }
    }
    return totalBytes;
}

Schema *
loadCSV() {
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }

    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
        fprintf(stderr, "Unable to read data.csv\n");
        exit(EXIT_FAILURE);
    }

    // Open main db file
    Schema *sch = parseSchema(line);
    Table *tbl;

    //Added

    Table_Open(DB_NAME, sch, true, &tbl);


    //destroy index file if existed
    PF_DestroyFile(INDEX_NAME);
    
    int err = AM_CreateIndex(DB_NAME, 0, 'i', 4);
    int indexFD = PF_OpenFile(INDEX_NAME);

    printf("table opened and index initialized\n");

    //Till here

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {

        int n = split(line, ",", tokens);
        assert (n == sch->numColumns);
        int len = encode(sch, tokens, record, sizeof(record));
        RecId rid;

        //Added

        Table_Insert(tbl, record, len, &rid);
        printf("Record insert: %d %s %s %s\n", rid, tokens[0], tokens[1], tokens[2]);

        // Indexing on the population column 
        char *popu = malloc(sizeof(int));
        EncodeInt(atoi(tokens[2]), popu);

        printf("Upto here\n");

        // Use the population field as the field to index on
        int er = AM_InsertEntry(indexFD, 'i', 4, popu, rid);

        if (er != AME_OK) {
            printf("AM insert error\n");
            exit(1);
        }
        
        //Till here
    }

    fclose(fp);
    Table_Close(tbl);

    err = PF_CloseFile(indexFD);
    checkerr(err);
    return sch;
}

int
main() {
    loadCSV();
}
