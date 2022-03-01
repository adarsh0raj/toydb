### CS387 Lab 5 - Implementing DB Internals with ToyDB

## Names and Roll Numbers

1. Adarsh Raj - 190050004
2. Gudipaty Aniket - 190050041

## References

1. https://www.javatpoint.com/file-organization-storage#:~:text=The%20primary%20technique%20of%20the,indirect%20pointers%20to%20the%20record.
2. https://www.javatpoint.com/dbms-file-organization
3. https://www.geeksforgeeks.org/function-pointer-in-c/

## Contributions

1. Adarsh Raj:

    - Added unimplemented code in loaddb.c and dumpdb.c (Task 2 and Task 3)
    - Ran the code for different variations of dumpdb and added screenshots
    - Made README and Added comments for task 2 and 3

2. Gudipaty Aniket

    - Added codes for different length functions after researching about the slotted page structure
    - Added table structure in tbl.h and all unimplemented functions in tbl.c
    - Added comments for the task 1

## Execution

Steps for Executing the given database:

1. In pflayer directory, run makefile using make first and then do the same in the amlayer directory.
2. Keep the database file named "data.csv" in the dblayer directory.
3. Run make file using make in the dblayer directory.
4. Run ./loaddb in the dblayer directory to load the database (in data.db) and create the index (in data.db.0).
5. Dumpdb is used to retrive the data from the database in one of the following two ways:

    - "./dumpdb s" in the dblayer directory to retrive the data in sequential order, using Table_Scan.
    - "./dumpdb i" in the dblayer directory to retrive the data using index scan. To change the index operation and value, change the       attributes in index_scan() function call in dumpdb.c

    - "./dumpdb i <condition> <value>" can also be used to retrive data from database using index scan and satisfying the given conditions.

6. To reload the database, make clean in dblayer directory and then follow from step 2 onwards.