#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"
#define RECORDS_NUM 20 // Number of records to insert
#define FILE_NAME "data.db" // Name of the database file

// Macro to handle function calls and exit on failure
#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; /* Call the function and store the error code */ \
    if (code != BF_OK)        /* Check if the call failed */ \
    {                         \
      BF_PrintError(code);    /* Print the error details */ \
      exit(code);             /* Exit the program */ \
    }                         \
  }

// function declare
void insertAndSearch();          // Function to insert and search records in the B+-tree

// Main function
int main() {
    printf("Starting main...\n");
    printf("Initializing Buffer Manager...\n");
    CALL_OR_DIE(BF_Init(LRU)); //
    srand(time(NULL)); // random num
    insertAndSearch(); // sunartisi gia ola ta operations
    printf("Closing Buffer Manager...\n"); 
    CALL_OR_DIE(BF_Close()); // cleanup
    printf("Buffer Manager closed successfully.\n");
    printf("Main completed successfully.\n");
    return 0; 
}

void insertAndSearch() {
    printf("Creating and testing B+-tree...\n");

  
    printf("Creating B+-tree file: %s\n", FILE_NAME); // neo b+ tree file
    if (BP_CreateFile(FILE_NAME) != 0) { // Create the file for the B+-tree
        printf("Failed to create B+-tree file\n");
        exit(EXIT_FAILURE); // fail
    }
    printf("B+-tree file created successfully.\n");

    // Step 2: Open the B+-tree file
    printf("Opening B+-tree file: %s\n", FILE_NAME);
    int file_desc; // Variable to store the file descriptor
    BPLUS_INFO* info = BP_OpenFile(FILE_NAME, &file_desc); // Open the file and retrieve metadata
    if (info == NULL) { // Check if the file was opened successfully
        printf("Failed to open B+-tree file\n");
        exit(EXIT_FAILURE); // Exit if the file fails to open
    }
    printf("B+-tree file opened successfully. File descriptor: %d\n", file_desc);
    printf("Inserting records into the B+-tree...\n");
    Record records[RECORDS_NUM]; // pinakas gia ta records
    for (int i = 0; i < RECORDS_NUM; i++) { // loop
        records[i] = randomRecord(); 
        if (BP_InsertEntry(file_desc, info, records[i]) == -1) { // insert ramdom record
            printf("Failed to insert record: ID = %d\n", records[i].id); // error/debugg
        } else {
            printf("Record inserted successfully: ID = %d\n", records[i].id); // success/ddebug
        }
    }
    printf("All records inserted successfully.\n");
  
   
    printf("Searching for inserted records...\n");
    for (int i = 0; i < RECORDS_NUM; i++) { // loop gia ta records
        Record* result = NULL; // Pointer gia na krathsw to result
        if (BP_GetEntry(file_desc, info, records[i].id, &result) == 0 && result != NULL) { // search
            printf("Record found: ");
            printRecord(*result); // print to record pou vrika gia debug
            free(result); // free gia thn getentry
        } else {
            printf("Record not found: ID = %d\n", records[i].id); 
        }
    }

    // elegxos an vriskw records pou den uparxoyn, isws na mhn to krathsoyme auto, mallon den xreiazetai
    int random_id = 10000 + rand() % 1000; // random id
    printf("Searching for non-existent ID: %d\n", random_id);
    Record* result = NULL; // Pointer gia na krathsw to result
    if (BP_GetEntry(file_desc, info, random_id, &result) == 0 && result != NULL) { // random id search
        printf("Unexpectedly found record: ");// den tha prepei na ginei pote auto, debug
        printRecord(*result);
        free(result); // Free gia to malloc pou kanw sthn getentry
    } else {
        printf("Record with ID %d not found as expected.\n", random_id); // debug(make sure it wasnt found)
    }

    printf("Closing B+-tree file after searching: %s\n", FILE_NAME); // Clean uppp, close the file
    if (BP_CloseFile(file_desc, info) != 0) { 
        printf("Failed to close B+-tree file\n");
        exit(EXIT_FAILURE); 
    }
    printf("B+-tree file closed successfully.\n");

    printf("B+-tree testing completed successfully.\n");
}
