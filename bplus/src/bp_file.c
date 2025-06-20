#include <stdio.h>          
#include <stdlib.h>         
#include <string.h>         

#include "bf.h"            
#include "bp_file.h"        
#include "record.h"         
#include <bp_datanode.h>    
#include <stdbool.h>        

#define bplus_ERROR -1 // mallon eprepe na htan apo pprin defined alla den htan?

#define MAX_RECORDS_PER_BLOCK ((BF_BLOCK_SIZE - sizeof(char) - sizeof(int)) / sizeof(Record))

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call;  \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return bplus_ERROR;      \
    }                         \
  }

int InsertIntoLeaf(int file_desc, BPLUS_INFO *bplus_info, BF_Block *block, Record record);

int GetBlockNumber(int file_desc, BF_Block *block) {
    int block_count; // total num of blocks 
    CALL_BF(BF_GetBlockCounter(file_desc, &block_count)); //  current block count
    return block_count - 1;   // to teleutaio block vrisketai sthn thesi count - 1
}

int UpdateParent(int file_desc, BPLUS_INFO *bplus_info, int parent_block_id, int promoted_key, int left_block_id, int right_block_id) {
    BF_Block *block;  // Block gia to node
    BF_Block_Init(&block); // Init block
    CALL_BF(BF_GetBlock(file_desc, parent_block_id, block)); // parentt block
    char *data = BF_Block_GetData(block);  // pointer gia ta  data tou block

    char block_type = data[0];  // prwto byte tha einai eite Leaf eite Internal
    int *num_keys_ptr = (int *)(data + sizeof(char)); // num of keys sto internal node
    int num_keys = *num_keys_ptr;  // copy

    int *keys = (int *)(data + sizeof(char) + sizeof(int)); // Pointer ston pinaka me ta keysrray
    int *children = (int *)((char *)keys + num_keys * sizeof(int)); // Pointer ston pinaka me ta paidia nodes(block IDs)

    printf("Updating parent node %d with promoted key %d\n", parent_block_id, promoted_key); // debug

    // eisagwgh tou promoted key sthn swsth thesi(sorted)
    int i;
    for (i = num_keys - 1; i >= 0 && keys[i] > promoted_key; i--) {
        keys[i + 1] = keys[i];           // Shift sta deksia 
        children[i + 2] = children[i + 1]; // Shift children pointers sta right
    }
    keys[i + 1] = promoted_key;    // new key->swsti thesi 
    children[i + 1] = left_block_id;     // Left child block 
    children[i + 2] = right_block_id;  // Right child block 
    (*num_keys_ptr) = num_keys + 1;  // ++ sto key count

    BF_Block_SetDirty(block);             // dirty 
    CALL_BF(BF_UnpinBlock(block));        // unpin 
    BF_Block_Destroy(&block);             // destroy 

    return 0; // Success
}

int CreateRootNode(int file_desc, BPLUS_INFO *bplus_info, Record record) {
    BF_Block *block;                           
    BF_Block_Init(&block);                     

    CALL_BF(BF_AllocateBlock(file_desc, block));  
    char *data = BF_Block_GetData(block);         
    memset(data, 0, BF_BLOCK_SIZE);     // gemizo to block me 0  

    data[0] = 'L';     // set leaf
    int *num_records = (int *)(data + sizeof(char)); 
    *num_records = 1;                            
    Record *records = (Record *)(data + sizeof(char) + sizeof(int)); // Pointer sta records 
    records[0] = record;            // Insert to prwto record

    int block_count;                              
    CALL_BF(BF_GetBlockCounter(file_desc, &block_count)); //  total blocks meta to allocation
    bplus_info->root_block = block_count - 1;     // nea riza== to teleutaio allocated block
    bplus_info->tree_height = 0;      // root=leaf
    bplus_info->block_count = block_count;   // block count sto metadata

    BF_Block_SetDirty(block);                     
    CALL_BF(BF_UnpinBlock(block));                
    BF_Block_Destroy(&block);                 

    return 0; // Success
}

int SplitLeafNode(int file_desc, BPLUS_INFO *bplus_info, BF_Block *block, Record record) {
    BF_Block *new_block;  // Block for the new leaf
    BF_Block_Init(&new_block);
    CALL_BF(BF_AllocateBlock(file_desc, new_block));  // block gia otan kanw to split

    char *data = BF_Block_GetData(block); // palio leaf
    char *new_data = BF_Block_GetData(new_block);  // neo leaf

    int *num_records = (int *)(data + sizeof(char));    
    int *new_num_records = (int *)(new_data + sizeof(char));
    *new_num_records = 0;                     

    Record *records = (Record *)(data + sizeof(char) + sizeof(int));    // rec paliou leaf
    Record *new_records = (Record *)(new_data + sizeof(char) + sizeof(int)); //rec neou leaf

    int mid = *num_records / 2; // dixotomos

    // ta misa records pane apo to palio leaf sto neo
    for (int i = mid; i < *num_records; i++) {
        new_records[*new_num_records] = records[i]; // cpy sto neo leaf
        (*new_num_records)++;                        // inc gia to neo leaf record num
    }
    *num_records = mid; // pleon tha exei ligotera records to palio leaf

    // insert sto katallhlo leaf
    if (record.id < new_records[0].id) {
        InsertIntoLeaf(file_desc, bplus_info, block, record); // palio leaf
    } else {
        InsertIntoLeaf(file_desc, bplus_info, new_block, record); //  neo leaf
    }

    // update ton geitoniko pointer tou paliou leaf sto neo leaf
    int *neighbor = (int *)(data + BF_BLOCK_SIZE - sizeof(int)); // pointer sto telos tou block
    int new_block_number = GetBlockNumber(file_desc, new_block); // neo leaf id
    *neighbor = new_block_number; // to palio  leaf ppleon dixnei sto neo leaf san geitonas 

    BF_Block_SetDirty(block);     // old leaf = dirty
    BF_Block_SetDirty(new_block); // new leaf= dirty

    // Promote a key (the first key of the new leaf) to the parent
    int promoted_key = new_records[0].id;
    if (bplus_info->tree_height == 0) {
        // an to dentro eixe ena leaf(root==leaf) ftiakse neo root
        CreateRootNode(file_desc, bplus_info, (Record){.id = promoted_key});
    } else {
        // alliws kane update tto parent node me to promoted key
        UpdateParent(file_desc, bplus_info, bplus_info->root_block, promoted_key, GetBlockNumber(file_desc, block), new_block_number);
    }

    CALL_BF(BF_UnpinBlock(new_block));
    BF_Block_Destroy(&new_block);      

    return 0; // success
}

// Insertrecord to leaf
int InsertIntoLeaf(int file_desc, BPLUS_INFO *bplus_info, BF_Block *block, Record record) {
    char *data = BF_Block_GetData(block);
    int *num_records = (int *)(data + sizeof(char));    
    Record *records = (Record *)(data + sizeof(char) + sizeof(int)); // Record array

    printf("Inserting record ID: %d into leaf block\n", record.id);

    
    for (int i = 0; i < *num_records; i++) {
        if (records[i].id == record.id) {
            printf("Duplicate record detected: ID = %d. Skipping insertion.\n", record.id);
            return 0; // den vazw duplicates
        }
    }

    // an yparxei xwros
    if (*num_records < MAX_RECORDS_PER_BLOCK) {
        int i;
        // neo recorcd inserted, sorted
        for (i = *num_records - 1; i >= 0 && records[i].id > record.id; i--) {
            records[i + 1] = records[i]; // shift gia na kanw xwro
        }
        records[i + 1] = record;  // Insert record
        (*num_records)++;// Inc record count
        BF_Block_SetDirty(block); // dirty 
        return 0;    // Success
    }

    
    printf("No space in block. Split required for record ID: %d\n", record.id); // debug mhnuma
    return SplitLeafNode(file_desc, bplus_info, block, record);  // den exw xwro, kanw split

}

int TraverseToLeaf(int file_desc, int *current_block, BF_Block *block, int id) {
    while (1) {
        CALL_BF(BF_GetBlock(file_desc, *current_block, block)); // curr block
        char *data = BF_Block_GetData(block);                 
        char block_type = data[0];    // leaf h internal check

        if (block_type == 'L') {
            // an einai fullo, ftasame ston komvo pou thelkame If it's a leaf, we have arrived at the desired node
            // den kanw unpin, tha to kanei h synarthsh pou me kalese
            return 0;
        }

        // allios einai internal komvos, prepei na vrw to paidi pou tha akolouthisw 
        int *num_keys_ptr = (int *)(data + sizeof(char)); // num of keys ston eswteriko komvo
        int num_keys = *num_keys_ptr;      // copy key
        int *keys = (int *)(data + sizeof(char) + sizeof(int)); // pinakas keys
        int *children = (int *)((char *)keys + num_keys * sizeof(int)); // children pointer

        int i;
        for (i = 0; i < num_keys; i++) {
            if (id < keys[i]) {        // an to search id mas einai ligotero apo to kleidi
                *current_block = children[i]; // follow auto to paidi
                break;
            }
        }
        if (i == num_keys) {           // an den yparxei key megalutero apo to IDD
            *current_block = children[num_keys]; // akolouthw to teleutaio paidi
        }

        CALL_BF(BF_UnpinBlock(block)); // unpin prin sinexisw
        // to *current_block pleon deixnei to epomeno block pou tha mpoume
    }
}

// Create a new B+ tree file with metadata
int BP_CreateFile(char *fileName) {
    int file_desc;
    BF_Block *block;
    BF_Block_Init(&block); // init block

    CALL_BF(BF_CreateFile(fileName));            
    CALL_BF(BF_OpenFile(fileName, &file_desc));  
    CALL_BF(BF_AllocateBlock(file_desc, block)); 

    char *data = BF_Block_GetData(block);       
    memset(data, 0, BF_BLOCK_SIZE);      // init block data ws 0

    BPLUS_INFO *info = (BPLUS_INFO *)data;    
    strcpy(info->file_type, "B+T");  // file type gia elegxo
    info->root_block = -1;            // den exw riza akoma
    info->tree_height = 0;     // upsos = 0
    info->block_count = 1;   //1 block == metadata

    BF_Block_SetDirty(block);    // Mark metadata block as dirty
    CALL_BF(BF_UnpinBlock(block));      // Unpin metadata block
    BF_Block_Destroy(&block);   // Destroy block structure
    CALL_BF(BF_CloseFile(file_desc));   // Close the file

    return 0;  // success
}

BPLUS_INFO* BP_OpenFile(char *fileName, int *file_desc) {
    BF_Block *block;         // Block for metadata
    BPLUS_INFO *info;      

    if (BF_OpenFile(fileName, file_desc) != BF_OK) {
        fprintf(stderr, "Error opening file: %s\n", fileName);
        return NULL;
    }

    BF_Block_Init(&block);

    // metadata block
    if (BF_GetBlock(*file_desc, 0, block) != BF_OK) {
        fprintf(stderr, "Error reading metadata block: %s\n", fileName);
        BF_CloseFile(*file_desc);
        BF_Block_Destroy(&block);
        return NULL;
    }

    // memory gia to BPLUS_INFO sthn swro
    info = (BPLUS_INFO *)malloc(sizeof(BPLUS_INFO));
    if (info == NULL) {
        fprintf(stderr, "Memory allocation failed for BPLUS_INFO.\n");
        BF_UnpinBlock(block);
        BF_Block_Destroy(&block);
        BF_CloseFile(*file_desc);
        return NULL;
    }

    char *data = BF_Block_GetData(block); // get data
    memcpy(info, data, sizeof(BPLUS_INFO)); // copy data 
    
    // elegxo gia to arxeio
    if (strncmp(info->file_type, "B+T", 3) != 0) {
        fprintf(stderr, "Invalid file type for B+-tree: %s\n", fileName);
        free(info);
        BF_UnpinBlock(block);
        BF_Block_Destroy(&block);
        BF_CloseFile(*file_desc);
        return NULL;
    }

    // Unpin metadata block
    if (BF_UnpinBlock(block) != BF_OK) {
        fprintf(stderr, "Error unpinning metadata block.\n");
        free(info);
        BF_Block_Destroy(&block);
        BF_CloseFile(*file_desc);
        return NULL;
    }

    BF_Block_Destroy(&block);
    return info;   // Return metadata
}

int BP_CloseFile(int file_desc, BPLUS_INFO* info) {
    if (info != NULL) {
        free(info);     // free oti desmefsa sto open tou arxeiou
        info = NULL;
    }
    if (BF_CloseFile(file_desc) != BF_OK) {
        fprintf(stderr, "Error closing file descriptor: %d\n", file_desc);
        return -1; // Error
    }

    return 0; // Success
}

int BP_InsertEntry(int file_desc, BPLUS_INFO *bplus_info, Record record) {
    BF_Block *block;    // gia ta pins
    BF_Block_Init(&block);   // Init block

    int current_block = bplus_info->root_block;

    // if keno dentro
    if (current_block == -1) {
        printf("Tree is empty. Creating root node...\n");
        if (CreateRootNode(file_desc, bplus_info, record) != 0) {
            fprintf(stderr, "Failed to create root node.\n");
            BF_Block_Destroy(&block);
            return -1; // fail with root
        }
        BF_Block_Destroy(&block);
        return 0; // sucess
    }

    // Traverse sto leaf node ppou exei to record
    if (TraverseToLeaf(file_desc, &current_block, block, record.id) != 0) {
        // fail
        BF_UnpinBlock(block);
        BF_Block_Destroy(&block);
        return -1;
    }

    // to 'block' pleon dixnei sto leaf ppou ppreppei na baloume to record
    if (InsertIntoLeaf(file_desc, bplus_info, block, record) != 0) {
        // fail
        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
        return -1;
    }

    // Success 
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    return 0; 
}

int BP_GetEntry(int file_desc, BPLUS_INFO *bplus_info, int id, Record **result) {
    BF_Block *block;                     // gia to traversal

    BF_Block_Init(&block);               // Init block
    char *data;                          // Pointer to block data
    int current_block = bplus_info->root_block; // ksekina apo to root

    while (current_block != -1) {
        CALL_BF(BF_GetBlock(file_desc, current_block, block));
        data = BF_Block_GetData(block);

        char block_type = data[0];       // L h I
        printf("Traversing block ID: %d, Block Type: %c\n", current_block, block_type);

        if (block_type == 'L') {
            // an einai fullo, psakse to record
            int *num_records = (int *)(data + sizeof(char)); // Num of records sta fulla
            Record *records = (Record *)(data + sizeof(char) + sizeof(int)); // Pointer se records
            printf("Leaf Node. Searching for ID: %d\n", id);

            // search gia to leaf, dedomenou enos ID  ID
            for (int i = 0; i < *num_records; i++) {
                printf("Record[%d]: ID = %d\n", i, records[i].id);
                if (records[i].id == id) {
                    // vrika to record, malloc gia na to krahsw
                    *result = malloc(sizeof(Record));
                    if (*result == NULL) {
                        fprintf(stderr, "Memory allocation failed for result.\n");
                        CALL_BF(BF_UnpinBlock(block));
                        BF_Block_Destroy(&block);
                        return -1; // Mem error
                    }
                    memcpy(*result, &records[i], sizeof(Record)); // vazw to record pou vrika sto result 
                    CALL_BF(BF_UnpinBlock(block));
                    BF_Block_Destroy(&block);
                    printf("Record found: ID = %d\n", id);
                    return 0; // found
                }
            }
            printf("Record with ID %d not found in current leaf.\n", id);
            CALL_BF(BF_UnpinBlock(block));
            BF_Block_Destroy(&block);
            *result = NULL;
            return -1; // Not found
        }

        // an einai eswterikos komvos, prepei na apofasisoume poion child pointer tha akolouthisoume
        int *num_keys_ptr = (int *)(data + sizeof(char));   // Num of keys ston eswteriko komvo 
        int num_keys = *num_keys_ptr;                       // Copy
        int *keys = (int *)(data + sizeof(char) + sizeof(int)); // Keys array
        int *children = (int *)((char *)keys + num_keys * sizeof(int)); // Children pointers

        printf("Internal Node Keys: ");
        for (int i = 0; i < num_keys; i++) {
            printf("%d ", keys[i]);     // debug
        }
        printf("\n");

        int i;
        // vriskw to paidi me vasi to search ID
        for (i = 0; i < num_keys; i++) {
            if (id < keys[i]) {
                current_block = children[i]; // akoloutha auto to paidi an if ID < keys[i]
                break;
            }
        }
        if (i == num_keys) {
            // an to id einai megalutero apo ola ta keys, akolouthw to teleutaio child node
            current_block = children[num_keys];
        }

        CALL_BF(BF_UnpinBlock(block)); // Unpin
        // o epomenos komvos ginetai to current node ktlp...
    }

    // an vgoume me current_block==-1 tote to dentro einai adeio h den vrikame kanena record 
    printf("Record with ID %d not found in the tree.\n", id);
    BF_Block_Destroy(&block);
    *result = NULL;
    return -1; // Not found
}
