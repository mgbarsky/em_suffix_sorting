#ifndef MERGE_H
#define MERGE_H

#include "merge_ranks.h"  //this is a specific definitions for merging ranks
#define HEAP_ELEMENT_T HeapElement
#define INPUT_ELEMENT_T RunRecord
#define OUTPUT_ELEMENT_T OutputElement
#define RUN_ID_T RunID

#define OUTPUT_BUFFER_SIZE 1024
#define MAX_MEM_INPUT_BUFFERS 512000000  //~500 MB
#define MAX_MEM_OUTPUT_BUFFERS 512000000 //~500 MB 

typedef struct merge_manager {
	HEAP_ELEMENT_T *heap;  //keeps 1 from each buffer in top-down order - smallest on top (according to compare function)
	HEAP_ELEMENT_T lastTransferred; //last element transferred from heap to output buffer - TBD maybe not needed
	FILE *inputFP; //stays closed, opens each time we need to reupload some amount of data from disk
	RUN_ID_T *inputFileNumbers;  //we need to know the file name and interval to open the necessary input file	
	FILE *outputFP;  
	OUTPUT_ELEMENT_T** outputBuffers; //buffer to store output elements until they are flushed to disk
	int *currentOutputBufferPositions;  //where to add element in each output buffer
	int outputBufferCapacity; //how many elements max can it hold
	INPUT_ELEMENT_T **inputBuffers; //array of buffers to buffer part of runs
	int inputBufferCapacity; //how many elements max can each hold
	int *currentInputFilePositions; //current position in each file, -1 if the run is complete
	int *currentInputBufferPositions; //position in current input buffer, if no need - -1
	int *currentInputBufferlengths;  //number of actual elements currently in input buffer - can be less than max capacity
	int currentHeapSize;
	int heapCapacity;  //corresponds to the total number of files (input buffers)
	int total_files;
	char output_dir [MAX_PATH_LENGTH];
	char input_prefix [MAX_PATH_LENGTH] ;
}MergeManager;

//1
int merge_runs (MergeManager * manager); //main loop

//2 
int init_merge (MergeManager * manager);  //creates and fills initial buffers, initializes heap taking 1 element from each buffer

//3
//int flush_output_buffer (MergeManager * manager);
int flush_output_buffers (MergeManager * manager, int file_number); //special for this app - multiple buffers for output

//4
int get_top_heap_element (MergeManager * manager, HEAP_ELEMENT_T * result);

//5
int insert_into_heap (MergeManager * manager, int file_number, INPUT_ELEMENT_T *input);

//6
int get_next_input_element(MergeManager * manager, int file_number, INPUT_ELEMENT_T *result);

//7
int refill_buffer (MergeManager * manager, int file_number);

void clean_up (MergeManager * merger);


//merge-specific allocations
INPUT_ELEMENT_T ** allocate_input_buffers (int num_elements);
INPUT_ELEMENT_T * allocate_input_buffer (int num_elements);

OUTPUT_ELEMENT_T *allocate_output_buffer (int num_elements);
HEAP_ELEMENT_T *allocate_heap (int num_elements);

//application-specific conversions and processing
int input_to_heap (INPUT_ELEMENT_T *input, int file_number, HEAP_ELEMENT_T *result);
int heap_to_output ( HEAP_ELEMENT_T *previous, HEAP_ELEMENT_T *current, OUTPUT_ELEMENT_T *resolved);
int compare_heap_elements (HEAP_ELEMENT_T *a, HEAP_ELEMENT_T *b);
int valid_output (OUTPUT_ELEMENT_T *outputRecord);
int heap_to_output_last_element (HEAP_ELEMENT_T *previous, HEAP_ELEMENT_T *current, OUTPUT_ELEMENT_T *resolved);
#endif