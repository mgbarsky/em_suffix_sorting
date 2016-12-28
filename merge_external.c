#include "merge.h"

//manager fields should be already initialized in the caller
int merge_runs (MergeManager * merger){	
	int  result; //stores SUCCESS/FAILURE returned at the end
	OUTPUT_ELEMENT_T output_result;
	int f_id ;

	//1. go in the loop through all input files and fill-in initial buffers
	if (init_merge (merger)!=SUCCESS)
		return FAILURE;

	output_result.file_number = -1;
	merger->lastTransferred.file_number = -1;

	while (merger->currentHeapSize > 0) { //heap is not empty
		HEAP_ELEMENT_T smallest;
		INPUT_ELEMENT_T next; //here next is of input_type
		
		if(get_top_heap_element (merger, &smallest)!=SUCCESS)
			return FAILURE;

		result = get_next_input_element (merger, smallest.file_number, &next);
		
		if (result==FAILURE)
			return FAILURE;

		if(result==SUCCESS) {//next element exists
			if (merger->lastTransferred.file_number == -1 )
				base_current_rank = smallest.currentRank;
			if(insert_into_heap (merger,smallest.file_number, &next)!=SUCCESS)
				return FAILURE;
		}			
		
		if(heap_to_output ( &(merger->lastTransferred),  &smallest, &output_result)!=SUCCESS)
			return FAILURE;

		if (valid_output(&output_result)) {  //app-specific	
			f_id = output_result.file_number;
			merger->outputBuffers[f_id][merger->currentOutputBufferPositions[f_id]]=output_result;
			merger->currentOutputBufferPositions[f_id]++;

			//staying on the last slot of the output buffer - next will cause overflow
			if(merger->currentOutputBufferPositions[f_id] == merger-> outputBufferCapacity ) {
				if(flush_output_buffers(merger, f_id)!=SUCCESS)
					return FAILURE;			
				merger->currentOutputBufferPositions[f_id]=0;
			}	
		}

		if (merger->currentHeapSize == 0) { //last heap element
			heap_to_output_last_element (&(merger->lastTransferred), &smallest,  &output_result);
			
			f_id = output_result.file_number;
			if(merger->currentOutputBufferPositions[f_id] == merger-> outputBufferCapacity ) {
				if(flush_output_buffers(merger, f_id)!=SUCCESS)
					return FAILURE;			
				merger->currentOutputBufferPositions[f_id]=0;
			}	
			merger->outputBuffers[f_id][merger->currentOutputBufferPositions[f_id] ]=output_result;
			merger->currentOutputBufferPositions[f_id] ++;
		}

		merger->lastTransferred = smallest;		
	}

	
	//flush what remains in output buffer
	for (f_id=0; f_id< merger->total_files; f_id++) {
		if(merger->currentOutputBufferPositions[f_id] > 0) {
			if(flush_output_buffers(merger, f_id)!=SUCCESS)
				return FAILURE;
		}
	}
	
	if (DEBUG) printf("Merge complete.\n");
	clean_up(merger);
	return SUCCESS;	
}


int init_merge (MergeManager * merger) {
	int i;
	INPUT_ELEMENT_T first = {0};
	
	for(i=0;i<merger->heapCapacity;i++) {
		if (refill_buffer(merger,i) == FAILURE){
			fprintf(stderr, "Failed to fill initial buffer %d\n",i);
			return FAILURE;
		}
	}

	for (i=0;i<merger->heapCapacity;i++) {	
		//get element from each buffer		
		if(get_next_input_element (merger,i, &first)==FAILURE) //at least 1 element should exist
			return FAILURE;
		
		//insert it into heap
		if(insert_into_heap (merger, i, &first)!=SUCCESS)
			return FAILURE;
	}	
	return SUCCESS;
}


int get_top_heap_element (MergeManager * merger, HEAP_ELEMENT_T * result){
	HEAP_ELEMENT_T item;
	int child, parent;

	if(merger->currentHeapSize == 0){
		printf( "UNEXPECTED ERROR: popping top element from an empty heap\n");
		return FAILURE;
	}

	*result=merger->heap[0];  //to be returned

	//now we need to reorganize heap - keep the smallest on top
	item = merger->heap [--merger->currentHeapSize]; // to be reinserted 

	parent =0;
	while ((child = (2 * parent) + 1) < merger->currentHeapSize) {
		// if there are two children, compare them 
		if (child + 1 < merger->currentHeapSize && 
				(compare_heap_elements(&(merger->heap[child]),&(merger->heap[child + 1]))>0)) 
			++child;
		
		// compare item with the larger 
		if (compare_heap_elements(&item, &(merger->heap[child]))>0) {
			merger->heap[parent] = merger->heap[child];
			parent = child;
		} 
		else 
			break;
	}
	merger->heap[parent] = item;
	
	return SUCCESS;
}



int insert_into_heap (MergeManager * merger, int file_number, INPUT_ELEMENT_T *input){

	HEAP_ELEMENT_T newHeapElement;
	int child, parent;

	if (input_to_heap (input, file_number, &newHeapElement) != SUCCESS)
		return FAILURE;

	if (merger->currentHeapSize == merger->heapCapacity) {
		printf( "Unexpected ERROR: heap is full\n");
		return FAILURE;
	}
  	
	child = merger->currentHeapSize++; /* the next available slot in the heap */
	
	while (child > 0) {
		parent = (child - 1) / 2;
		if (compare_heap_elements(&(merger->heap[parent]),&newHeapElement)>0) {
			merger->heap[child] = merger->heap[parent];
			child = parent;
		} 
		else 
			break;
	}
	merger->heap[child]= newHeapElement;	
	return SUCCESS;
}

int get_next_input_element (MergeManager * merger, int file_number, 
			INPUT_ELEMENT_T *result){
	
	if(merger->currentInputBufferPositions[file_number] == -1) //run is complete
		return EMPTY; //empty
	
	//there are still elements in the buffer		
	if(merger->currentInputBufferPositions[file_number] < merger->currentInputBufferlengths[file_number]) 	
		*result=  merger->inputBuffers[file_number][merger->currentInputBufferPositions[file_number]++];		
	else {
		int refillResult = refill_buffer (merger, file_number);
		if(refillResult==SUCCESS)			
			return get_next_input_element (merger,  file_number, result);
		else 
			return refillResult;
	}
	return SUCCESS; //success
}

int input_to_heap (INPUT_ELEMENT_T *input, int file_number, HEAP_ELEMENT_T *result){	
	result->currentRank = input->currentRank;
	result->nextRank = input->nextRank;
	result->count = input->count;
	result->file_number = file_number;
	return SUCCESS;
}

void clean_up(MergeManager * merger)
{
	int i;
	for (i=0; i<merger->total_files;i++)
		free(merger->inputBuffers [i]);
	free(merger->inputBuffers);

	free(merger->currentInputFilePositions);
	free(merger->currentInputBufferPositions);
	free(merger->currentInputBufferlengths);
	for (i=0; i<merger->total_files;i++)
		free(merger->outputBuffers [i]);
	free(merger->outputBuffers);
	free(merger->currentOutputBufferPositions);
	free(merger->heap);
	free(merger->inputFileNumbers);
}



int refill_buffer (MergeManager * merger, int file_number) {
	
	char currentInputFileName[MAX_PATH_LENGTH];	
	int result;	

	if(merger->currentInputFilePositions[file_number] == -1) {
		merger->currentInputBufferPositions[file_number] = -1; //signifies no more elements
		return EMPTY; //run is complete - no more elements in the input file
	}

	sprintf(currentInputFileName, "%s/run_%d_%d" , merger->output_dir, merger->inputFileNumbers[file_number].file_id, 
		merger->inputFileNumbers[file_number].interval_id);
	
	OpenBinaryFileRead (&(merger->inputFP), currentInputFileName);
	result = fseek ( merger->inputFP , merger->currentInputFilePositions[file_number]*sizeof (RunRecord) , SEEK_SET );

	if (result!=SUCCESS) {
		printf ("fseek failed on file %s trying to move to position %ld\n", currentInputFileName, 
			merger->currentInputFilePositions[file_number]*sizeof (RunRecord) );
		return FAILURE;
	}

	if ((result = fread (merger->inputBuffers[file_number], sizeof (RunRecord), merger->inputBufferCapacity, merger->inputFP)) > 0) {
		merger->currentInputBufferPositions[file_number] = 0;
		merger->currentInputBufferlengths [file_number] = result;
		merger->currentInputFilePositions [file_number] += result;

		fclose(merger->inputFP);
		merger->inputFP = NULL;

		if (result < merger->inputBufferCapacity) //no more reads
			merger->currentInputFilePositions [file_number] = -1;
		return SUCCESS;
	}
	
	//no more elements - we read exactly until the end of the file in the previous upload
	merger->currentInputFilePositions [file_number] = -1;
	merger->currentInputBufferPositions[file_number] = -1;
	fclose(merger->inputFP);
	merger->inputFP = NULL;

	return EMPTY;	
}


