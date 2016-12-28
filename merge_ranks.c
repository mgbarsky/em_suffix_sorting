#include "merge.h"

//application-specific variables
long base_current_rank; // rank we update as we go over counts
long current_rank; //original rank preserved for output back

static long counts_so_far; //how many counts we seen for this prev_rank so far

int valid_output (OUTPUT_ELEMENT_T *outputRecord) {
	int ret = (outputRecord->file_number == -1)? 0: 1;
	return ret;
}

int heap_to_output_last_element (HEAP_ELEMENT_T *previous, HEAP_ELEMENT_T *current, OUTPUT_ELEMENT_T *resolved) {
	int sign =1;
	resolved->output.currentRank = current->currentRank;
	resolved->output.nextRank = current->nextRank;
	
	if (previous->file_number == -1 && current->count == 1)
		sign =-1;
	if (previous->file_number != -1 &&  current->currentRank!= previous->currentRank)
		base_current_rank = current->currentRank;
	if (previous->file_number != -1 &&  current->currentRank== previous->currentRank 
				&& current->nextRank!= previous->nextRank && current->count == 1)
		sign =-1;
	if (current->nextRank <= 0)		
		sign =-1;
	
	resolved->output.newRank = sign * base_current_rank;
	resolved->file_number = current->file_number;

	return SUCCESS;
}

//we are comparing 2 heap elements by current rank, then by next rank, if equal - by file_id
int compare_heap_elements (HEAP_ELEMENT_T *a, HEAP_ELEMENT_T *b) {
	if (a->currentRank == b->currentRank ) {
		if (a->nextRank == b->nextRank)
			return a->file_number - b->file_number;
		return Absolute(a->nextRank) - Absolute (b->nextRank);
	}
	
	return a->currentRank - b->currentRank;
}

int heap_to_output ( HEAP_ELEMENT_T *previous, HEAP_ELEMENT_T *current, OUTPUT_ELEMENT_T *result) {
	
	result->file_number = -1;
	
	if (previous->file_number == -1) {		
		counts_so_far = current->count;		
		return SUCCESS;
	}

	if (current->currentRank == previous->currentRank) {
		if (current->nextRank == previous->nextRank) {
			counts_so_far += current->count;
		
			result->file_number = previous->file_number;
			result->output.currentRank = previous->currentRank;
			result->output.nextRank = previous->nextRank;
			result->output.newRank = base_current_rank;
			return SUCCESS;
		}

		//current rank is the same, but next ranks are different
		result->file_number = previous->file_number;
		result->output.currentRank = previous->currentRank;
		result->output.nextRank = previous->nextRank;
		result->output.newRank = counts_so_far == 1? -base_current_rank: base_current_rank;	
		base_current_rank += counts_so_far;
	
		counts_so_far = current->count;
	
		return SUCCESS;
	}

	//current rank of current is different from current rank of previous
	result->file_number = previous->file_number;
	result->output.currentRank = previous->currentRank;
	result->output.nextRank = previous->nextRank;
	result->output.newRank = counts_so_far == 1? -base_current_rank: base_current_rank;	
	
	base_current_rank = current->currentRank;
	
	counts_so_far = current->count;
	
	return SUCCESS;
	
}

int flush_output_buffers (MergeManager *merger, int f_id) {
	int i;
	char file_name [MAX_PATH_LENGTH];
	int total_to_write = merger->currentOutputBufferPositions[f_id];
	RunID id = merger->inputFileNumbers [f_id];
	sprintf (file_name, "%s/global_%d_%ld", merger->output_dir, 
				id.file_id, id.interval_id);
	OpenBinaryFileAppend (&(merger->outputFP), file_name); 
	
	for (i=0; i< merger->currentOutputBufferPositions[f_id]; i++) { 
		GlobalRecord record = merger->outputBuffers[f_id][i].output;	

		if (record.newRank != record.currentRank) { 			
			Fwrite (&record, sizeof (GlobalRecord), 1, merger->outputFP);
		}
	}
	
	fclose (merger->outputFP);
	merger->outputFP = NULL;
	
	return SUCCESS;
}