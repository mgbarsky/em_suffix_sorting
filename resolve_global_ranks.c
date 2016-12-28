#include "merge.h"


int resolve_global_ranks (char *temp_dir ) {
	MergeManager manager = {0};
	FILE *summaryFP=NULL;	
	char file_name [MAX_PATH_LENGTH];
	
	int i, result;

	sprintf(file_name, "%s/run", temp_dir);
	strcpy (manager.input_prefix, file_name);	

	sprintf (file_name, "%s/merge_summary", temp_dir);
	OpenBinaryFileRead (&summaryFP, file_name);
	fseek (summaryFP, 0, SEEK_END);  
	manager.total_files=ftell (summaryFP)/sizeof(RunID);
	rewind(summaryFP);
	
	
	//allocate inputFileNames
	manager.inputFileNumbers = (RunID *) Calloc (manager.total_files*sizeof(RunID));;
	result = fread(manager.inputFileNumbers, sizeof(RunID), manager.total_files, summaryFP);
	fclose (summaryFP);
	if (result != manager.total_files) {
		printf ("Error reading summary file %s - wanter %d record, but found %d\n",file_name, manager.total_files, result);
		return FAILURE;
	}
	
	if (result == 0)
		return EMPTY;
		
	//allocate input buffers
	manager.inputBuffers = (RunRecord **) Calloc (manager.total_files * sizeof (RunRecord *));
	manager.inputBufferCapacity = MAX_MEM_INPUT_BUFFERS/(sizeof(INPUT_ELEMENT_T)*(manager.total_files));
	for (i=0; i<manager.total_files;i++)
		manager.inputBuffers [i] = (RunRecord *) Calloc (manager.inputBufferCapacity *sizeof(RunRecord));

	//allocate position pointers
	manager.currentInputFilePositions = (int *) Calloc (manager.total_files * sizeof(int));
	manager.currentInputBufferPositions  = (int *) Calloc (manager.total_files * sizeof(int));
	manager.currentInputBufferlengths  = (int *) Calloc (manager.total_files * sizeof(int));
	
	//allocate output buffers - multiple in this algorithm
	manager.outputBufferCapacity =  MAX_MEM_OUTPUT_BUFFERS/(sizeof(OutputElement)*(manager.total_files));
	manager.outputBuffers = (OutputElement **) Calloc (manager.total_files * sizeof (OutputElement*));
	for (i=0; i<manager.total_files;i++)
		manager.outputBuffers [i] = (OutputElement *) Calloc (manager.outputBufferCapacity *sizeof(OutputElement));
 	
	manager.currentOutputBufferPositions  = (int *) Calloc (manager.total_files * sizeof(int));	

	//allocate heap
	manager.heap = (HeapElement *) Calloc (manager.total_files * sizeof (HeapElement));
	manager.heapCapacity = manager.total_files;
	manager.currentHeapSize = 0;
	
	manager.outputFP = NULL;
	manager.inputFP = NULL;	
	
	strcpy(manager.output_dir, temp_dir);
	return merge_runs (&manager);
}

