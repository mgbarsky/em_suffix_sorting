#include "utils.h"
#include "algorithm.h"

int update_one_interval (FILE *currentFP, LocalRecord *local_records,GlobalRecord *global_records){
	return SUCCESS;
}

int update_local_ranks (char * ranks_dir, char * temp_dir) {

	FILE * local_interval_FP = NULL;
	FILE * global_resolved_FP = NULL;
	FILE * current_FP = NULL;
	FILE *summaryFP = NULL;
	RunID * intervals;
	LocalRecord *local_records;
	GlobalRecord *global_records;

	int all_processed = 2;
	int total_local;
	int total_resolved;
	int prev_pos_infile = -1;

	char file_name[MAX_PATH_LENGTH];
	char local_file_name[MAX_PATH_LENGTH];
	int i, result, total_intervals, current_file_id, current_interval_id, prev_file_id = -1;

	sprintf (file_name, "%s/merge_summary", temp_dir);

	if(!(summaryFP= fopen ( file_name , "rb" )))	{
		return EMPTY;
	}

	fseek (summaryFP, 0, SEEK_END);  
	total_intervals = ftell (summaryFP)/sizeof(RunID);
	rewind(summaryFP);
	
	//allocate input chunk interval info
	intervals = (RunID *) Calloc (total_intervals*sizeof(RunID));
	result = fread(intervals, sizeof(RunID), total_intervals, summaryFP);
	fclose (summaryFP);


	//allocate local and global buffers of maximum size
	local_records = (LocalRecord *)Calloc (DEFAULT_TRIPLE_BUFFER_SIZE*sizeof(LocalRecord));
	global_records = (GlobalRecord *)Calloc (DEFAULT_TRIPLE_BUFFER_SIZE*sizeof(GlobalRecord));

	for (i=0; i< total_intervals; i++) {
		current_file_id = intervals[i].file_id;
		current_interval_id = intervals[i].interval_id;

		//open input file with local ranks from previous iteration
		if (current_file_id != prev_file_id ) {
			if (current_FP != NULL )
				fclose (current_FP);
			sprintf (file_name, "%s/ranks_%d", ranks_dir, current_file_id);
			OpenBinaryFileReadWrite (&current_FP, file_name);
			prev_file_id = current_file_id;
			prev_pos_infile = -1;
		}
		
		//open file where pos, current, next are sorted by curr, next
		sprintf (local_file_name, "%s/local_%d_%d", temp_dir, current_file_id, current_interval_id);
		OpenBinaryFileRead (&local_interval_FP, local_file_name);
		
		
		//open file with global updates - if exists
		sprintf (file_name, "%s/global_%d_%d", temp_dir, current_file_id, current_interval_id);
		if((global_resolved_FP = fopen ( file_name , "rb" )))	{
			all_processed = 0;
			//read content of local into buffer
			fseek (local_interval_FP, 0, SEEK_END);  
			total_local = ftell (local_interval_FP)/sizeof(LocalRecord);
			rewind(local_interval_FP);
			result = fread (local_records, sizeof (LocalRecord), total_local, local_interval_FP);
			if (result != total_local) {
				printf ("Error reading local file %s with triples: wanted to read %d but fread returned %d\n", local_file_name, total_local,result);
				return FAILURE;
			}

			//read content of global resolved into buffer
			fseek (global_resolved_FP, 0, SEEK_END);  
			total_resolved = ftell (global_resolved_FP)/sizeof(GlobalRecord);
			rewind(global_resolved_FP);
			result = fread (global_records, sizeof (GlobalRecord), total_resolved, global_resolved_FP);
			if (result != total_resolved) {
				printf ("Error reading global resolved ranks file %s: wanted to read %d but fread returned %d\n", file_name, total_resolved,result);
				return FAILURE;
			}

			//now go over 2 buffers - local and resolved - and update input file in the current position

			fclose (global_resolved_FP);
		}
		fclose (local_interval_FP);
	}
		
	if (current_FP != NULL )
		fclose (current_FP);	
	
	
/*
	while (1) {
		if (solution_next > original_next) {			
			if (read_original(originalFP, &pos, &original_next)==-1) {
				// no more original text to read, update done
				break;
			}
		} else if (solution_next == original_next) {
			// update
			bucket_id = pos / START_POS_INTERVAL;
			if ( bucket_id >= NUM_INTERVALS ) {
				printf ("Input too big: need bucket number %d each with %d elements\n", 
					bucket_id, START_POS_INTERVAL );
				exit (1);
			}
			if (outputFPs [bucket_id] == NULL) {
				sprintf (output_filename, "%s/%d", output_prefix, bucket_id);
				append_file_open(&outputFPs[bucket_id], output_filename);
			}
			fprintf(outputFPs [bucket_id], "%d,%ld\n", pos, update_rank);

			if (read_original(originalFP, &pos, &original_next)==-1) {
				// no more original text to read, update done
				break;
			}
		} else { // solution_next < original_next
			if (read_solution(solutionFP, &solution_next, &update_rank)==-1) { 
				// solution read done, no more updates, write the rest as it is				
				break;
			}
		}
	}
	*/
	return all_processed;
}