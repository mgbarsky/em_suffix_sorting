#include "utils.h"
#include "algorithm.h"

int update_local_ranks (char * ranks_dir, char * temp_dir) {
	FILE * local_interval_FP = NULL;
	FILE * global_resolved_FP = NULL;
	FILE * current_FP = NULL;
	FILE *summaryFP = NULL;
	long *override_buffer = NULL;

	RunID * intervals;
	LocalRecord *local_records;
	GlobalRecord *global_records;

	int all_processed = 2;
	int total_local;
	int total_resolved;
	
	char file_name[MAX_PATH_LENGTH];
	char local_file_name[MAX_PATH_LENGTH];
	int i,result, total_intervals, current_file_id, current_interval_id, prev_file_id = -1, local_pos, resolved_pos, d;
	int interval_start, interval_end;
	int local_buffer_start=-1, local_buffer_end;
	int total_to_update;

	LocalRecord curr_local = {0};
	GlobalRecord curr_global = {0};
	
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
	override_buffer = (long *) Calloc (DEFAULT_TRIPLE_BUFFER_SIZE * sizeof (long));

	for (i=0; i< total_intervals; i++) {
		current_file_id = intervals[i].file_id;
		current_interval_id = intervals[i].interval_id;

		interval_start = current_interval_id * DEFAULT_TRIPLE_BUFFER_SIZE;
		
		//open input file with local ranks from previous iteration
		if (current_file_id != prev_file_id ) {
			if (current_FP != NULL )
				fclose (current_FP);
			sprintf (file_name, "%s/ranks_%d", ranks_dir, current_file_id);
			OpenBinaryFileReadWrite (&current_FP, file_name);
			prev_file_id = current_file_id;
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
			fclose (local_interval_FP);

			//read content of global resolved into buffer
			fseek (global_resolved_FP, 0, SEEK_END);  
			total_resolved = ftell (global_resolved_FP)/sizeof(GlobalRecord);
			rewind(global_resolved_FP);
			result = fread (global_records, sizeof (GlobalRecord), total_resolved, global_resolved_FP);
			if (result != total_resolved) {
				printf ("Error reading global resolved ranks file %s: wanted to read %d but fread returned %d\n", file_name, total_resolved,result);
				return FAILURE;
			}
			fclose (global_resolved_FP);

			//now go over 2 buffers - local and resolved - and update global ranks in current interval o verride buffer, if they are resolved - merge
			//first of all -read corresponding interval from current ranks file
			//move pointer to the update position
			fseek ( current_FP , interval_start*sizeof(long) , SEEK_SET);

			//prepare the ovewrride buffer
			result = fread(override_buffer, sizeof(long), DEFAULT_TRIPLE_BUFFER_SIZE, current_FP);
			total_to_update = result;

			//move file pointer back to prepare for writing
			fseek ( current_FP , -total_to_update*sizeof(long) , SEEK_CUR );
			
			resolved_pos = 0;
			local_pos = 0;
			curr_local.currentRank = 0;
			curr_global.currentRank =0;

			while (local_pos < total_local && resolved_pos < total_resolved) {
				if (curr_local.currentRank == 0 )  //reading the first record from each buffer
					curr_local = local_records[local_pos];
				if (curr_global.currentRank == 0)
					curr_global = global_records [resolved_pos];

				if (curr_local.currentRank == curr_global.currentRank && curr_local.nextRank == curr_global.nextRank) {
					int pos_in_override_buf = curr_local.pos - interval_start;
					override_buffer [pos_in_override_buf] = curr_global.newRank;
					local_pos++;
					if (local_pos < total_local) {
						curr_local = local_records[local_pos];
					}
				}
				else { //they are not equal
					//current local does not have resolved counterpart - advance to the next current local
					if (curr_local.currentRank < curr_global.currentRank || (curr_local.currentRank == curr_global.currentRank && Absolute(curr_local.nextRank) < Absolute(curr_global.nextRank))){
						local_pos++;
						if (local_pos < total_local) {
							curr_local = local_records[local_pos];
						}						
					}
					//this global rank has been used by all local ranks - move to the next resolved
					else {
						resolved_pos++;
						if (resolved_pos < total_resolved)
							curr_global = global_records [resolved_pos];
					}
				}
			}
			Fwrite (override_buffer, sizeof(long), total_to_update, current_FP);
			if (DEBUG_SMALL) {
				printf("After current iteration updated ranks for file %d are:\n", current_file_id);
				for (d=0; d< total_to_update; d++) {
					printf ("%ld ", override_buffer[d]);
				}
				printf("\n");
			}
		}
	}
		
	if (current_FP != NULL )
		fclose (current_FP);	
	
	free (local_records);
	free (global_records);
	free (override_buffer);

	return all_processed;
}