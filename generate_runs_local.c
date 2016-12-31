#include "utils.h"
#include "algorithm.h"

/* it will read chunk of ranks file into a buffer
** for each such buffer it will generate following 2 local outputs:
** triples (pos, curr, next), sorted by curr, next into file local_fileid_bufferid
** triples (curr, next, count) into file run_fileid_bufferid
** at most MAX_BUFFERS_PERFILE are allowed for each file - to combat the limitation of the maximum number of files
** returns EMPTY if all ranks are resoved
*/

int compare_current_next (const void * a, const void *b) {
	LocalRecord *r_a = (LocalRecord *) a;
	LocalRecord *r_b = (LocalRecord *) b;

	if (r_a->currentRank > r_b->currentRank)
		return 1;

	if (r_a->currentRank < r_b->currentRank)
		return -1;

	if (Absolute (r_a->nextRank) > Absolute (r_b->nextRank))
		return 1;
	if (Absolute (r_a->nextRank) < Absolute (r_b->nextRank))
		return -1;

	return r_a->pos - r_b->pos;

}

void flush_local_run (char *temp_dir, int file_id, int buffer_id, LocalRecord *output_buffer, int num_elements){
	FILE *runFP=NULL;
	FILE *localFP=NULL;
	int i;
	int count = 0;
	long prev_current = -1;
	long prev_next = -1;
	char run_file_name [MAX_PATH_LENGTH];
	char local_file_name [MAX_PATH_LENGTH];
	RunRecord record;

	sprintf (run_file_name, "%s/run_%d_%d", temp_dir, file_id, buffer_id);
	sprintf (local_file_name, "%s/local_%d_%d", temp_dir, file_id, buffer_id);

	OpenBinaryFileWrite (&runFP, run_file_name);
	OpenBinaryFileWrite (&localFP, local_file_name);

	qsort (output_buffer, num_elements, sizeof (LocalRecord), compare_current_next);

	if (DEBUG_SMALL) {
		printf ("Sorted local records:\n");
		for (i=0; i < num_elements; i++) {
			printf ("pos=%d curr=%ld next=%ld\n", output_buffer[i].pos, output_buffer[i].currentRank, output_buffer[i].nextRank);
		}


	}
	Fwrite (output_buffer, sizeof(LocalRecord), num_elements, localFP);
	for (i=0; i < num_elements; i++) {
		if (output_buffer[i].currentRank != prev_current ) {			 
			if (prev_current != -1) { //collected counts
				record.currentRank = prev_current;
				record.nextRank = prev_next;
				record.count = count;
				if (DEBUG_SMALL) 	printf ("Run record curr=%ld next=%ld count=%d\n", record.currentRank, record.nextRank, record.count);	
				Fwrite (&record, sizeof(RunRecord), 1, runFP);
				count=0;
			}		
		}
		else if (output_buffer[i].currentRank == prev_current && output_buffer[i].nextRank != prev_next) {
			record.currentRank = prev_current;
			record.nextRank = prev_next;
			record.count = count;
			if (DEBUG_SMALL) 	printf ("Run record curr=%ld next=%ld count=%d\n", record.currentRank, record.nextRank, record.count);			
			Fwrite (&record, sizeof(RunRecord), 1, runFP);
			count=0;
		}
		prev_current = output_buffer[i].currentRank;
		prev_next = output_buffer[i].nextRank;
		count++;
	}
	
	//write the last record
	record.currentRank = prev_current;
	record.nextRank = prev_next;
	record.count = count;
	if (DEBUG_SMALL) 	printf ("Run record curr=%ld next=%ld count=%d\n", record.currentRank, record.nextRank, record.count);	
	Fwrite (&record, sizeof(RunRecord), 1, runFP);

	fclose (runFP);
	fclose (localFP);
}

int generate_runs_single_file (char * current_ranks_file_name, char *temp_dir, int file_id, int h, FILE *summaryFP) {
	int pos_infile;
	int next_pos_infile;
	int pos_in_buffer;
	int next_pos_in_buffer;
	long *input_buffer;
	int total_in_buffer;
	int total_to_process;
	FILE *inputFP=NULL;
	
	int read_result;
	int pos_output_buffer = 0;
	RunID summary_record;

	int buffer_id;
	LocalRecord *output_buffer_local;
	
	int prefix_len;
	int no_more_runs = 1;

	//determine next pos shift from the current
	prefix_len = (1 << h);

	//allocate buffers
	input_buffer = (long *) Calloc ((DEFAULT_TRIPLE_BUFFER_SIZE+prefix_len)*sizeof(long));
	output_buffer_local = (LocalRecord *) Calloc ((DEFAULT_TRIPLE_BUFFER_SIZE + prefix_len) *sizeof (LocalRecord));


	//open ranks file
	OpenBinaryFileRead (&inputFP, current_ranks_file_name);
	pos_infile = 0;
	buffer_id = 0;

	//read ranks into input buffer
	while ((read_result = fread (input_buffer, sizeof (long), (DEFAULT_TRIPLE_BUFFER_SIZE + prefix_len),inputFP))>prefix_len) {
		total_in_buffer = read_result;
		total_to_process = read_result - prefix_len;
		pos_in_buffer = 0;
		next_pos_in_buffer = prefix_len;
		for (pos_in_buffer=0; pos_in_buffer < total_to_process; pos_in_buffer++) {
			
			if (input_buffer[pos_in_buffer] > 0 ) {
				no_more_runs = 0;
				//generate LocalRecords and fill output buffer
				output_buffer_local[pos_output_buffer].currentRank = input_buffer[pos_in_buffer];
				output_buffer_local[pos_output_buffer].nextRank = input_buffer[pos_in_buffer + prefix_len];
				output_buffer_local[pos_output_buffer].pos = pos_infile;
				pos_output_buffer++;

				if (pos_output_buffer == DEFAULT_TRIPLE_BUFFER_SIZE) {
					flush_local_run (temp_dir, file_id, buffer_id, output_buffer_local, pos_output_buffer);
					
					summary_record.file_id = file_id;
					summary_record.interval_id = buffer_id;
					Fwrite (&summary_record, sizeof(RunID), 1, summaryFP);
					buffer_id ++;
					pos_output_buffer=0;
				}
			}
			pos_infile++;
		}

		//we need to go backwards - we read prefix_len more characters that we needed	
		//printf ("Curr pos in file %d\n",ftell(inputFP));
		fseek (inputFP, - prefix_len*sizeof(long), SEEK_CUR);
	}

	if (pos_output_buffer > 0) {
		flush_local_run (temp_dir, file_id, buffer_id, output_buffer_local, pos_output_buffer);
		summary_record.file_id = file_id;
		summary_record.interval_id = buffer_id;
		Fwrite (&summary_record, sizeof(RunID), 1, summaryFP);
	}

	fclose (inputFP);

	free (input_buffer);
	free (output_buffer_local);

	if (no_more_runs)
		return EMPTY;
	return SUCCESS;
}

int generate_local_runs (char * input_dir, char * temp_dir, int num_files, int h) {
	FILE *summaryFP;
	char summary_file_name [MAX_PATH_LENGTH];
	int f;
	char input_file_name [MAX_PATH_LENGTH];
	int no_more_runs = 1;
	int ret;


	sprintf (summary_file_name, "%s/merge_summary", temp_dir);
	OpenBinaryFileWrite (&summaryFP, summary_file_name);

	for (f=0; f < num_files; f++) {
		sprintf (input_file_name, "%s/ranks_%d", input_dir, f);
		ret = generate_runs_single_file (input_file_name, temp_dir, f, h, summaryFP);
		if (ret != EMPTY)
			no_more_runs = 0;
		if (ret == FAILURE)
			return FAILURE;
	}

	fclose (summaryFP);
	if (no_more_runs)
		return EMPTY;
	return SUCCESS;
}