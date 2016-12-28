#include "utils.h"

// count number of characters in binary file and write result into a file as 255-size array of counts
void count_characters_infile (char * input_file_name, char * output_file_name) {
	char *buffer;
	int i;
	FILE * inputFP=NULL; FILE * outputFP = NULL;
	int read;
	long *char_counts = (long *) Calloc (MAX_CHAR*sizeof(long));
	buffer = (char *) Calloc (DEFAULT_CHAR_BUFFER_SIZE*sizeof(char));

	OpenBinaryFileRead (&inputFP, input_file_name);
	while ((read = fread (buffer,sizeof(char),DEFAULT_CHAR_BUFFER_SIZE,inputFP)) > 0){		
		for (i=0; i < read; i++) {
			char current = (buffer[i]>=0)?buffer[i]:-buffer[i];
			char_counts [(int)current] ++;
		}
	}
	
	fclose(inputFP);
	OpenBinaryFileWrite (&outputFP, output_file_name);
	Fwrite (char_counts, sizeof(long), MAX_CHAR, outputFP);
	fclose (outputFP);

	free (buffer);
	free (char_counts);
}


int count_characters (char *input_directory, int num_files, char * temp_directory) {
	char input_file_name [MAX_PATH_LENGTH];
	char output_file_name [MAX_PATH_LENGTH];
	int f, i;
	long *total_counts;
	long *counts_buffer;
	long *initial_ranks;
	FILE *inputFP; FILE * outputFP;
	int read;

	long current_rank;
	for (f=0; f< num_files; f++) {
		sprintf (input_file_name, "%s/binary_input_%d", input_directory, f);
		sprintf (output_file_name, "%s/counts_%d", temp_directory, f);

		count_characters_infile (input_file_name, output_file_name);
	}

	//now merge counts - later with map-reduce
	total_counts = (long *) Calloc (MAX_CHAR*sizeof(long));
	counts_buffer = (long *) Calloc (MAX_CHAR*sizeof(long));
	initial_ranks = (long *) Calloc (MAX_CHAR*sizeof(long));
	for (f=0; f< num_files; f++) {
		sprintf (input_file_name, "%s/counts_%d", temp_directory, f);
		OpenBinaryFileRead (&inputFP, input_file_name);
		read = fread (counts_buffer, sizeof(long), MAX_CHAR, inputFP);
		if (read != MAX_CHAR) {
			printf ("Reading error of long counts from file %s\n", input_file_name);
			return FAILURE;
		}
		for (i=0; i< MAX_CHAR; i++) {
			total_counts [i] += counts_buffer[i];
		}
		fclose (inputFP);
	}


	if (DEBUG) {
		for (i=0; i< MAX_CHAR; i++) {
			if (total_counts [i] >0)
				printf ("Count for character '%c' = %ld\n", (char)i,  total_counts [i]);
		}
	}

	//now we need to determine initial ranks for each possible one of 255 characters
	current_rank = 0; //the first rank is 0 - can be several sentinel ranks, but they all will be negative
	for (i=0; i< MAX_CHAR; i++) {
		if (total_counts [i]  == 0)
			initial_ranks [i] = -1; //no need for a rank for this character - it is not present in the input
		else {
			initial_ranks [i] = current_rank;
			current_rank += total_counts [i];
		}
	}

	if (DEBUG) {
		for (i=0; i< MAX_CHAR; i++) {
			if (initial_ranks [i] != -1)
				printf ("Initial rank for character '%c' = %ld\n", (char)i,  initial_ranks [i]);
		}
	}

	sprintf (output_file_name, "%s/initial_ranks", temp_directory);
	OpenBinaryFileWrite (&outputFP, output_file_name);
	Fwrite (initial_ranks, sizeof(long), MAX_CHAR, outputFP);
	fclose (outputFP);

	free (initial_ranks);
	free (total_counts);
	free (counts_buffer);

	return SUCCESS;
}