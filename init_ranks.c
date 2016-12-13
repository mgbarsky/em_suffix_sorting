#include "utils.h"
#include "algorithm.h"

void file_to_ranks (char * input_file_name, char * output_file_name, long *ranks_array, int file_id) {
	char *input_buffer;  //for reading original characters from input file
	long *output_buffer; //for writing initial ranks to a new ranks file

	int i, b, d;
	FILE * inputFP=NULL; FILE * outputFP = NULL;
	int read;
	output_buffer = (long *) Calloc (DEFAULT_LONG_BUFFER_SIZE*sizeof(long));
	input_buffer = (char *) Calloc (DEFAULT_CHAR_BUFFER_SIZE*sizeof(char));

	OpenBinaryFileRead (&inputFP, input_file_name);
	OpenBinaryFileWrite (&outputFP, output_file_name);

	b=0; //position in output buffer
	while ((read = fread (input_buffer,sizeof(char),DEFAULT_CHAR_BUFFER_SIZE,inputFP)) > 0){		
		for (i=0; i < read; i++) {
			char current = ABS (input_buffer[i]);
			if (ranks_array [(int)current] == -1) {
				printf ("Unexpected error: no initial rank for character %c\n", current);
				exit (1);
			}

			if (input_buffer[i] == 0 ) //sentinel - resolved
				output_buffer [b++] = - file_id; //negative - resolved
			else
				output_buffer [b++] = ranks_array [(int)current]; //positive - unresolved
			if (b == DEFAULT_LONG_BUFFER_SIZE) {
				if (DEBUG_SMALL) {
					for (d=0; d<b; d++)
						printf("%ld ",output_buffer[d]); 
				}
				Fwrite (output_buffer, sizeof (long), b, outputFP);
			}
		}
	}
	
	if (b > 0) {
		if (DEBUG_SMALL) {
			for (d=0; d<b; d++)
				printf("%ld ",output_buffer[d]); 
		}
		Fwrite (output_buffer, sizeof (long), b, outputFP);
	}
	printf ("\n");

	fclose(inputFP);	
	fclose (outputFP);

	free (output_buffer);
	free (input_buffer);
}


int init_ranks (char *input_dir, char * output_dir, int total_files, char *initial_ranks_filename) {
	char input_file_name [MAX_PATH_LENGTH];
	char output_file_name [MAX_PATH_LENGTH];
	FILE *initFP;
	long *initial_ranks;
	int result, f;

	initial_ranks = (long *) Calloc (MAX_CHAR*sizeof(long));
	OpenBinaryFileRead (&initFP, initial_ranks_filename);
	result = fread (initial_ranks, sizeof(long), MAX_CHAR, initFP);
	if (result != MAX_CHAR) {
		printf ("Failed to read initial ranks from file %s\n", initial_ranks_filename);
		return FAILURE;
	}
	fclose (initFP);

	for (f = 0; f < total_files; f++) {
		sprintf (input_file_name, "%s/binary_input_%d", input_dir, f);
		sprintf(output_file_name, "%s/ranks_%d", output_dir, f);
		file_to_ranks (input_file_name, output_file_name, initial_ranks, f);
	}

	free (initial_ranks);
	return SUCCESS;
}
