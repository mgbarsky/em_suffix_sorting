#include "utils.h"
#include "algorithm.h"

char *input_dir = "input";
char *temp_dir = "tmp";
char *output_dir = "output";
int num_input_files = 0;

//2 directories are created and input files are preprocessed into binary input with sentinel
int main (int argc, char ** argv) {
	char file_name [MAX_PATH_LENGTH];
	int h, result;
	int more_runs = 1;
	if (argc < 2 ) {
		printf ("To run ./algorithm <number input files>\n");
		return 1;
	}

	num_input_files = atoi (argv[1]);
	if (DEBUG_SMALL) printf ("sizeof(long) = %ld\n", sizeof(long));
	//step 1 - count characters and compute initial ranks for each character
	if (count_characters (input_dir, num_input_files, temp_dir) != SUCCESS)
		return FAILURE;


	//step 2. Replace input file of characters with an array of initial ranks - this array will be updated in each iteration of an algorithm
	sprintf (file_name, "%s/initial_ranks", temp_dir);
	if (init_ranks (input_dir, output_dir, num_input_files, file_name)!=SUCCESS)
		return FAILURE;
	
	h=0;
	while (more_runs) {
		more_runs = 0;
		//generate sorted runs with counts and local rank pairs grouped by file_id and interval_id 
		result = generate_local_runs (output_dir, temp_dir, num_input_files, h);
		if (result != EMPTY)
			more_runs = 1;
		if (result == FAILURE)
			return FAILURE;

		//merge local ranks into global ranks - from all 

		//update local ranks with resolved global ranks

		h++;
	}
	return SUCCESS;
}