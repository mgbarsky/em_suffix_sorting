#include "utils.h"

int main (int argc, char ** argv) {
	FILE * inputFP = NULL;
	FILE * outputFP = NULL;
	char *file_name;
	char * output_directory;
	int file_id;
	int line_breaks = 0;
	char *buffer;
	int pos_in_buffer = 0;
	char line [MAX_LINE];
	int line_len;
	char output_file_name [MAX_PATH_LENGTH];
	int i;

	if (argc < 4 ) {
		printf ("To run input_to_binary <text file name> <output directory> <file id> <line_breaks 0/1>\n");
		return 1;
	}

	file_name = argv[1];

	output_directory = argv [2];

	file_id = atoi (argv[3]);
	if (argc > 4) 
		line_breaks = atoi (argv[4]);

	//we collect characters in buffer before flushing it to disk
	buffer = (char *) Calloc (DEFAULT_CHAR_BUFFER_SIZE);
	
	//open text input for reading
	if(!(inputFP= fopen ( file_name , "r" )))	{
		printf("Could not open input text file \"%s\" for reading \n", file_name);
		exit (1);
	}

	sprintf (output_file_name, "%s/binary_input_%d", output_directory, file_id);
	OpenBinaryFileWrite (&outputFP, output_file_name);
	
	while (fgets (line, MAX_LINE-1, inputFP)!=NULL ) {				
		line[strcspn(line, "\r\n")] = '\0';
		line_len = strlen(line);
		for (i=0; i< line_len; i++) {
			buffer[pos_in_buffer++] = line[i];
			if (pos_in_buffer == DEFAULT_CHAR_BUFFER_SIZE) {
				Fwrite (buffer, sizeof(char), pos_in_buffer, outputFP);
				pos_in_buffer = 0;
			}
		}
		if (line_breaks) {
			buffer[pos_in_buffer++] = 0; //sentinel character - less than any letter of the alphabet - we assume that alphabet is positive char
			if (pos_in_buffer == DEFAULT_CHAR_BUFFER_SIZE) {
				Fwrite (buffer, sizeof(char), pos_in_buffer, outputFP);
				pos_in_buffer = 0;
			}
		}
	}
	
	fclose(inputFP);
	if (pos_in_buffer == DEFAULT_CHAR_BUFFER_SIZE) {
		Fwrite (buffer, sizeof(char), pos_in_buffer, outputFP);
		pos_in_buffer = 0;
	}
	//add sentinel - for the end-of-chunk
	buffer[pos_in_buffer++] = 0;

	//add double-sentinel to signify end of file
	if (line_breaks) 
		buffer[pos_in_buffer++] = 0;

	if (pos_in_buffer > 0)
		Fwrite (buffer, sizeof(char), pos_in_buffer, outputFP);

	fclose(outputFP);
	free(buffer);
}