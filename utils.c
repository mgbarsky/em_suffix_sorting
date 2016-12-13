#include "utils.h"

void * Calloc (int num_bytes) {
	void * result =  calloc (num_bytes, 1);
	if (result == NULL) {
		printf ("Could not allocate array of size %d bytes\n", num_bytes);
		exit (-1);
	}
	return result;
}

void OpenBinaryFileRead (FILE ** fp, char * file_name) {
	if(!(*fp= fopen ( file_name , "rb" )))	{
		printf("Could not open input binary file \"%s\" for reading \n", file_name);
		exit (1);
	}
}

void OpenBinaryFileWrite (FILE ** fp, char * file_name) {	
	if(!(*fp= fopen ( file_name , "wb" )))	{
		printf("Could not open output binary file \"%s\" for writing \n", file_name);
		exit (1);
	}
}


void OpenBinaryFileAppend (FILE **fp, char * file_name) {	
	if(!(*fp= fopen ( file_name , "ab" )))	{
		printf("Could not open binary file \"%s\" for appending \n", file_name);
		exit (1);
	}
}

void Fwrite (const void *buffer, size_t elem_size, size_t num_elements, FILE *fp ) {
	int written = fwrite (buffer, elem_size, num_elements, fp);
	if (written != num_elements) {
		printf ("Failed to write %ld elements to file: fwrite returned %ld\n", num_elements, written);
		exit (1);
	}
}

