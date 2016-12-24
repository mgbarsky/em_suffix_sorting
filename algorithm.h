#ifndef ALGORITHM_H
#define ALGORITHM_H

int count_characters (char *input_directory, int num_files, char * temp_directory);
int init_ranks (char *input_dir, char * output_dir, int total_files, char *initial_ranks_filename);
int generate_local_runs (char * input_dir, char * temp_dir, int num_files, int h);
int resolve_global_ranks (char *temp_dir );
int update_local_ranks (char * ranks_dir, char * temp_dir);

typedef struct local_triple {
	long currentRank;
	long nextRank;
	int pos;
} LocalRecord;

typedef struct global_triple {
	long currentRank;
	long nextRank;
	long newRank;
} GlobalRecord;

typedef struct run_triple {
	long currentRank;
	long nextRank;
	long count;
} RunRecord;


typedef struct file_buffer_pair {
	int file_id;
	int interval_id;
}RunID;

#endif