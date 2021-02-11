/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#include "storagemanager.h"
#include "storage.h"


/* 
 * create or restarts an instance of the database at the 
 * provided database location.
 * If restart is true this function will call the restart_database function.
 * If restart is false this function will call the new database function.
 * @param db_loc - the absolute path for the database.
 * @param page_size - the pages size for the database.
 * @param buffer_size - the maximum number of pages the 
 *                      database can hold in its page buffer at one time. 
 * @param restart - if true it will attempt to restart the database at the
 *                  provided database location. 
 * @return the result of either the restart_database or new_database function calls.
 */
int create_database( char * db_loc, int page_size, int buffer_size, bool restart){
	if(restart){
		return restart_database(db_loc);
	}else{
		int com_len = strlen(db_loc) + 7;
		char *com = (char *)malloc(com_len*sizeof(char));
		strcat(com,"rm -r ");
		strcat(com, db_loc);
		strcat(com, "*");
		system(com);
		free(com);
		return new_database(db_loc, page_size, buffer_size);
	}
}

/* 
 * This function will try to restart the database at the provided database location.
 * @param db_loc - the absolute path for the database to restart.
 * @return 0 if the database is restarted successfully, otherwise -1;
 */
int restart_database( char * db_loc ){
	int result;
	db_config* config = (db_config *)malloc(sizeof(db_config));
	result = get_db_config(db_loc, config);  
	if(config == NULL){
		return -1;
	}else{
		free_config( config );
		return 0;
	}
}

/* 
 * create an instance of the database at the provided database location.
 * @param db_loc - the absolute path for the database.
 * @param page_size - the pages size for the database.
 * @param buffer_size - the maximum number of pages the 
 *                      database can hold in its page buffer at one time.
 * @return 0 if the database is started successfully, otherwise -1;
 */
int new_database( char * db_loc, int page_size, int buffer_size ){
	// need to store database info into file
	FILE *dbFile;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	memset(config_file, 0, db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "wb"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: invalid database config file location %s\n",config_file);
    	return -1;
    }
    char * empty_buffer = (char *)malloc(buffer_size*sizeof(char));
    fwrite(&(page_size), sizeof(int), 1, dbFile);
    fwrite(&(buffer_size), sizeof(int), 1, dbFile);
    fwrite(db_loc, sizeof(char), db_loc_len, dbFile);
    fwrite(empty_buffer, sizeof(char), buffer_size, dbFile);

    free(empty_buffer);
	fclose(dbFile);
	return 0;
}

/* 
 * Allocate memory for the config structure before calling.
 * Get database config info and page buffer and place into volatile memory.
 */
int get_db_config( char * db_loc, db_config* config ){
	FILE *dbFile;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	config->page_buffer = (char *)malloc(config->buffer_size*sizeof(char));

	memset(config_file, 0, db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "rb"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: invalid database config file %s\n",db_loc);
    	return -1;
    }
    fread(&(config->page_size),sizeof(int),1,dbFile);
    fread(&(config->buffer_size),sizeof(int),1,dbFile);
    //allocate memory for buffer and file location
    config->db_location = (char *)malloc(db_loc_len*sizeof(char));
    config->page_buffer = (char *)malloc((config->buffer_size)*sizeof(char)); 
    fread(config->db_location, sizeof(char), db_loc_len, dbFile);
    fread(config->page_buffer, sizeof(char), config->buffer_size, dbFile);

    free( config_file );
    fclose(dbFile);
    return 0;
}

/*
 * Update the buffer in database config.
 * Return 0 with success and -1 for failure.
 */
int update_db_config( char * db_loc, char * buffer ){
	// buffer size should NEVER change
	FILE *dbFile;
	int result;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	int buf_size = strlen(buffer);
	db_config* config = (db_config *)malloc(sizeof(db_config));
	
	result = get_db_config(db_loc, config); 
	strcpy(config->page_buffer, buffer);


	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	memset(config_file, 0, db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "wb"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: invalid database config file %s\n",db_loc);
    	return -1;
    }
    fwrite(&(config->page_size), sizeof(int), 1, dbFile);
    fwrite(&(config->buffer_size), sizeof(int), 1, dbFile);
    fwrite(config->db_location, sizeof(char), db_loc_len, dbFile);
    fwrite(config->page_buffer, sizeof(char), config->buffer_size, dbFile);

    free_config( config );
    free( config_file );
	fclose(dbFile);
	return 0;
}

/*
 * Free database config volatile memory.
 */
void free_config( db_config *config ){
	free(config->db_location);
	free(config->page_buffer);  // ERROR: invalid pointer
	free(config);
}

/*
 * Pretty print the database config file into text. 
 */
void pretty_print_db_config( db_config *config){
	printf("Page Size: %d, Buffer Size: %d, Database Path: %s\n Buffer: %s\n", config->page_size, config->buffer_size, config->db_location, config->page_buffer);
}

/*
 * Pretty print lookup table
 */
void print_lookup_table(lookup_table *table){
	printf("Table count: %d\n",table->table_count);
	for(int i = 0; i < table->table_count; i++){
		print_table_bin( &(table->table_data[i]) );
	}
}

/*
 * String interpretation of the table storeage locations.
 */
void print_table_bin(table_pages *table_data){
	printf("Table id: %d, bin size: %d, page info: {",table_data->table_id,table_data->bin_size);
	for(int i = 0; i < table_data->bin_size; i++){
		int page_id = (table_data->byte_info)[i][0];
		int s_byte = (table_data->byte_info)[i][1];
		int e_byte = (table_data->byte_info)[i][2];
		if( i == (table_data->bin_size-1) ){
			printf("[%d: %d, %d] ", page_id, s_byte, e_byte);
		}else{
			printf("[%d: %d, %d], ", page_id, s_byte, e_byte);
		}
	}
	printf("}\n");
}

/*
 * Allocate initial memory for lookup table structure.
 * Return 0 with success and -1 with failure.
 * TODO: rework not to generate the structure pointer locally but have it passed
 */
int initialize_lookup_table(int num_of_tables, lookup_table* table){ 
	table->table_count = num_of_tables;
	table->table_data = (table_pages *)malloc(num_of_tables*(sizeof(table_pages)));
	return 0;
}

/*
 * Initialize memory for array of structures which contain location
 * of the pages for each individual table as well as the byte offsets
 * on the respective pages.
 */
void init_table_pages(int arr_size, int t_id, table_pages * t_data){
	t_data->table_id = t_id;
	t_data->bin_size = arr_size;
	t_data->byte_info = (int **)malloc(arr_size*(sizeof(int*)));
	for(int i = 0; i<arr_size; i++){
		(t_data->byte_info)[i] = calloc(LOOKUP_TUPLE_SIZE, sizeof(int));
	}
}
/*
 * If the lookup table already exists then read the current information from
 * the lookup table into volatile memory for manipulation.
 * Return 0 with success and -1 with failure.
 */
int read_lookup_file(char* db_loc, lookup_table* table){
	int t_count = 0;
	ssize_t read = 0;
	size_t len = 0;
	int table_indx = 0;
	FILE* pFile;

	int file_loc_len = strlen(db_loc) + LOOKUP_FILE_NAME_LEN;
	char * lookup_file = (char *)malloc(file_loc_len*sizeof(char));
	memset(lookup_file, 0, file_loc_len*sizeof(char));
	strcat(lookup_file, db_loc);
	strcat(lookup_file, LOOKUP_TABLE_FILE);

	pFile = fopen(lookup_file, "rb"); 
	if(pFile == NULL){
    	fprintf(stderr,"ERROR: read_lookup_file, invalid lookup file %s\n",lookup_file);
    	return -1;
    }

    fread(&(t_count), sizeof(int), 1, pFile); // get table count
    initialize_lookup_table( t_count, table );
	while( 1 ){
		// cast first part of the line to the lookup_info struct to get bin & table info
		int t_id = -1;
		int b_size = -1;
		fread(&t_id,sizeof(int),1,pFile);
		fread(&b_size,sizeof(int),1,pFile);
		// allocate enough memory for the bin 
		init_table_pages(b_size, t_id, &((table->table_data)[table_indx]));
		// drop lookup_table struct pointer to this memory (cast)
		/// Loop through each individual integer and store them into byte_info
		for( int i = 0; i < b_size; i++ ){
			fread(&(((table->table_data)[table_indx].byte_info)[i][0]),sizeof(int),1,pFile);  // page id
			fread(&(((table->table_data)[table_indx].byte_info)[i][1]),sizeof(int),1,pFile);  // start byte
			fread(&(((table->table_data)[table_indx].byte_info)[i][2]),sizeof(int),1,pFile);  // end byte
		}
		// add this struct pointer to the list of struct pointers (list of table information)
		// next line
		if(feof(pFile) || table_indx == (t_count-1)){ //read until end of file character or there aren't any more tables
			break;
		}
		table_indx++;
	}
	fclose( pFile );
	return 0;
}

/*
 * Save lookup table information to the given file location.
 * Return 0 for success and -1 for failure
 */ 
int write_lookup_table(lookup_table* lookup_table, char* db_loc){
	// wrtie the lookup table to a file marker lookup
	FILE* wFile;
	int file_loc_len = strlen(db_loc) + LOOKUP_FILE_NAME_LEN;
	char * lookup_file = (char *)malloc(file_loc_len*sizeof(char));
	memset(lookup_file, 0, file_loc_len*sizeof(char));
	strcat(lookup_file, db_loc);
	strcat(lookup_file, LOOKUP_TABLE_FILE);

	wFile = fopen(lookup_file, "wb");
	if(wFile == NULL){
		fprintf(stderr, "ERROR: write_lookup_table, invalid lookup file %s\n", db_loc);
		return false;
	}
	fwrite(&(lookup_table->table_count), sizeof(int), 1, wFile);
	for( int i = 0; i < lookup_table->table_count; i++ ){
		int t_id = ((lookup_table->table_data)[i]).table_id;
		int b_size = ((lookup_table->table_data)[i]).bin_size;
		fwrite(&t_id, sizeof(int), 1, wFile);
		fwrite(&b_size, sizeof(int), 1, wFile);
		for( int j = 0; j < b_size; j++ ){
			fwrite(&(((lookup_table->table_data)[i]).byte_info[j][0]),sizeof(int),1,wFile);
			fwrite(&(((lookup_table->table_data)[i]).byte_info[j][1]),sizeof(int),1,wFile);
			fwrite(&(((lookup_table->table_data)[i]).byte_info[j][2]),sizeof(int),1,wFile);
		}
	}
	fclose(wFile);
	return 0;
}

/*
 * Get the table info structure from the lookup table and add a new memory info
 * section to the bin.
 * Return 0 with success and -1 with failure.
 */
int update_lookup_table(lookup_table* l_table, int table_id, int page_id, int s_byte, int e_byte){
	// update lookup table
	int table_info_loc = get_table_info(l_table, table_id);
	(l_table->table_data[table_info_loc]).byte_info = (int **)realloc((l_table->table_data[table_info_loc]).byte_info, ((l_table->table_data[table_info_loc]).bin_size+1)*sizeof(int*));
	int* tmp = calloc(LOOKUP_TUPLE_SIZE, sizeof(int));
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size] = tmp;
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size][0] = page_id;
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size][1] = s_byte;
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size][2] = e_byte;
	(l_table->table_data[table_info_loc]).bin_size++;
	return 0;
}

/*
 * Based on the given table id do a simple linear search for the id
 *
 * Return location of table information in lookup table or -1 if not found.
 */
int get_table_info(lookup_table* l_table, int table_id){
	bool success = false;
	for( int i = 0; i < l_table->table_count; i++ ){
		if( (l_table->table_data)[i].table_id == table_id ){
			success = true;
			return i;
		}
	}
	if(!success){
		return -1;
	}
}

/*
 * Free memory used by lookup table
 */
void free_lookup_table(lookup_table* l_table){
	for( int i = 0; i < l_table->table_count; i++ ){
		int b_size = (l_table->table_data[i]).bin_size;
		free_table_pages( &(l_table->table_data[i]) );
	}
	free( l_table->table_data );
}

/*
 * Free table_page byte_info
 */
void free_table_pages(table_pages* t_data){
	for( int i = 0; i < t_data->bin_size; i++ ){
		free( t_data->byte_info[i] );
	}
	free( t_data->byte_info );
}


/*
 * Just for testing. 
 * TODO: REMOVE!!
 */
int main(int argc, char const *argv[])
{
	// needed to start
	char db_path[] = "/home/stu2/s17/jxg4323/Courses/CSCI421/Project/TestDb/";
	int page_size = 25;
	int buffer_size = 50;
	bool restart_flag = false;

	printf("Database Path %s\n", db_path);
	printf("%s\n", DATABASE_CONFIG_FILE);

	// Database config testing
	int result;
	result = create_database(db_path, page_size, buffer_size, restart_flag);

	char * new_buf = (char *)malloc(buffer_size*sizeof(char));
	new_buf = "bleh blah boop beep";
	update_db_config( db_path, new_buf ); // Good

	db_config *test_get = (db_config *)malloc(sizeof(db_config));
	get_db_config( db_path, test_get );  // Good
	pretty_print_db_config( test_get );
	
	// Lookup Table Testing
	int table_count = 2;
	int arr_size = 5;
	// Create some test data
	if(restart_flag){
		lookup_table * l_table = (lookup_table *)malloc(sizeof(l_table));
		result = initialize_lookup_table( table_count, l_table );
		for( int i = 0; i < table_count; i++ ){
			init_table_pages(arr_size, i, &((l_table->table_data[i])));
			for(int j = 0; j < arr_size; j++ ){
				((l_table->table_data)[i]).byte_info[j][0] = j+1; 
				((l_table->table_data)[i]).byte_info[j][1] = j+2; 
				((l_table->table_data)[i]).byte_info[j][2] = j+3; 
			}
		}
		print_lookup_table( l_table );
		printf("---------------\n");
		update_lookup_table(l_table, 1, 3, 13, 56);
		print_lookup_table( l_table );
		printf("---------------\n");
		
		write_lookup_table( l_table, db_path );
	}else{
		// Read in lookup File
		lookup_table * l_table = (lookup_table *)malloc(sizeof(l_table));
		read_lookup_file( db_path, l_table );

		print_lookup_table( l_table );

		free_lookup_table( l_table );
	}


	return 0;
}