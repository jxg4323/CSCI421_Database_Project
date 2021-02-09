/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#include <storagemanager.h>
#include <storage.h>


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
	db_info* config = get_db_config(db_loc);
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
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "w"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: invalid database config file %s\n",db_loc);
    	return -1;
    }
    char * empty_buffer = (char *)malloc(MIN_ALLOC*sizeof(char));
    fwrite(&(page_size), sizeof(int), 1, dbFile);
    fwrite(&(buffer_size), sizeof(int), 1, dbFile);
    fwrite(db_loc, sizeof(char), MIN_ALLOC, dbFile);
    fwrite(empty_buffer, sizeof(char), buffer_size, dbFile);

    free(db_loc);
    free(empty_buffer);
	fclose(dbFile);
	return 0;
}

/*
 * Get database config info and page buffer and place into volatile memory.
 */
db_info *get_db_config( char * db_loc ){
	FILE *dbFile;
	db_config db_info = NULL;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "r"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: invalid database config file %s\n",db_loc);
    	return NULL;
    }
    fread(&(db_info.page_size),sizeof(int),1,dbFile);
    fread(&(db_info.buffer_size),sizeof(int),1,dbFile);
    //allocate memory for buffer and file location
    db_info.db_location = (char *)malloc(MIN_ALLOC*sizeof(char)); // file location can't be more than 50 chars long
    db_info.page_buffer = (char *)malloc((db_info.buffer_size)*sizeof(char)); 
    fread(&(db_info.db_location), sizeof(char), MIN_ALLOC, dbFile);
    fread(&(db_info.page_buffer), sizeof(char), db_info.buffer_size, dbFile);
    fclose(dbFile);
    return db_info;
}

/*
 * Update the buffer in database config.
 * Return 0 with success and -1 for failure.
 */
int update_db_config( char * db_loc, char * buffer ){
	// buffer size should NEVER change
	db_info *config = get_db_config(db_loc);
	config->page_buffer = buffer;

	FILE *dbFile;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "w"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: invalid database config file %s\n",db_loc);
    	return -1;
    }
    fwrite(&(config->page_size), sizeof(int), 1, dbFile);
    fwrite(&(config->buffer_size), sizeof(int), 1, dbFile);
    fwrite(config->db_loc, sizeof(char), MIN_ALLOC, dbFile);
    fwrite(config->page_buffer, sizeof(char), buffer_size, dbFile);

    free(db_loc);
    free(page_buffer);
	fclose(dbFile);
	return 0;
}

void free_config( db_info *config ){
	free(config->db_location);
	free(config->page_buffer);
	free(config);
}

/*
 * Pretty print lookup table
 */
void print_lookup_table(lookup_table *table){
	printf("Table count: %d\n",table->table_count);
	for(int i = 0; i < table->table_count; i++){
		char * t_str = table_bin_string(table->table_data);
		printf("%s\n", t_str);
	}
}

/*
 * String interpretation of the table storeage locations.
 */
char *table_bin_string(table_page_locs *table_data){
	char * string = (char *)malloc(table_data->bin_size*sizeof(char));
	sprintf(string, "Table id: %d, bin size: %d, page info: {",table_data->table_id,table_data->bin_size);
	for(int i = 0; i < table_data->bin_size; i++){
		int page_id = (table_data->byte_info)[i][0];
		int s_byte = (table_data->byte_info)[i][1];
		int e_byte = (table_data->byte_info)[i][2];
		char tmp[44]; // space for 3 ints and 8 chars
		sprintf(tmp, "[%d, %d, %d], ", page_id, s_byte, e_byte);
		strcat(string, tmp);  
	}
	strcat(string, "}");
	return string;
}

/*
 * Allocate initial memory for lookup table structure.
 */
lookup_table *initialize_lookup_table(int num_of_tables){ 
	lookup_table table = NULL;
	table.table_data = (table_page_locs *)malloc(num_of_tables*(sizeof(table_page_locs)));
	return &table;
}

/*
 * Initialize memory for array of structures which contain location
 * of the pages for each individual table as well as the byte offsets
 * on the respective pages.
 */
void init_table_pages(int arr_size, int t_id, table_page_locs * t_data){
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
 */
lookup_table *read_lookup_file(char* db_loc){
	int t_count = 0;
	lookup_table *table = NULL;
	ssize_t read = 0;
	size_t len = 0;
	int table_indx = 0;
	FILE* pFile;

	int file_loc_len = strlen(db_loc) + LOOKUP_FILE_NAME_LEN;
	char * lookup_file = (char *)malloc(file_loc_len*sizeof(char));
	strcat(lookup_file, db_loc);
	strcat(lookup_file, LOOKUP_TABLE_FILE);

	pFile = fopen(lookup_file, "r"); 
	if(pFile == NULL){
    	fprintf(stderr,"ERROR: invalid lookup file %s\n",lookup_file);
    	return false;
    }

    fread(&t_count, sizeof(int),1,pFile) // get table count
    table = initialize_lookup_table(t_count);
	while( 1 ){
		// cast first part of the line to the lookup_info struct to get bin & table info
		int t_id = -1;
		int b_size = -1;
		fread(&t_id,sizeof(int),1,pFile);
		fread(&b_size,sizeof(int),1,pFile);
		// allocate enough memory for the bin 
		init_table_pages(b_size, t_id, (table->table_data)[table_idx])
		// drop lookup_table struct pointer to this memory (cast)
		/// Loop through each individual integer and store them into byte_info
		for( int i = 0; i < b_size; i++ ){
			fread(&(((table->table_data)[table_idx].byte_info)[i][0]),sizeof(int),1,pFile)  // page id
			fread(&(((table->table_data)[table_idx].byte_info)[i][1]),sizeof(int),1,pFile)  // start byte
			fread(&(((table->table_data)[table_idx].byte_info)[i][2]),sizeof(int),1,pFile)  // end byte
		}
		// add this struct pointer to the list of struct pointers (list of table information)
		// next line
		if(feof(pFile)){ //read until end of file character
			break;
		}
		table_indx++;
	}
	fclose( pFile );
	return table;
}

/*
 * Save lookup table information to the given file location.
 */ 
int write_lookup_table(lookup_table* lookup_table, char* db_loc){
	// wrtie the lookup table to a file marker lookup
	FILE* wFile;
	int file_loc_len = strlen(db_loc) + LOOKUP_FILE_NAME_LEN;
	char * lookup_file = (char *)malloc(file_loc_len*sizeof(char));
	strcat(lookup_file, db_loc);
	strcat(lookup_file, LOOKUP_TABLE_FILE);

	wFile = fopen(lookup_file, "w");
	if(wFile == NULL){
		fprintf(stderr, "ERROR: invalid lookup file %s\n", db_loc);
		return false;
	}
	fwrite(&(lookup_table->table_count), sizeof(int), 1, wFile);
	for( int i = 0; i < lookup_table->table_count; i++ ){
		int t_id = ((lookup_table->table_data)[i]).table_id;
		int b_size = ((lookup_table->table_data)[i]).bin_size;
		for( int j = 0; j < b_size; j++ ){
			fwrite(&(((lookup_table->table_data)[i]).byte_info[j][0]),sizeof(int),1,wFile);
			fwrite(&(((lookup_table->table_data)[i]).byte_info[j][1]),sizeof(int),1,wFile);
			fwrite(&(((lookup_table->table_data)[i]).byte_info[j][2]),sizeof(int),1,wFile);
		}
	}
	fclose(wFile);
	// return 0 for success and -1 for failure
	return 0;
}

/*
 * Get the table info structure from the lookup table and add a new memory info
 * section to the bin.
 */
lookup_table *update_lookup_table(lookup_table* l_table, int table_id, int page_id, int s_byte, int e_byte){
	// update lookup table
	table_page_locs *t_info = get_table_info(l_table, table_id);
	t_info->byte_info = (int **)realloc(t_info->byte_info, (t_info->bin_size+1)*sizeof(int*));
	int* tmp = calloc(LOOKUP_TUPLE_SIZE, sizeof(int));
	(t_info->byte_info)[t_info->bin_size] = tmp;
	t_info->bin_size++;
}

/*
 * Based on the given table id do a simple linear search for the id
 *
 * Return info for the table page locations or NULL if not found.
 */
table_page_locs *get_table_info(lookup_table* l_table, int table_id){
	bool success = false;
	for( int i = 0; i < l_table->table_count; i++ ){
		if( (l_table->table_data)[i].table_id == table_id ){
			success = true;
			return &((l_table->table_data)[i]);
		}
	}
	if(!success){
		return NULL;
	}
}

/*
 * Free memory used by lookup table
 */
void free_lookup_table(lookup_table* l_table){
	for( int i = 0; i < l_table->table_count; i++ ){
		int b_size = (l_table->table_data[i]).bin_size;
		for( int j = 0; j < b_size; j++ ){
			free( (l_table->table_data[i])[j] );
		}
	}
	free( l_table->table_data );
}