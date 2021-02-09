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

}

/*
 * This function will try to restart the database at the provided database location.
 * @param db_loc - the absolute path for the database to restart.
 * @return 0 if the database is restarted successfully, otherwise -1;
 */
int restart_database( char * db_loc ){

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
lookup_table *read_lookup_file(char* file_loc){
	int t_count = 0;
	lookup_table *table = NULL;
	ssize_t read = 0;
	size_t len = 0;
	int table_indx = 0;
	FILE* pFile;

	pFile = fopen(file_loc, "r"); 
	if(pFile == NULL){
    	fprintf(stderr,"ERROR: invalid lookup file %s\n",file_loc);
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
int write_lookup_table(lookup_table* lookup_table, char* file_loc){
	// wrtie the lookup table to a file marker lookup
	FILE* wFile;

	wFile = fopen(file_loc, "w");
	if(wFile == NULL){
		fprintf(stderr, "ERROR: invalid lookup file %s\n", file_loc);
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