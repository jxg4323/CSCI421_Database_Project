/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#include "lookupmanager.h"
#include "storage.h"


		// TODO: CHANGE lookup table layout to [table_id, bin_size, [{pageid: row, col, size}]] --> TEST IT


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
		int row = (table_data->byte_info)[i][1];
		int col = (table_data->byte_info)[i][2];
		int size = (table_data->byte_info)[i][3];
		if( i == (table_data->bin_size-1) ){
			printf("[%d: %d, %d, %d] ", page_id, row, col, size);
		}else{
			printf("[%d: %d, %d, %d], ", page_id, row, col, size);
		}
	}
	printf("}\n");
}

/*
 * Allocate initial memory for lookup table structure.
 * Return 0 with success and -1 with failure.
 */
int initialize_lookup_table(int num_of_tables, lookup_table* table){ 
	table->table_count = num_of_tables;
	table->table_data = (table_pages *)malloc(num_of_tables*(sizeof(table_pages)));
	return 0;
}

void manage_table_array(int count, lookup_table *table, bool increase){
	if(increase){
		table->table_data = (table_pages *)realloc(table->table_data, count*sizeof(table_pages));
	}else{
		table->table_data = (table_pages *)malloc(count*sizeof(table_pages));
	}
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
			fread(&(((table->table_data)[table_indx].byte_info)[i][1]),sizeof(int),1,pFile);  // row_offset
			fread(&(((table->table_data)[table_indx].byte_info)[i][2]),sizeof(int),1,pFile);  // col_offset
			fread(&(((table->table_data)[table_indx].byte_info)[i][3]),sizeof(int),1,pFile);  // record_size
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
		return -1;
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
			fwrite(&(((lookup_table->table_data)[i]).byte_info[j][3]),sizeof(int),1,wFile);
		}
	}
	free( lookup_file );
	fclose(wFile);
	return 0;
}

/*
 * Get the table info structure from the lookup table and add a new memory info
 * section to the bin.
 * Return 0 with success and -1 with failure.
 */
int update_lookup_table(lookup_table* l_table, int table_id, int page_id, int row, int col, int r_size){
	// update lookup table
	int table_info_loc = get_table_info(l_table, table_id);
	(l_table->table_data[table_info_loc]).byte_info = (int **)realloc((l_table->table_data[table_info_loc]).byte_info, ((l_table->table_data[table_info_loc]).bin_size+1)*sizeof(int*));
	int* tmp = calloc(LOOKUP_TUPLE_SIZE, sizeof(int));
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size] = tmp;
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size][0] = page_id;
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size][1] = row;
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size][2] = col;
	((l_table->table_data[table_info_loc]).byte_info)[(l_table->table_data[table_info_loc]).bin_size][3] = r_size;
	(l_table->table_data[table_info_loc]).bin_size++;
	return 0;
}

/*
 * Based on the given table id do a simple linear search for the id
 *
 * Return index location of table information in lookup table or -1 if not found.
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
 * Return address of the table_pages struct for the given table or Null if not found
 */
table_pages *get_table_struct( lookup_table* l_table, int table_id ){
	bool success = false;
	for( int i = 0; i < l_table->table_count; i++ ){
		if( (l_table->table_data)[i].table_id == table_id ){
			success = true;
			return &(l_table->table_data[i]);
		}
	}
	if(!success){
		return NULL;
	}
}

/*
 * Delete table information from l_table.
 * Go through the table list and remove and free information, 
 * return pointer with success and NULL with failure.
 *
 * Reminder: 
 * 		--> The calling function should immediately after delete all records for the given table
 * 		--> remove all table meta data pertaining to it --> should be done by the delete_table function
 */
lookup_table* delete_table_info(lookup_table *l_table, int table_id){
	// Retrieve the table_pages pointer from the lookup_table 
	int t_loc = get_table_info(l_table, table_id);
	int new_size = l_table->table_count - 1;
	lookup_table* temp = (lookup_table *)malloc(sizeof(lookup_table));
	initialize_lookup_table( new_size, temp );
	if( t_loc >= 0 ){
		// add every element in the array except the specified one
		int j = 0;
		for(int i = 0; i < l_table->table_count; i++){
			if( i != t_loc && j < new_size){
				// need to allocate memory for the byte_info of table_data
				init_table_pages(l_table->table_data[i].bin_size, l_table->table_data[i].table_id, &(temp->table_data[j]));
				temp->table_data[j] = l_table->table_data[i];
				// TODO: add loop to set byte_info data
				for(int s = 0; s < l_table->table_data[i].bin_size; s++){
					temp->table_data[j].byte_info[s][0] = l_table->table_data[i].byte_info[s][0];
					temp->table_data[j].byte_info[s][1] = l_table->table_data[i].byte_info[s][1];
					temp->table_data[j].byte_info[s][2] = l_table->table_data[i].byte_info[s][2];
					temp->table_data[j].byte_info[s][3] = l_table->table_data[i].byte_info[s][3];
				}
				j++;
			}
		}
		// TODO: Error here when allocating the given lookup table
	}else{
		return NULL;
	}
	return temp;
}

/*
 * Add new table information with a base standard bin size
 * allocated. 
 * Return 0 with success and -1 for failure.
 */
int add_table_info(lookup_table *l_table, int table_id){
	int end = (l_table->table_count == -1) ? 0 : l_table->table_count;
	int count = (l_table->table_count == -1) ? 1 : l_table->table_count++;
	// if the table already exists in the lookup table return -1
	if( get_table_info( l_table, table_id ) >= 0 ){  
		return -1;
	}
	table_pages *new_table = (table_pages *)malloc(sizeof(table_pages));
	init_table_pages(MIN_BIN_SIZE, table_id, new_table);
	bool size_up = (l_table->table_count == -1) ? false : true; 
	manage_table_array(end+1, l_table, size_up);
	// add new table to the end of the lookup table
	l_table->table_data[end] = *new_table;
	l_table->table_count = count;
	return 0;
}

/*
 * Free the memory associated with the table's page location information.
 * Return 0 for success and -1 if failure.
 */
int clear_table_bin(lookup_table *l_table, int table_id){
	int loc = get_table_info( l_table, table_id );
	if( loc != -1 ){
		free_table_pages( &(l_table->table_data[loc]) );
		l_table->table_data[loc].bin_size = 0;
	}else{
		return -1;
	}
	return 0;
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
}
