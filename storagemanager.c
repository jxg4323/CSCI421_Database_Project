/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#include "storagemanager.h"
#include "lookupmanager.h"
#include "pagedescriptor.h"
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
	allocate_db_data(page_size, buffer_size, db_loc);
	table_l = (lookup_table *)malloc(sizeof(lookup_table));
	init_page_buffer();
	allocate_all_schemas();
	allocate_page_manager(0, 0, false);
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
	int db_result = 0;
	int lookup_result = 0;
	int schema_result = 0;
	int pmanage_result = 0;
	db_result = get_db_config(db_loc, db_data);  
	lookup_result = read_lookup_file(db_loc, table_l);
	schema_result = get_all_schemas(db_loc);
	pmanage_result = read_page_manager(db_loc, page_descs);
	if(db_result == -1 || lookup_result == -1 || schema_result == -1 || pmanage_result == -1){
		return -1;
	}else{
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
	// need to store database info into file --> the allocate sets new database
	FILE *dbFile;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	memset(config_file, 0, db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "wb"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: new_database invalid database config file location %s\n",config_file);
    	free( db_data );
    	return -1;
    }
    fwrite(&(page_size), sizeof(int), 1, dbFile);
    fwrite(&(buffer_size), sizeof(int), 1, dbFile);
    fwrite(db_loc, sizeof(char), db_loc_len, dbFile);
    initialize_lookup_table( -1, table_l );
    init_page_manager( 0, 0, page_desc );
    db_data->page_size = page_size;
    db_data->buffer_size = buffer_size;
    strcpy(db_data->db_location, db_loc);

    free(config_file);
	fclose(dbFile);
	return 0;
}

/*
 * Safely shutdown storage manager. 
 */
int terminate_database(){
	// confirm database config file has been writen
	// write buffer to file
	write_db_config( db_data->db_location );
	// get lookup table & confirm written
	write_lookup_table( table_l, db_data->db_location );
	// Write tablemetadata file
	write_all_schemas( db_data->db_location );
	// Write page manager file
	write_page_manager( db_data->db_location, page_descs );
	// free used memory and return
	free_lookup_table( table_l );
	free_config( db_data );
	free_table_schemas( all_table_schemas );
}

/*
 * Allocate local db_config data for use for storagemanager.c
 */
int allocate_db_data(int page_size, int buf_size, char *db_loc){
	db_data = (db_config *)malloc(sizeof(db_config));
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	db_data->page_size = page_size;
    db_data->buffer_size = buf_size;
    db_data->db_location = (char *)malloc(db_loc_len*sizeof(char));
	memset(db_data->db_location, 0, db_loc_len*sizeof(char));
}

/*
 * Pretty print the database config file into text.
 */
void pretty_print_db_config( db_config *config ){
    printf("Page Size: %d, Buffer Size: %d, Database Path: %s\n", config->page_size, config->buffer_size, config->db_location);
}

/* 
 * Allocate memory for the config structure before calling.
 * Get database config info and page buffer and place into volatile memory.
 */
int get_db_config( char * db_loc, db_config* config ){
	FILE *dbFile;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;
	char * config_file = (char *)malloc(db_loc_len*sizeof(char));

	memset(config_file, 0, db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "rb"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: get_db_config invalid database config file %s\n",db_loc);
    	return -1;
    }
    fread(&(config->page_size),sizeof(int),1,dbFile);
    fread(&(config->buffer_size),sizeof(int),1,dbFile);
    //allocate memory for buffer and file location
    config->db_location = (char *)malloc(db_loc_len*sizeof(char));
    fread(config->db_location, sizeof(char), db_loc_len, dbFile);

    free( config_file );
    fclose(dbFile);
    return 0;
}

/*
 * Update the buffer in database config.
 * Return 0 with success and -1 for failure.
 */
int write_db_config( char * db_loc ){
	// buffer size should NEVER change
	FILE *dbFile;
	int result;
	int db_loc_len = strlen(db_loc) + DATABASE_FILE_NAME_LEN;

	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	memset(config_file, 0, db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "wb"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: write_db_config invalid database config file %s\n",db_loc);
    	return -1;
    }
    fwrite(&(db_data->page_size), sizeof(int), 1, dbFile);
    fwrite(&(db_data->buffer_size), sizeof(int), 1, dbFile);
    fwrite(db_data->db_location, sizeof(char), db_loc_len, dbFile);

    free( config_file );
	fclose(dbFile);
	return 0;
}

/*
 * Free database config volatile memory.
 */
void free_config( db_config *config ){
	free(config->db_location);
	free(config);
}


/*
 * Allocate memory for local table_schema_array WITHOUT allocating
 * memory for the array of structures.
 */
void allocate_all_schemas(){
	all_table_schemas = (table_schema_array *)malloc(sizeof(table_schema_array));
	all_table_schemas->last_made_id = -1;
	all_table_schemas->table_count = -1;
}

/*
 * Allocate structure array for the table schemas
 */
void manage_all_schema_array(int count, bool increase_size){
	if( increase_size ){ // if True increase the size of the table_schemas
		all_table_schemas->tables = realloc(all_table_schemas->tables, count*sizeof(table_data *));
	}else{
		all_table_schemas->tables = malloc(count*sizeof(table_data *));
	}
}

/*
 * Initialize the given structure with the correct id, type array length, and
 * key indices array length. Also, allocate the memory required for the arrays.
 */
void init_table_schema(int t_id, int types_len, int key_len, struct table_data **t_schema, int table_index){
    t_schema[table_index] = malloc(sizeof(table_data));
	t_schema[table_index]->id = t_id;
	t_schema[table_index]->data_types_size = types_len;
	t_schema[table_index]->key_indices_size = key_len;
	t_schema[table_index]->data_types = malloc(types_len*sizeof(int));
	t_schema[table_index]->key_indices = malloc(key_len*sizeof(int));
}

/*
 * Free the table schema array volatile memory
 */
void free_table_schemas( table_schema_array* schemas ){
	for( int i = 0; i < schemas->table_count; i++ ){
		free( schemas->tables[i]->data_types );
		free( schemas->tables[i]->key_indices );
		free( schemas->tables[i] );
	}
	free( schemas->tables );
}

/*
 * Pretty print the table schemas
 */
void pretty_print_table_schemas( table_schema_array *schemas ){
	printf(" Table Schemas \n");
	for(int i = 0; i < schemas->table_count; i++){
		printf("Table ID: %d Data types: [ ", schemas->tables[i]->id);
		for( int j = 0; j < schemas->tables[i]->data_types_size; j++){
			if( j == schemas->tables[i]->data_types_size-1 ){
				printf("%d ", schemas->tables[i]->data_types[j]);
			}else{
				printf("%d, ", schemas->tables[i]->data_types[j]);
			}
		}
		printf(" ], Key Indices: [ ");
		for( int j = 0; j < schemas->tables[i]->key_indices_size; j++){
			if( j == schemas->tables[i]->key_indices_size-1 ){
				printf("%d ", schemas->tables[i]->key_indices[j]);
			}else{
				printf("%d, ", schemas->tables[i]->key_indices[j]);
			}
		}
		printf(" ]\n");
	}
}

/*
 * Loop through the file allocating the volatile memory for the table schemas.
 * Return 0 with success and -1 for failure.
 */
int get_all_schemas(char * db_loc){
	int table_indx = 0;
	// Read the metadata file and store in volatile memory
	FILE *schema_fp;
	int file_len = strlen(db_loc) + TABLE_METADATA_FILE_LEN;
	char *schema_file = (char *)malloc(file_len*sizeof(char));

	memset(schema_file, 0, file_len*sizeof(char));
	strcat(schema_file, db_loc);
	strcat(schema_file, TABLE_METADATA_FILE);

	schema_fp = fopen(schema_file, "rb");
	if( schema_fp == NULL){
		fprintf(stderr, "ERROR: get_all_schemas invalid table metadata file %s\n", schema_file);
		free(schema_file);
		return -1;
	}
	fread(&(all_table_schemas->last_made_id),sizeof(int),1,schema_fp);
	fread(&(all_table_schemas->table_count), sizeof(int),1,schema_fp);
	manage_all_schema_array(all_table_schemas->table_count, false);
	while( 1 ){
		// cast first part of the line to the lookup_info struct to get bin & table info
		int t_id = -1;
		int types_len = -1;
		int key_len = -1;
		fread(&t_id,sizeof(int),1,schema_fp);
		fread(&types_len,sizeof(int),1,schema_fp);
		fread(&key_len,sizeof(int),1,schema_fp);
		// init structure for table schema
		init_table_schema(t_id, types_len, key_len, all_table_schemas->tables, table_indx);
		fread((all_table_schemas->tables[table_indx])->data_types,sizeof(int),types_len,schema_fp);
		fread((all_table_schemas->tables[table_indx])->key_indices,sizeof(int),key_len,schema_fp);
		// next line
		if(feof(schema_fp) || table_indx == (all_table_schemas->table_count-1)){ //read until end of file character or there aren't any more tables
			break;
		}
		table_indx++;
	}
	free( schema_file );
	fclose( schema_fp );
	return 0;
}

/*
 * Loop through the table schemas writing them to the disk.
 * Return 0 with success and -1 for failure.
 */
int write_all_schemas(char * db_loc){
	// Loop through all the table metadata structures and write the contents to a given file
	FILE *wFile;
	int file_len = strlen(db_loc) + TABLE_METADATA_FILE_LEN;
	char *schema_file = (char *)malloc(file_len*sizeof(char));

	memset(schema_file, 0, file_len*sizeof(char));
	strcat(schema_file, db_loc);
	strcat(schema_file, TABLE_METADATA_FILE);

	wFile = fopen(schema_file, "wb");
	if( wFile == NULL){
		fprintf(stderr, "ERROR: write_all_schemas invalid table metadata file %s\n", schema_file);
		free(schema_file);
		return -1;
	}
	fwrite(&(all_table_schemas->last_made_id),sizeof(int),1,wFile);
	fwrite(&(all_table_schemas->table_count),sizeof(int),1,wFile);
	for( int i = 0; i < all_table_schemas->table_count; i++){
		fwrite(&(all_table_schemas->tables[i]->id),sizeof(int),1,wFile);
		fwrite(&(all_table_schemas->tables[i]->data_types_size),sizeof(int),1,wFile);
		fwrite(&(all_table_schemas->tables[i]->key_indices_size),sizeof(int),1,wFile);
		fwrite(all_table_schemas->tables[i]->data_types,sizeof(int),all_table_schemas->tables[i]->data_types_size*sizeof(int),wFile);
		fwrite(all_table_schemas->tables[i]->key_indices,sizeof(int),all_table_schemas->tables[i]->key_indices_size*sizeof(int),wFile);
	}
	free( schema_file );
	fclose( wFile );
	return 0;
}

/*
 * Look for table schema information based on given table.
 * Return table_data pointer for schema data if the table 
 * exists o.w. return NULL.
 */
table_data* get_table_schema( int table_id ){
	for(int i = 0; i < all_table_schemas->table_count; i++){
		if ( all_table_schemas->tables[i]->id == table_id ){
			return all_table_schemas->tables[i];
		}
	}
	return NULL;
}

/*
 * This function will drop the table with the provided id
 * from the database. This will remove all data as well as information
 * about the table.
 * @param table_id - the id of the table to drop
 * @return 0 if table succesfully dropped, -1 otherwise.
 */
int drop_table( int table_id ){
	// delete all records for the table
    int cleared = 0 ;
    if(cleared == -1){ return -1; }
	// delete the table struct from the metadata array
    int table_count = all_table_schemas->table_count;
    int offset = 0;
    table_data **table_array = malloc(sizeof(table_data *) * (table_count-1));
    for(int i = 0; i < table_count; i++){
        if(table_id != all_table_schemas->tables[i]->id){
            table_array[i-offset] = malloc(sizeof(table_data));
            table_array[i-offset]->id = all_table_schemas->tables[i]->id;
            table_array[i-offset]->data_types_size = all_table_schemas->tables[i]->data_types_size;
            table_array[i-offset]->key_indices_size = all_table_schemas->tables[i]->key_indices_size;
            table_array[i-offset]->data_types = malloc(sizeof(int) * table_array[i-offset]->data_types_size);
            for(int j = 0; j < table_array[i-offset]->data_types_size; j++){
                table_array[i-offset]->data_types[j] = all_table_schemas->tables[i]->data_types[j];
            }
            table_array[i-offset]->key_indices = malloc(sizeof(int) * table_array[i-offset]->key_indices_size);
            for(int j = 0; j < table_array[i-offset]->key_indices_size; j++){
                table_array[i-offset]->key_indices[j] = all_table_schemas->tables[i]->key_indices[j];
            }
        } else {
            offset = 1;
        }
        free(all_table_schemas->tables[i]->key_indices);
        free(all_table_schemas->tables[i]->data_types);
        free(all_table_schemas->tables[i]);
    }
    all_table_schemas->table_count = table_count - 1;
    free(all_table_schemas->tables);
    all_table_schemas->tables = table_array;
    // delete the table_info from the lookup table
    table_l = delete_table_info(table_l, table_id);
    return 0;
}

/*
 * This function will clear the table with the provided id
 * from the database. This will remove all data but not the table.
 * @param table_id - the id of the table to clear
 * @return 0 if table succesfully cleared, -1 otherwise.
 */
int clear_table( int table_id ){
    // In table metadata structure array remove the data types and key indices for given id
    // delete all of the records for the table
    if(get_table_info(table_l, table_id) == -1){ return -1; }

/*    int key_indices_size = 0;
    for(int i = 0; i < all_table_schemas->table_count; i++){
        if(table_id = all_table_schemas->tables[i].id){
            key_indices_size = all_table_schemas->tables[i].key_indices_size;
        }
    }


    union record_item *key_values = malloc(sizeof(union record_item) * key_indices_size);
    union record_item **records;
    int num_records = get_records(table_id, &records);
    if (num_records > -1) {
        for (int i = 0; i < num_records; i++) {
            for(int j = 0; j < key_indices_size; j++){
                key_values[j] = records[i][j];
            }
            remove_record(table_id, key_values);
            free(&(records[i]));
        }
    } else {
        free(records);
        free(key_values);
        return -1;
    }
    free(records);
    free(key_values);*/
    // clear the table bin information from lookup table
    clear_table_bin( table_l, table_id );
    return 0;
}

/*
 * This will add a table to the database with the provided data types and
 * primary key.
 * @param data_types - an integer array representing the data types stored
                       in a tupler in the table.
 * @param key_indices - an interger array representing the indicies that
                        make up the parimary key. The order of the indicies
						in this array deternmine the ordering of the attributes
						in the primary key.
 * @param data_types_size - the size of the data types array
 * @param key_indices_size - the size of the key indicies array.
 * @return the id of the table created, -1 upon error.
 */
int add_table( int * data_types, int * key_indices, int data_types_size, int key_indices_size ){
	//TODO: reacllocate the table_schema_array->tables array to accomodate the new table
	int end_indx = (all_table_schemas->last_made_id == -1) ? 0 : all_table_schemas->table_count;
   	int new_id = (all_table_schemas->last_made_id == -1) ? 0 : all_table_schemas->last_made_id+1;
    table_data *table = get_table_schema(new_id);
    if(table != NULL ){ return -1; }
   	// reallocate memory for the new meta infomation struct for the table and append it to the metadata file
   	manage_all_schema_array( (all_table_schemas->table_count+1),true );
   	init_table_schema( new_id, data_types_size, key_indices_size, all_table_schemas->tables, end_indx );
   	memcpy( all_table_schemas->tables[end_indx]->data_types, data_types, data_types_size*sizeof(int) );
   	memcpy( all_table_schemas->tables[end_indx]->key_indices, key_indices, key_indices_size*sizeof(int) );
   	all_table_schemas->last_made_id = new_id;
   	all_table_schemas->table_count = end_indx + 1;
   	add_table_info(table_l, new_id);
    return new_id;
}

void allocate_page_manager(int l_id, int page_count, bool reset){
	if(reset){
		page_descs = (page_manager *)malloc(sizeof(page_manager));
		init_page_manager(page_count, l_id, page_descs);
	}else{
		page_descs = (page_manager *)malloc(sizeof(page_manager));
	}
}

/*
 * record item = pointer to the 2d array of records.
 */
int get_records( int table_id, union record_item *** table ){
	table_pages * t_page_info = get_table_struct( table_l, table_id );
	table_data *table_schema = get_table_schema( table_id );
	int c = 0;
	// loop through table record locations and compare keys
	for( int i = 0; i < table_info->bin_size; i++ ){
		int p_id = table_info->byte_info[i][0];
		int row = table_info->byte_info[i][1];
		int col = table_info->byte_info[i][2]; // start offset
		int r_size = table_info->byte_info[i][3]; // should = data_types_size
		int p_loc = request_page_in_buffer( p_id );
		int total = 0;
		memcpy( &(table[c]), &(page_buffer->pages[p_loc].page_records[row][col]), r_size*sizeof(r_item));
		c++;
	}
	return table_info->bin_size;
}

int get_page( int page_id, union record_item *** page ){
	int buf_loc = request_page_in_buffer( page_id );
	// NOT sure how to return number of records in page without knowing size of record arrays???
		// DO I care???
	page = &(page_buffer->pages[buf_loc].page_records);


	return page_buffer->pages[buf_loc].num_of_records;
}

/*
 * Loop through records in pages that correspond to the given table
 * and when the record in the page matches all the key values 
 * copy the values of the record into the data parameter and return 0.
 */
int get_record( int table_id, union record_item * key_values, union record_item ** data ){
	table_pages *table_info = get_table_struct( table_l, table_id );
	table_data *table_schema = get_table_schema( table_id );
	// loop through table record locations and compare keys
	for( int i = 0; i < table_info->bin_size; i++ ){
		int p_id = table_info->byte_info[i][0];
		int row = table_info->byte_info[i][1];
		int col = table_info->byte_info[i][2]; // start offset
		int r_size = table_info->byte_info[i][3];
		int p_loc = request_page_in_buffer( p_id );
		int total = 0;
		for( int j = 0; j < table_schema->key_indices_size; j++){
			int key_loc = table_schema->key_indices[j];
			if( key_values[j] == page_buffer->pages[p_loc].page_records[row][key_loc+col]){
				total++;
			}
		}
		if( total == table_schema->key_indices_size-1 ){ // all keys match
			memcpy(data, page_buffer->pages[p_loc].page_records[row], r_size*sizeof(r_item));
			return 0;
		}
	}
	return -1;
}

/*
 * 
 */
int insert_record( int table_id, union record_item * record ){

}

int update_record( int table_id, union record_item * record ){

}

int remove_record( int table_id, union record_item * key_values ){
	
}

/*
 * Initialize array of records for the page, due to the fact theat
 * the page isn't aware of the table structure. The page doesn't 
 * know how many record_items are in a record. The buffer only cares 
 * about what records are there.
 */
void init_page_layout( int pid, int record_num, page_info* page ){
	page->page_id = pid;
	page->num_of_records = record_num;
	page->page_records = (r_item **)malloc(db_data->page_size*sizeof(r_item));
}
/*
 * Initialize global page buffer pointer and allocate the number
 * of pages in memory and initialize the pages_id to -1 and record 
 * count to floor( page_size / sizeof(record_item) ).
 */
void init_page_buffer(){
	page_buffer = (buffer_manager *)malloc(sizeof(buffer_manager));
	page_buffer->num_of_pages = db_data->buffer_size;
	page_buffer->last_id = -1;
	page_buffer->pages = (page_info *)malloc(page_buffer->num_of_pages*sizeof(page_info));
	for( int i = 0; i < page_buffer->num_of_pages; i++){
		init_page_layout( -1, (int)floor(db_data->page_size / sizeof(r_item)), &(page_buffer->pages[i]) );
	}
}

/*
 * Read data in a page to the buffer. If there isn't enough 
 * room in the buffer remove the least requested page from
 * the buffer. Then add the page to the buffer, increasing
 * its req count in the process. If the page is already in
 * the buffer then return the location of it in the array
 * and increment its req_count.
 * Return location of page in the buffer o.w. -1
 */
int request_page_in_buffer( int page_id ){
	int loc = -1;
	// check if page is already in buffer
	for( int i = 0; i < page_buffer->num_of_pages; i++){
		if ( page_buffer->pages[i].page_id == page_id ){ 
			loc = i;
		}
	}
	if( loc >= 0 ){ // if page is in buffer increase req count 
		page_buffer->pages[loc].req_count++;
		return loc;
	}else{ // if page isn't in buffer
		int free_spot = -1;
		if( check_buffer_status() ){ 
			free_spot = remove_least_used_page();
			add_page_to_buffer( page_id, free_spot );
		}else{
			free_spot = get_open_spot();
			add_page_to_buffer( page_id, free_spot );
		}
		page_buffer->pages[free_spot].req_count++;
		return free_spot;
	}
}

/*
 * Add the given page to the buffer in the location specified
 * to the function.
 * Return 0 with success and -1 for failure.
 */
int add_page_to_buffer( int page_id, int loc ){
	int result = 0;
	// read in the records to array
	result = read_page( page_id, &(page_buffer->pages[loc]) );
	return result;
}

/*
 * Assumes all page id's are positive.
 * Check if the buffer is full, if not return false and true o.w.
 */
bool check_buffer_status( ){
	bool full = true; // assume buffer is full
	for( int i = 0; i < page_buffer->num_of_pages; i++){
		if( page_buffer->pages[i].page_id == -1 ){
			full = false; // page struct isn't in use
		}
	}
	return full;
}

/*
 * Remove the least used page from the buffer. Find the least used page
 * and remove it. Set its page_id, record_count, req_count to -1 and
 * memset its record array to null.
 * Return location of removed page, showing its available and -1 o.w.
 */
int remove_least_used_page( ){
	int least_loc = -1;
	int smallest = INT_MAX;
	for( int i = 0; i < page_buffer->num_of_pages; i++){
		if ( smallest > page_buffer->pages[i].req_count ){
			smallest = page_buffer->pages[i].req_count;
			least_loc = i;
		}
	}
	if( write_page( &(page_buffer->pages[least_loc]) ) >= 0 ){
		// reset the page_buffer location to base values a.k.a its available now
		page_buffer->pages[least_loc].page_id = -1;
		page_buffer->pages[least_loc].req_count = -1;
		memset(page_buffer->pages[least_loc].page_records, 0, db_data->page_size*sizeof(r_item));
	}

	return least_loc;
}

/*
 * Find first available position in page buffer to write to.
 * Return location in buffer o.w. -1 for failure.
 */
int get_open_spot(){
	for( int i = 0; i < page_buffer->num_of_pages; i++){
		if ( page_buffer->pages[i].page_id == -1 ){
			return i;
		}
	}
	return -1;
}

/*
 * Write page information to the corresponding page file
 * on the disk.
 * Return 0 with success and -1 for failure.
 */
int write_page( page_info* page ){
	FILE* fp;
	int file_len = strlen(db_data->db_location) + PAGE_FILE_LEN + MAX_PAGES_FILE_CHARS;
	char *page_file = (char *)malloc(file_len*sizeof(char));
	memset(page_file, 0, file_len*sizeof(char));
	strcat(page_file, db_data->db_location);
	strcat(page_file, PAGE_FILENAME_BEGIN);
	sprintf(page_file, "%s_%d",page_file,page->page_id);

	fp = fopen(page_file, "wb");
	if( fp == NULL ){
		fprintf(stderr, "ERROR: write_page, invalid page file %s\n", page_file);
		return -1;
	}
	// DON'T write req_count only use when page is in buffer.
	fwrite(page->page_records,sizeof(r_item),db_data->page_size,fp);

	free( page_file );
	fclose( fp );
	return 0;
}

/*
 * Read page information from disk into the passed structure.
 * Treats the records as a one dimensional array of record_items
 * because the page structure doesn't care about the table structure
 * for the records.
 * Return 0 for success and -1 for failure.
 */
int read_page( int page_id, page_info* page ){
	FILE* fp;
	long fsize;
	int file_len = strlen(db_data->db_location) + PAGE_FILE_LEN + MAX_PAGES_FILE_CHARS;
	char *page_file = (char *)malloc(file_len*sizeof(char));
	memset(page_file, 0, file_len*sizeof(char));
	strcat(page_file, db_data->db_location);
	strcat(page_file, PAGE_FILENAME_BEGIN);
	sprintf(page_file, "%s_%d",page_file,page->page_id);

	fp = fopen(page_file, "rb");
	if( fp == NULL ){
		fprintf(stderr, "ERROR: read_page, invalid page file %s\n", page_file);
		return -1;
	}
	// read page file into 2d array
	page->page_id = page_id;
	page->num_of_records = (int)floor(db_data->page_size / sizeof(r_item));
	fread(page->page_records,sizeof(r_item),db_data->page_size,fp);
	page->req_count = 0;
	return 0;
}

/*
 * This will purge the page buffer to disk.
 * @return 0 on success, -1 on failure.
 */
int purge_buffer(){
	int result = 0;
    // basically loop through the buffer array and write the pages to the filee system
	for(int i = 0; i < page_buffer->num_of_pages; i++){
		if( write_page( &(page_buffer->pages[i]) ) == -1){
			result = -1;
		}
	}
	return result;
}

void free_buffer(){
	for(int i = 0; i < page_buffer->num_of_pages; i++){
		free( page_buffer->pages[i].page_records );
	}
	free( page_buffer->pages );
}

void print_page_buffer(){
    printf("Page Buffer with Last id '%d' and num of pages '%d'\n", page_buffer->last_id, page_buffer->num_of_pages);
    for(int i = 0; i < page_buffer->num_of_pages; i++){
        printf("\tPage %d: [num of records = %d] [req count = %d]\n", page_buffer->pages[i].page_id, page_buffer->pages[i].num_of_records, page_buffer->pages[i].req_count);
    }
}

/*
 * Just for testing. 
 * TODO: REMOVE!!
 */
int main(int argc, char const *argv[])
{
	// needed to start
	char db_path[] = "/home/stu2/s17/jxg4323/Courses/CSCI421/Project/TestDb/";
	int page_size = 4096;
	int buffer_size = 10;
	bool restart_flag = false;

	printf("Database Path %s\n", db_path);
	printf("%s\n", DATABASE_CONFIG_FILE);

	// Database config testing
	int result;
	result = create_database(db_path, page_size, buffer_size, restart_flag);
	
	// Lookup Table Testing
	int table_count = MIN_TABLE_COUNT;
	int arr_size = MIN_BIN_SIZE;
	// Create some test data
	if(restart_flag){
		// Read in lookup File
		read_lookup_file( db_path, table_l );

		print_lookup_table( table_l );

		write_db_config( db_path ); // Good

		get_db_config( db_path, db_data );  // Good
		pretty_print_db_config( db_data );
		pretty_print_db_config( db_data ); 

		get_all_schemas( db_path );
		pretty_print_table_schemas( all_table_schemas );

		terminate_database();
	}else{
		// for( int i = 0; i < table_count; i++ ){
		// 	for(int j = 0; j < arr_size; j++ ){
		// 		((table_l->table_data)[i]).byte_info[j][0] = j+1;
		// 		((table_l->table_data)[i]).byte_info[j][1] = j+2;
		// 		((table_l->table_data)[i]).byte_info[j][2] = j+3;
		// 	}
		// }
		// print_lookup_table( table_l );
		// printf("--------UPDATE TABLE INFO-------\n");
		// update_lookup_table(table_l, 1, 3, 13, 56);
		// print_lookup_table( table_l );
		// printf("--------NEW TABLE INFO-------\n");
		// add_table_info(table_l, 4);
		// update_lookup_table(table_l, 4, 1, 21, 69);
		// print_lookup_table( table_l );
		// printf("---------DELETE TABLE INFO------\n");
		// table_l = delete_table_info(table_l, 4);
		// print_lookup_table( table_l );
		// printf("---------------------\n");
		// printf("--------NEW TABLE INFO-------\n");
		// result = add_table_info(table_l, 1);
		// if( result == -1 ){
		// 	printf("ERROR: table %d already exists\n", 1);
		// }
		// update_lookup_table(table_l, 1, 1, 15, 76);
		// print_lookup_table( table_l );
		// printf("----------------------\n");
		// clear_table_bin( table_l, 1 );
		// print_lookup_table( table_l );
		// printf("----------------------\n");



		get_db_config( db_path, db_data );  // Good
		pretty_print_db_config( db_data );

		int * d_tmp = (int *)malloc(10*sizeof(int));
		int * k_tmp = (int *)malloc(10*sizeof(int));
		for( int i = 0; i < 10; i++){
			d_tmp[i] = i*5;
			k_tmp[i] = i *8;
		}
		result = add_table( d_tmp, k_tmp, 10, 10 );
		pretty_print_table_schemas( all_table_schemas );

		update_lookup_table(table_l, 0, 2, 15, 78);
		print_lookup_table( table_l );

		terminate_database();
	}
	return 0;
}
