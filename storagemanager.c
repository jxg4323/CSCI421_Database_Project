/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#include "storagemanager.h"
#include "lookupmanager.h"
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
	allocate_all_schemas();
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
	db_result = get_db_config(db_loc, db_data);  
	lookup_result = read_lookup_file(db_loc, table_l);
	schema_result = get_all_schemas(db_loc);
	if(db_result == -1 || lookup_result == -1 || schema_result == -1){
		free( table_l );
		free_config( db_data );
		// TODO: create free function for table schema
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
    char * empty_buffer = (char *)malloc(buffer_size*sizeof(char));
    fwrite(&(page_size), sizeof(int), 1, dbFile);
    fwrite(&(buffer_size), sizeof(int), 1, dbFile);
    fwrite(db_loc, sizeof(char), db_loc_len, dbFile);
    fwrite(empty_buffer, sizeof(char), buffer_size, dbFile);
    // initialize base values for lookup table
    initialize_lookup_table(MIN_TABLE_COUNT, table_l);
    for(int t_id = 0; t_id < MIN_TABLE_COUNT; t_id++){
    	init_table_pages(MIN_BIN_SIZE, t_id, &(table_l->table_data[t_id]));
    }
    db_data->page_size = page_size;
    db_data->buffer_size = buffer_size;
    strcpy(db_data->db_location, db_loc);

    free(empty_buffer);
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
	update_db_config( db_data->db_location, db_data->page_buffer );
	// get lookup table & confirm written
	write_lookup_table( table_l, db_data->db_location );
	// Write tablemetadata file
	write_all_schemas( db_data->db_location );
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
	db_data->page_buffer = (char *)malloc(buf_size*sizeof(char));
	memset(db_data->db_location, 0, db_loc_len*sizeof(char));
	memset(db_data->page_buffer, 0, buf_size*sizeof(char));
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
		all_table_schemas->tables = (table_data *)realloc(all_table_schemas->tables, count*sizeof(table_data));
	}else{
		all_table_schemas->tables = (table_data *)malloc(count*sizeof(table_data));
	}
}

/*
 * Initialize the given structure with the correct id, type array length, and
 * key indices array length. Also, allocate the memory required for the arrays.
 */
void init_table_schema(int t_id, int types_len, int key_len, table_data *t_schema){
	t_schema->id = t_id;
	t_schema->data_types_size = types_len;
	t_schema->key_indices_size = key_len;
	t_schema->data_types = malloc(types_len*sizeof(int));
	t_schema->key_indices = malloc(key_len*sizeof(int));
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
	
	strcpy(db_data->page_buffer, buffer);


	char * config_file = (char *)malloc(db_loc_len*sizeof(char));
	memset(config_file, 0, db_loc_len*sizeof(char));
	strcat(config_file, db_loc);
	strcat(config_file, DATABASE_CONFIG_FILE);

	dbFile = fopen(config_file, "wb"); 
	if(dbFile == NULL){
    	fprintf(stderr,"ERROR: update_db_config invalid database config file %s\n",db_loc);
    	return -1;
    }
    fwrite(&(db_data->page_size), sizeof(int), 1, dbFile);
    fwrite(&(db_data->buffer_size), sizeof(int), 1, dbFile);
    fwrite(db_data->db_location, sizeof(char), db_loc_len, dbFile);
    fwrite(db_data->page_buffer, sizeof(char), db_data->buffer_size, dbFile);

    free( config_file );
	fclose(dbFile);
	return 0;
}

/*
 * Free database config volatile memory.
 */
void free_config( db_config *config ){
	free(config->db_location);
	free(config->page_buffer);  
	free(config);
}

/*
 * This will pruge the page buffer to disk.
 * @return 0 on success, -1 on failure.
 */
int purge_buffer(){
    // basically loop through the buffer array and write the pages to the filee system
}


void free_table_schemas( table_schema_array* schemas ){
	for( int i = 0; i < schemas->table_count; i++ ){
		free( schemas->tables[i].data_types );
		free( schemas->tables[i].key_indices );
	}
	free( schemas->tables );
}

/*
 * Pretty print the table schemas
 */
void pretty_print_table_schemas( table_schema_array *schemas ){
	printf(" Table Schemas \n");
	for(int i = 0; i < schemas->table_count; i++){
		printf("Table ID: %d Data types: [ ", schemas->tables[i].id);
		for( int j = 0; j < schemas->tables[i].data_types_size; j++){
			if( j == schemas->tables[i].data_types_size-1 ){
				printf("%d ", schemas->tables[i].data_types[j]);
			}else{
				printf("%d, ", schemas->tables[i].data_types[j]);
			}
		}
		printf(" ], Key Indices: [ ");
		for( int j = 0; j < schemas->tables[i].key_indices_size; j++){
			if( j == schemas->tables[i].key_indices_size-1 ){
				printf("%d ", schemas->tables[i].key_indices[j]);
			}else{
				printf("%d, ", schemas->tables[i].key_indices[j]);
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
		init_table_schema(t_id, types_len, key_len, &(all_table_schemas->tables[table_indx]));
		fread((all_table_schemas->tables[table_indx]).data_types,sizeof(int),types_len,schema_fp);
		fread((all_table_schemas->tables[table_indx]).key_indices,sizeof(int),key_len,schema_fp);
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
		fwrite(&(all_table_schemas->tables[i].id),sizeof(int),1,wFile);
		fwrite(&(all_table_schemas->tables[i].data_types_size),sizeof(int),1,wFile);
		fwrite(&(all_table_schemas->tables[i].key_indices_size),sizeof(int),1,wFile);
		fwrite(all_table_schemas->tables[i].data_types,sizeof(int),all_table_schemas->tables[i].data_types_size*sizeof(int),wFile);
		fwrite(all_table_schemas->tables[i].key_indices,sizeof(int),all_table_schemas->tables[i].key_indices_size*sizeof(int),wFile);
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
		if ( all_table_schemas->tables[i].id == table_id ){
			return &(all_table_schemas->tables[i]);
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
    //if(cleared == -1){ return -1; }
	// delete the table struct from the metadata array
    int table_count = all_table_schemas->table_count;
    int offset = 0;
    table_data *table_array = malloc(sizeof(table_data) * (table_count-1));
    for(int i = 0; i < table_count; i++){
        if(table_id != all_table_schemas->tables[i].id){
            table_data *table = malloc(sizeof(table_data));
            table->id = all_table_schemas->tables[i].id;
            table->data_types_size = all_table_schemas->tables[i].data_types_size;
            table->key_indices_size = all_table_schemas->tables[i].key_indices_size;
            table->data_types = malloc(sizeof(int) * table->data_types_size);
            for(int j = 0; j < table->data_types_size; j++){
                table->data_types[j] = all_table_schemas->tables[i].data_types[j];
            }
            table->key_indices = malloc(sizeof(int) * table->key_indices_size);
            for(int j = 0; j < table->key_indices_size; j++){
                table->key_indices[j] = all_table_schemas->tables[i].key_indices[j];
            }
            table_array[i-offset] = *table;
        } else {
            offset = 1;
        }
        free(all_table_schemas->tables[i].key_indices);
        free(all_table_schemas->tables[i].data_types);
        free(&(all_table_schemas->tables[i]));
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
   	init_table_schema( new_id, data_types_size, key_indices_size, &(all_table_schemas->tables[end_indx]) );
   	/*for(int i = 0; i < data_types_size; i++){
   	    all_table_schemas->tables[end_indx].data_types[i] = data_types[i];
   	}
    for(int i = 0; i < key_indices_size; i++){
        all_table_schemas->tables[end_indx].key_indices[i] = key_indices[i];
    }*/
   	memcpy( all_table_schemas->tables[end_indx].data_types, data_types, data_types_size*sizeof(int) );
   	memcpy( all_table_schemas->tables[end_indx].key_indices, key_indices, key_indices_size*sizeof(int) );
   	all_table_schemas->last_made_id = new_id;
   	all_table_schemas->table_count = end_indx + 1;
   	// add_table_info(table_l, new_id);
    return new_id;
}

/*
 * Just for testing. 
 * TODO: REMOVE!!
 */
int main(int argc, char const *argv[])
{
	// needed to start
	char db_path[] = "/home/stu3/s8/amf3379/Courses/CS421/Project_Test/";
	int page_size = 25;
	int buffer_size = 50;
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

		char * new_buf = (char *)malloc(buffer_size*sizeof(char));
		new_buf = "bleh blah boop beep";
		update_db_config( db_path, new_buf ); // Good

		get_db_config( db_path, db_data );  // Good
		pretty_print_db_config( db_data );
		pretty_print_db_config( db_data ); 

		get_all_schemas( db_path );
		pretty_print_table_schemas( all_table_schemas );

		terminate_database();
	}else{
        print_lookup_table( table_l );
        printf("----------------------\n");
        pretty_print_table_schemas(all_table_schemas);
        print_lookup_table( table_l );
		terminate_database();
	}
	return 0;
}
