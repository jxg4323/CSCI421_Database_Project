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
	allocate_schema_data();
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
	schema_result = get_table_schema(db_loc);
	if(lookup_result == -1 || lookup_result == -1){
		free( table_l );
		free_config( db_data );
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
	write_table_schema( db_data->db_location );
	// free used memory and return
	free_lookup_table( table_l );
	free_config( db_data );
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
 * Allocate memory for local table_schema_array with base data
 * and an array of table schemas of size 1.
 */
void allocate_schema_data(){
	all_table_schemas = (table_schema_array *)malloc(sizeof(table_schema_array));
	all_table_schemas->last_made_id = -1;
	all_table_schemas->table_count = -1;
	all_table_schemas->tables = (table_data *)malloc(sizeof(table_data));
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
 *
 */
void write_page_buffer(){
	// buffer written to physical page
	// 
}

void add_to_page_buffer(){
	// handle how to add information to page buffer & when excedes
}

int get_table_schema(char * db_loc){

}

int write_table_schema(char * db_loc){
	
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

		terminate_database();
	}else{
		for( int i = 0; i < table_count; i++ ){
			for(int j = 0; j < arr_size; j++ ){
				((table_l->table_data)[i]).byte_info[j][0] = j+1; 
				((table_l->table_data)[i]).byte_info[j][1] = j+2; 
				((table_l->table_data)[i]).byte_info[j][2] = j+3; 
			}
		}
		print_lookup_table( table_l );
		printf("--------UPDATE-------\n");
		update_lookup_table(table_l, 1, 3, 13, 56);
		print_lookup_table( table_l );
		printf("--------NEW TABLE-------\n");
		add_table_info(table_l, 4);
		update_lookup_table(table_l, 4, 1, 21, 69);
		print_lookup_table( table_l );
		printf("---------DELETE TABLE------\n");
		table_l = delete_table_info(table_l, 4);
		print_lookup_table( table_l );
		printf("---------------------\n");
		printf("--------NEW TABLE-------\n");
		result = add_table_info(table_l, 1);
		if( result == -1 ){
			printf("ERROR: table %d already exists\n", 1);
		}
		update_lookup_table(table_l, 1, 1, 15, 76);
		print_lookup_table( table_l );

		write_lookup_table( table_l, db_path );
		terminate_database();
	}
	return 0;
}

/*
 * This function will drop the table with the provided id
 * from the database. This will remove all data as well as information
 * about the table.
 * @param table_id - the id of the table to drop
 * @return 0 if table succesfully dropped, -1 otherwise.
 */
int drop_table( int table_id ){
    int cleared = clear_table(table_id);
    if(cleared == 0){
        FILE *file;
        FILE *temp;
        file = fopen(TABLE_METADATA_FILE, "rb");
        temp = fopen("temp", "wb");
        if( file == NULL || temp == NULL){ return -1; }
        fseek(file, 0, SEEK_END);
        long file_size = ftell(file);
        fseek(file, 0, SEEK_SET);
        int id;
        int key_indices_size;
        int *key_indices;
        int data_types_size;
        int *data_types;
        fread(&id, sizeof(int), 1, file);
        fwrite(&id, sizeof(int), 1, temp);
        fread(&id, sizeof(int), 1, file);
        while(id != table_id){
            fwrite(&id, sizeof(int), 1, temp);
            fread(&data_types_size, sizeof(int), 1, file);
            fwrite(&data_types_size, sizeof(int), 1, temp);
            int *data_types = malloc(sizeof(int) * data_types_size);
            fread(&data_types, sizeof(int), data_types_size, file);
            fwrite(&data_types, sizeof(int), data_types_size, temp);
            free(data_types);
            fread(&key_indices_size, sizeof(int), 1, file);
            fwrite(&key_indices_size, sizeof(int), 1, temp);
            int *key_indices = malloc(sizeof(int) * key_indices_size);
            fread(&key_indices, sizeof(int), key_indices_size, file);
            fwrite(&key_indices, sizeof(int), key_indices_size, temp);
            free(key_indices);
            fread(&id, sizeof(int), 1, file);
        }
        fread(&data_types_size, sizeof(int), 1, file);
        fseek(file, sizeof(int) * data_types_size, SEEK_CUR);
        fread(&key_indices_size, sizeof(int), 1, file);
        fseek(file, sizeof(int) * key_indices_size, SEEK_CUR);
        while( file_size != ftell(file)){
            fread(&id, sizeof(int), 1, file);
            fwrite(&id, sizeof(int), 1, temp);
            fread(&data_types_size, sizeof(int), 1, file);
            fwrite(&data_types_size, sizeof(int), 1, temp);
            int *data_types = malloc(sizeof(int) * data_types_size);
            fread(&data_types, sizeof(int), data_types_size, file);
            fwrite(&data_types, sizeof(int), data_types_size, temp);
            free(data_types);
            fread(&key_indices_size, sizeof(int), 1, file);
            fwrite(&key_indices_size, sizeof(int), 1, temp);
            int *key_indices = malloc(sizeof(int) * key_indices_size);
            fread(&key_indices, sizeof(int), key_indices_size, file);
            fwrite(&key_indices, sizeof(int), key_indices_size, temp);
            free(key_indices);
        }
        fclose(file);
        fclose(temp);
        remove(TABLE_METADATA_FILE);
        rename("temp", TABLE_METADATA_FILE);
        return 0;
    }
    return -1;
}

/*
 * This function will clear the table with the provided id
 * from the database. This will remove all data but not the table.
 * @param table_id - the id of the table to clear
 * @return 0 if table succesfully cleared, -1 otherwise.
 */
int clear_table( int table_id ){
    if(get_table_info(table_l, table_id) == -1){ return -1; }
    FILE *file;
    file = fopen(TABLE_METADATA_FILE, "rb");
    fseek(file, sizeof(int),SEEK_SET);
    if(NULL != file) {
        int record_length;
        int id;
        int key_indices_size;
        int data_types_size;
        fread(&id, sizeof(int), 1, file);
        while(id != table_id){
            fread(&data_types_size, sizeof(int), 1, file);
            fseek(file, sizeof(int) * data_types_size, SEEK_CUR);
            fread(&key_indices_size, sizeof(int), 1, file);
            fseek(file, sizeof(int) * key_indices_size, SEEK_CUR);
            fread($id, sizeof(int), 1, file);
        }
        fread(&data_types_size, sizeof(int), 1, file);
        fseek(file, sizeof(int) * data_types_size, SEEK_CUR);
        fread(&key_indices_size, sizeof(int), 1, file);
        int *key_indices = malloc(key_indices_size * sizeof(int));
        fread(&key_indices, sizeof(int), key_indices_size, file);
        union record_item *key_values = malloc(sizeof(record_item) * key_indices_size);
        union record_item **records;
        int num_records = get_records(table_id, records);
        if (num_records > -1) {
            for (int i = 0; i < num_records; i++) {
                for(int j = 0; j < key_indices_size, j++){
                    key_values[j] = records[i][j]
                }
                remove_record(table_id, key_values);
            }
        }
        free(records);
        free(key_indices);
        free(key_values);
        return 0;
    }
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
    FILE *file;
    file = fopen(TABLE_METADATA_FILE, "a+b");
    int new_id = -1;
    if(NULL != file) {
        int last_id = -2;
        fread(&last_id, sizeof(int), 1, file);
        new_id = last_id + 1;
        if(new_id > 0) {
            fwrite(&new_id, sizeof(int), 1, file);
            fwrite(&data_types_size, sizeof(int), 1, file);
            fwrite(data_types, sizeof(int), data_types_size, file);
            fwrite(&key_indices_size, sizeof(int), 1, file);
            fwrite(key_indices, sizeof(int), key_indices_size, file);
            fclose(file);
            file = fopen(TABLE_METADATA_FILE, "r+b");
            if (NULL != file) {
                fwrite(&new_id, sizeof(int), 1, file);
            } else {
                return -1;
            }
        }
    }
    if(NULL != file){ fclose(file); }
    return new_id;
}