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
	// TODO: allocate table metadata file
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
	db_result = get_db_config(db_loc, db_data);  
	lookup_result = read_lookup_file(db_loc, table_l);
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

	get_db_config( db_path, db_data );  // Good
	pretty_print_db_config( db_data );
	pretty_print_db_config( db_data ); 
	
	// Lookup Table Testing
	int table_count = MIN_TABLE_COUNT;
	int arr_size = MIN_BIN_SIZE;
	// Create some test data
	if(restart_flag){
		// Read in lookup File
		read_lookup_file( db_path, table_l );

		print_lookup_table( table_l );

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
		printf("---------------\n");
		update_lookup_table(table_l, 1, 3, 13, 56);
		print_lookup_table( table_l );
		printf("---------------\n");
		
		write_lookup_table( table_l, db_path );
		terminate_database();
	}
	return 0;
}