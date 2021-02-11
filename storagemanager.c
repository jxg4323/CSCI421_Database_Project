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