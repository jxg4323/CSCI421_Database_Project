/**
 * Authors: James Green, Alex Frankel, Kelsey Dunn, Varnit Tewari
 * Due Date: 3/12/2021
 * Assignment: Phase 2 Database Project
 * Professor: Scott Johnsson
 */
#include "database1.h"
#include "database.h"
#include "ddlparse.h"

/*
 * This function will be used to execute SQL statements that
 * do not return any data. For instance, schema modification,
 * insertion, delete, and update statements.
 *
 * @param statement - the SQL statement to execute.
 * @return 0 if the statement is executed successfully
           -1 otherwise
 */
int execute_non_query(char * statement){
	char *newStr = (char *)malloc((strlen(statement)+1)*sizeof(char));
	memset( newStr, '\0', (strlen(statement)+1)*sizeof(char));
	strcpy(newStr, statement);

	char delimit[] = " \t\r\n\0";
	char *token = strtok(newStr, delimit);

	if(strncasecmp(token, "create", 6) == 0 || strncasecmp(token, "alter", 5) == 0 || strncasecmp(token, "drop", 4) == 0){
		token = strtok(NULL, delimit);
		if(strncasecmp(token, "table", 5) == 0){
			int result = parse_ddl_statement(statement);
			if(result >= 0){
				printf("SUCCESS\n");
			}else{
				fprintf(stderr,"ERROR\n");
			}
			free( newStr );
			return 0;
		}else{
			fprintf(stderr, "ERROR: '%s' doesn't match 'table'. \n", token);
			free( newStr );
			return -1;
		}
	}else{
		fprintf(stderr,"ERROR: bad query.\n");
		free( newStr );
		return -1;
	}
	return 0;
}

/*
 * This function will be used to execute SQL statement that
 * return data. For instance, SQL SELECT queries.
 *
 * @param query - the SQL query to execute
 * @param result - a 2d array of record_item (output variable).
 *                This will be used to output the values of the query.
                  This will be a pointer to the first item in the 2d array.
				  The user of the function will be resposible for freeing.
 * @return the number of tuples in the result, -1 if upon error.
 */
int execute_query(char * query, union record_item *** result){
	// This function will be used when executing database queries that return tables of data.
	int res = 0;
	catalogs* schemas = get_schemas(); // used to pass table info to dmlparser

	char *newStr = (char *)malloc((strlen(query)+1)*sizeof(char));
	memset( newStr, '\0', (strlen(query)+1)*sizeof(char));
	strcpy(newStr, query);

	char delimit[] = " \t\r\n\0";
	char *token = strtok(newStr, delimit);

	if( strcasecmp(token, "select") == 0 ){
		res = parse_dml_query( query, result, schemas );
	}else{
		res = parse_dml_statement( query, schemas );
	}

	return res;
}

/*
 * This function is resposible for safely shutdown the database.
 * It will store to hardware any data needed to restart the database.
 * @return 0 on success; -1 on failure.
 */
int shutdown_database(){
	printf("-------TERMINATING DATABASE-----------\n");
	int result = 0;
	result = terminate_logs( db_path );
	result = terminate_database();
	free( db_path );
	return 0;
}

/* 
 * Loops through the provided string and returns true
 * if every character is a digit and false o.w.
 */
bool is_number( char *str ){
	bool isNum = true;
	int period_count = 0;
	for( int i = 0; i < strlen(str); i++ ){
		if( !isdigit(str[i]) ){ isNum = false; }
		if( str[i] == '.' ){
			isNum = true;
			period_count++;
		}
	}
	return isNum && period_count <= 1;
}

/*
 * Returns true 1 if the arguments are valid, false 0 if not.
 */
int arg_manager(bool restart, char *argv[], int argc){
	if(restart){
		//if restart is true
		//doesn't matter if args 3 & 4 are given bc they get ignored
		if(argc == 2 || argc == 4){
			printf("ARGUMENTS VALID....\n");
			printf("ATTEMPTING DATABASE RESTART...\n");
		}else{
			fprintf(stderr, "INVALID NUMBER OF ARGUMENTS.\n");
			usage( true );
    		return 0;
		}
	}else{
		if(argc == 4){
			bool arg2check = is_number(argv[2]);
			bool arg3check = is_number(argv[3]);
			if(arg2check && arg3check){
				printf("ARGUMENTS VALID....\n");
            			printf("ATTEMPTING NEW DATABASE START...\n");
			}else{
				if(!arg2check){
					fprintf(stderr, "ERROR: INVALID ARGUMENT... page_size: '%s' isn't a number \n", argv[2]);
				}else if( !arg3check ){
					fprintf(stderr, "ERROR: INVALID ARGUMENT... buffer_size: '%s' isn't a number \n", argv[3]);
				}else{
					fprintf(stderr, "ERROR: BOTH page_size: '%s' and buffer_size: '%s' are invalid numbers\n", argv[2], argv[3]);
				}
				return 0;
			}
		}else{
			usage( true );
    		return 0;
		}
	}
	return 1;
}

/*
 * Open the directory and confirm the metadata file exists
 * and isn't empty.
 * Return 1 to restart the database
 * 		  0 to create a new database
 *       -1 with an error
 */
int health_check( ){
	int restart = 0;
	DIR* directory = opendir( db_path );
	if(directory){ // if directory can be opened check metadata file
		int length = snprintf(NULL, 0, "%smetadata.dat", db_path);
		char * meta_loc = (char*)malloc(sizeof(char)*length+1);
		snprintf(meta_loc, length+1, "%smetadata.data", db_path);
		int file_len = strlen(db_path) + TABLE_CATALOG_FILE_LEN;
		char * cat_file = (char *)malloc(file_len*sizeof(char));
		memset(cat_file, 0, file_len*sizeof(char));
		strcat(cat_file, db_path);
		strcat(cat_file, TABLE_CATALOG_FILE);
		int size = 0;
		if( access( meta_loc, F_OK ) == 0 ){ // File exists
			FILE* fp = fopen(meta_loc, "rb");
			fseek( fp, 0, SEEK_END );
			size = ftell( fp );
			restart = (size > 0) ? 1 : 0; // only restart if metadata file has contents
			fclose( fp );
		}else{
			restart = 0;
		}

		// confirm table schema is there as well
		if( access( cat_file, F_OK) == 0 ){
			FILE* fp = fopen(cat_file, "rb");
			fseek( fp, 0, SEEK_END );
			size = ftell( fp );
			restart = ( size > 0 ) ? 1: 0; 
			fclose( fp );
		}else{
			restart = 0;
		}

		free( cat_file );
		free( meta_loc );
	}else if (ENOENT == errno){
		fprintf(stderr, "ERROR: Directory in path %s doesn't exist.\n", db_path );
		restart = -1;
	}
	closedir(directory);
	return restart;
}


int run(int argc, char *argv[]){
	if( argv[1] != NULL ){
		int length = strlen(argv[1]);
		db_path = (char *)malloc((length+1)*sizeof(char));
		strcpy(db_path, argv[1]);
	}else{
		usage(true);
		return -1;
	}
	int restart = health_check( );
	if(restart == 1){
		//if directory exists, call restart
		int results = arg_manager(true, argv, argc);
		if(results){
			printf("------------RESTARTING DATABASE----------------\n");
			create_database(argv[1], 0, 0, true); //create_db calls restart in storagemanager
			if( read_logs( db_path ) == -1 ){ return -1; }
		}else{
			printf("TERMINATING PROGRAM.\n");
            return 0;
		}
	}else if( restart == 0) {
		//if directory exists but no file contents, call start
		int results = arg_manager(false, argv, argc);
		if(results){
			printf("------------CREATING DATABASE----------------\n");
    		create_database(argv[1], atoi(argv[2]), atoi(argv[3]), false);
			printf("DATABASE CREATED SUCCESSFULLY.\n");
		}else{
			fprintf(stderr,"ERROR: WASN'T ABLE TO CREATE NEW DATABASE. TERMINATING PROGRAM.\n");
            return -1;
		}
	}else{
		usage( true );
		return -1;
	}
	

	//loop for user input
	char *input = NULL;
	ssize_t n = 1024;
	ssize_t charRead;
	int quit = -1; //initialize quit
	char *tokenString;

	printf("Enter SQL query: ");
	while((charRead = getdelim(&input, &n, 59, stdin)) > 0){ //ascii for semicolon = 59

		//make copy of string so its not destroyed in tokenizing
		tokenString = (char *)malloc((charRead+1)*sizeof(char));
		strcpy(tokenString, input);

		//ignore whitespace by tokenizing
		char delims[] = DELIMITER;
		char *token = strtok(tokenString, delims);

		// TODO: FOR PHASE 2, test with new records
		if( strcasecmp(token, "test") == 0 ){
			union record_item people[4][3];
			people[0][0].i = 0;
			memcpy( people[0][1].v, "James\0", 6*sizeof(char));
			people[0][2].i = 23;

			people[1][0].i = 1;
			memcpy( people[1][1].v, "Kelsey\0", 7*sizeof(char));
			people[1][2].i = 22;

			people[2][0].i = 2;
			memcpy( people[2][1].v, "Alex\0", 5*sizeof(char));
			people[2][2].i = 20;

			people[3][0].i = 3;
			memcpy( people[3][1].v, "Varnit\0", 7*sizeof(char));
			people[3][2].i = 20;
			insert_record( 0, people[0] );
			insert_record( 0, people[1] );
			insert_record( 0, people[2] );
			insert_record( 0, people[3] );
			printf("INSERT!!\n");
			union record_item *record;
			union record_item key;
			key.i = 0;
			get_record( 0, &key, &record );
			printf("HERE!\n");
		}

		// View All Current Table Schemas in Volatile Memory
		if( strcasecmp(token, "print") == 0){
			printf("Current status of all Table Schemas in memory\n");
			print_logs( );
		}else if((quit = strcasecmp(token, "quit")) == 0){
			printf("EXITING PROGRAM....\n");
			free( tokenString );
            break;
		}else if(strcasecmp(token, "create") == 0 || strcasecmp(token, "alter") == 0 || strcasecmp(token, "drop") == 0 ){ 
			execute_non_query(input);
		}else if(strcasecmp(token, "select") == 0 || strcasecmp(token, "insert") == 0 || strcasecmp(token, "update") == 0 || strcasecmp(token, "delete") == 0 ){
			union record_item** query_res;
			execute_query( input, &query_res );
			free( query_res );
		}else{
			fprintf(stderr, "'%s' was not a recognized command.\n", token);
		}
		free( tokenString );
		printf("Enter SQL query: ");
	}
	free(input);
	
	shutdown_database();
	printf("DATABASE SHUTDOWN SUCCESSFUL.\n");
	return 0;
}

void usage(bool error){
	if( error ){
		fprintf( stderr, "USAGE: ./database <db_loc> <page_size> <buffer_size>\n" );
	}else{
		fprintf( stdout, "USAGE: ./database <db_loc> <page_size> <buffer_size>\n" );
	}
}


int main(int argc, char *argv[])
{

	int run_result = run( argc,argv );

	return 0;
}

// Helper Functions

/*
 * Check to make sure the name of the attribute isn't a reserved
 * word such as the constraionts or types.
 * @return 0 if the name isn't a reserved work 
 * 		  -1 otherwise
 */
int confirm_name(char* attr_name){
	int result = 0;
	if( strcasecmp(attr_name, "primarykey") == 0 ){ 
		fprintf(stderr, "ERROR: attribute name can't be key word primarykey\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "unique") == 0 ){
		fprintf(stderr, "ERROR: attribute name can't be key word unique\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "notnull") == 0 ){
		fprintf(stderr, "ERROR: attribute name can't be key word notnull\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "char") == 0 ){
		fprintf(stderr, "ERROR: attribute name can't be key word char\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "varchar") == 0 ){
		fprintf(stderr, "ERROR: attribute name can't be key word varchar\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "integer") == 0 ){
		fprintf(stderr, "ERROR: attribute name can't be key word integer\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "boolean") == 0 ){
		fprintf(stderr, "ERROR: attribute name can't be key word boolean\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "double") == 0 ){
		fprintf(stderr, "ERROR: attribute name can't be key word double\n" );
		result = -1;
	}else if( strcasecmp(attr_name, "default") == 0){
		fprintf(stderr, "ERROR: attribute name can't be key word default\n" );
		result = -1;
	}
	return result;
}

/*
 * Retrieve the default value for the provided attribute, if null
 * is the default then assign provided default for record otherwise
 * set the default value in the record param.
 *
 * Based on the type of attribute set the appropriate default value.
 * @parm: value - default value
 * @parm: type - type of the attribute
 * @parm: record - return paramter which will contain the default value
 * @return 0 if successfully able to set the record value
 *		  -1 otherwise.
 */
int get_default_value(char* value, char *attr_name, int type, int str_len, union record_item *record){
	bool null = (strcasecmp(value,"null") == 0);
	int result = 0;
	char *eptr;
	switch( type ){
		case 0: //int
			if( null ){ record->i = INT_MAX; }
			else{ record->i = atoi(value); }
			break;
		case 1: // double
			if( null ){ record->d = INT_MAX; }
			else{ record->d = strtod( value,&eptr ); }
			break;
		case 2: // boolean
			if( null ){ record->b = INT_MAX; }
			if(strcasecmp(value, "true") == 0){
	            record->b = true;
	        } else {
	            record->b = false;
	        }
			break;
		case 3: // char
			if( null ){ memset(record->c, '\0', (str_len)*sizeof(char)); }
			else if( str_len <= strlen(value) ){ // char default value is beyond size of char attribute
				fprintf(stderr, "ERROR: '%s' default value is too large for the '%s' attribue.\n", value, attr_name);
				result = -1;
			}
			else{ strcpy(record->c, value); }
			break;
		case 4: // varchar
			if( null ){ memset(record->v, '\0', (str_len)*sizeof(char)); }
			else{ strcpy(record->v, value); }
			break;
		default:
			fprintf(stderr, "ERROR: unknown type for '%s'\n", attr_name);
			result = -1;
			break;
	}
	return result;
}

/*
 * Count the number of occurences of the given character 
 * @param c in the provided string and return the count.
 * This function does NOT ignore case.
 */
int char_occur_count( char* str, char c ){
    int str_len = strlen(str);
    int occur = 0;
    for( int i = 0; i < str_len; i++ ){
        if( str[i] == c ){ occur++; } 
    }
    return occur;
}
