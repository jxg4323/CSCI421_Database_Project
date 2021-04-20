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
			if(result == 0){
				printf("SUCCESS\n");
			}else{
				fprintf(stderr,"ERROR\n");
			}
			free( newStr );
			return 0;
		}else{
			fprintf(stderr, "ERROR: BAD QUERY. \n");
			free( newStr );
			return -1;
			//bad query.
		}
	}else{
		// TODO: IMPLEMENT DML PARSER CALL (insert, update, delete)
		fprintf(stderr,"ERROR: either bad query or query was meant for the DML parser.\n");
		free( newStr );
		return -1;
		//either dml or bad query
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
	// It is just stubbed out for now.
	catalogs* schemas = get_schemas(); // used to pass table info to dmlparser
	return 0;
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
			free( tokenString );
			print_logs( );
		}else if((quit = strcasecmp(token, "quit")) == 0){
			printf("EXITING PROGRAM....\n");
			free( tokenString );
            break;
		}else{ // TODO: maybe have token check for insert, update, delete
			execute_non_query(input);
			free( tokenString );
		}
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

	//int run_result = run( argc,argv );

	// Condition Stack Test
	where_cmd* top = NULL;

	conditional_cmd* first = new_condition_cmd();
	conditional_cmd* secnd = new_condition_cmd();

	first->first_attr = 0;
	first->comparator = gt;
	first->value.i = 0;
	secnd->first_attr = 1;
	secnd->comparator = lt;
	secnd->value.i = 3;

	push_where_node(&top, COND, first);
	push_where_node(&top, AND, NULL);
	push_where_node(&top, COND, secnd);

	where_cmd* temp = peek(top);

	pop_where_node(&top);
	pop_where_node(&top);
	pop_where_node(&top);

	if( where_is_empty(top) ){
		printf("Removed all nodes\n");
	}else{
		printf("There is still some left\n");
	}

	return 0;
}

/*
 * Loop through the provided tokens, and create the table catalog
 * and initialize the table metadata for the storagemanager.
 * @param cat - array of table_catalog structures
 * @param token_count - number of tokens given
 * @param tokens - array of tokens that should have already been
 				   been validated by the parser and between each
 				   command is an empty string.
 * @return the metadata id of the table if successful and -1 o.w.
 */
int create_table( catalogs *cat, int token_count, char** tokens ){
    int current = 2;
    bool valid = true;
    // return -1 if name is in use
    if( check_table_name(cat, tokens[current]) ){
    	fprintf(stderr, "ERROR: table %s already exists\n", tokens[current] );
    	return -1;
    }
    int table_index = cat->table_count;
    manage_catalogs(cat, cat->table_count+1, true);
    init_catalog(&(cat->all_tables[table_index]), table_index, 0, 0, 0, 0, 0, tokens[current]);
    current = 4; // skip over empty string
    while(current < token_count && valid ){
        if(strcasecmp(tokens[current], "primarykey") == 0){
            current++;
            char ** prims = (char **)malloc(sizeof(char *));
            int prim_count = 0;
            while(strcmp(tokens[current], "") != 0){
                prim_count++;
                prims = (char **)realloc(prims, prim_count * sizeof(char *));
                prims[prim_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( prims[prim_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(prims[prim_count-1], tokens[current]);
                current++;
            }
            int added = add_primary_key(&(cat->all_tables[table_index]), prims, prim_count);
            if(added == -1){
                valid = false;
            }
            current++;
            for( int i = 0; i < prim_count; i++ ){
            	free( prims[i] );
            }
            free( prims );
        } else if(strcasecmp(tokens[current], "unique") == 0){
            current++;
            char ** uniques = (char **)malloc(sizeof(char *));
            int uniq_count = 0;
            while(strcmp(tokens[current], "") != 0){
                uniq_count++;
                uniques = (char **)realloc(uniques, uniq_count * sizeof(char *));
                uniques[uniq_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( uniques[uniq_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(uniques[uniq_count-1], tokens[current]);
                current++;
            }
            int added = add_unique_key(&(cat->all_tables[table_index]), uniques, uniq_count);
            if(added == -1){
                valid = false;
            }
            current++;
            for( int i = 0; i < uniq_count; i++ ){
            	free( uniques[i] );
            }
            free( uniques );
        } else if(strcasecmp(tokens[current], "foreignkey") == 0){
            current++;
            int foreign_count = 1;
            char *f_name;
            char **foreigns = (char **)malloc(sizeof(char *));
            int indx = 0;
            while(strcasecmp(tokens[current], "references") != 0){
                foreigns = (char **)realloc(foreigns, foreign_count * sizeof(char *));
                foreigns[indx] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( foreigns[indx], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(foreigns[indx], tokens[current]);
                indx++;
                foreign_count++;
                current++;
            }
            current++;
            f_name = (char*)malloc(strlen(tokens[current]+1)*sizeof(char));
            memset( f_name, '\0', (strlen(tokens[current])+1)*sizeof(char));
            strcpy( f_name, tokens[current] );
            current++;
            while(strcmp(tokens[current], "") != 0){
                foreigns = (char **)realloc(foreigns, foreign_count * sizeof(char *));
                foreigns[indx] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( foreigns[indx], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(foreigns[indx], tokens[current]);
                foreign_count++;
                current++;
            }
            int added = add_foreign_data(cat, &(cat->all_tables[table_index]), foreigns, indx, f_name);
            if(added == -1){
                valid = false;
            }
            current++;
            for( int i = 0; i < foreign_count; i++ ){
            	free( foreigns[i] );
            }
            free( foreigns );
            free( f_name );
        } else {
            int name_idx = current++;
            int type_idx = current++;
            int str_len = -1;
            int constraints[3] = { 0 };
            union record_item ignored_value;
            while(strcmp(tokens[current], "") != 0 && valid != false){
                if(strcasecmp(tokens[current], "notnull") == 0){
                    constraints[0] = 1;
                    current++;
                } else if(strcasecmp(tokens[current], "primarykey") == 0){
                    constraints[1] = 1;
                    current++;
                } else if(strcasecmp(tokens[current], "unique") == 0){
                    constraints[2] = 1;
                    current++;
                } else if( is_number( tokens[current] ) ){ // attribute is a char/varchar & size is irrelevant b/c of fixed record size
                	str_len = atoi(tokens[current]); 
                	current++;
                } else {
                    valid = false;
                }// TODO: add token catch for default value NO!
            }
            int added = -1;
            if(valid != false) {
                added = add_attribute(&(cat->all_tables[table_index]), tokens[name_idx], tokens[type_idx], constraints, str_len, 0, ignored_value);
            }
            if(added == -1){
                valid = false;
            }
            current++;
        }
    }
    if( valid == false ){
    	delete_table( &(cat->all_tables[table_index]) );
        return -1;
    } else {
    	int* data_types = get_table_data_types( &(cat->all_tables[table_index]) );
    	int num_types = cat->all_tables[table_index].attribute_count;
    	int num_keys = cat->all_tables[table_index].primary_size;
    	int meta_id = add_table( data_types, cat->all_tables[table_index].primary_tuple, num_types, num_keys );
    	if( meta_id == -1 ){
    		fprintf(stderr, "ERROR: unable to add table %s to metadata file.\n", cat->all_tables[table_index].table_name );
    		delete_table( &(cat->all_tables[table_index]) );
    		return -1;
    	}
    	cat->all_tables[table_index].storage_manager_loc = meta_id;
    	free( data_types );
        return meta_id;
    }
}

int drop_table_ddl( catalogs *cat, char *name ){
    int idx = get_catalog(cat, name);
    if( idx == -1 ){
    	fprintf(stderr, "ERROR: attempted to drop an unknown table %s\n", name);
    	return idx;
    }
    int success = drop_table( cat->all_tables[idx].storage_manager_loc );
    if( success == -1 ){ return success; }
    delete_table( &(cat->all_tables[idx]) );
    return 1;
}

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
 * Loop through the provided tokens parsing for command authenticity
 * and if command is genuine then execute add/drop command.
 * 
 * @parm: cat - Catalog of table schemas
 * @parm: token_count - Number of tokens in the alter statement
 * @parm: tokens - command statement tokens formatted as follows:
 				   ["alter", "table", "<table_name>", "add", "<attr_name>", 
 				   "<type>", "<size>", "default", "<default_value>"]
 * @return: 0 with success and -1 otherwise.
 */
int alter_table(catalogs *cat, int token_count, char **tokens){
    if(token_count != 6 && token_count != 7 && token_count != 9 && token_count != 10) {
        fprintf( stderr, "ERROR: wrong amount of tokens for alter table.\n" );
        return -1;
    } 
    // Command type catch
    if( strcasecmp(tokens[3], "add") == 0 ){
    	int name_check = confirm_name(tokens[4]);
    	int type = type_conversion(tokens[5]);
    	union record_item default_val;
    	int str_len = -1;
    	if( name_check == -1 ){ return -1; }
    	char* attr_name = strdup(tokens[4]);
    	if( type == -1 ){ 
    		fprintf(stderr, "ERROR: '%s' is not a valid type\n", tokens[5]);
    		return -1;
    	}else if( type == 3 || type == 4 ){ // then next token is length of char/varchar
    		if( is_number(tokens[6]) ){ 
    			str_len = atoi(tokens[6]); 
    		}else{
    			fprintf(stderr, "ERROR: '%s' isn't a valid character length for %s\n", tokens[6], tokens[5]);
    			return -1;
    		}
    		if( token_count == 8 ){
    			return add_attribute_table( cat, tokens[2], attr_name, tokens[5], default_val, 0, str_len);
    		}else if( token_count == 10 ){
    			if( strcasecmp(tokens[7],"default") != 0 ){
    				fprintf(stderr, "ERROR: '%s' has to be 'default' when setting a default value for '%s'\n", tokens[6], tokens[5]);
    				fprintf(stderr, "usage: char or varchar statements are layout follows:\nalter table <table_name> add/drop <attr_name> <type> default \"<defaul_value>\"\n");
    				return -1;
    			}
    			// set default value for attribute
    			if( get_default_value(tokens[8], attr_name, type, str_len, &default_val) == -1 ){ return -1; }
    			return add_attribute_table( cat, tokens[2], attr_name, tokens[5], default_val, 1, str_len );
    		}else{ // error
    			fprintf(stderr, "ERROR: invalid number of tokens for char or varchar attributes.\n");
    			fprintf(stderr, "usage: char or varchar statements are layout follows:\nalter table <table_name> add/drop <attr_name> <type> default \"<defaul_value>\"\n");
    			return -1;
    		}
    	}else{ // type is int, dobule, or bool
    		if( token_count == 7 ){ // no default value
    			return add_attribute_table( cat, tokens[2], attr_name, tokens[5], default_val, 0, -1 );
    		}else if( token_count == 9 ){ // default value
    			if( strcasecmp(tokens[6],"default") != 0 ){
    				fprintf(stderr, "ERROR: '%s' has to be 'default' when setting a default value for '%s'\n", tokens[6], tokens[5]);
    				fprintf(stderr, "usage: int,double, and boolean statements are layout follows:\nalter table <table_name> add/drop <attr_name> <type> default <defaul_value>\n");
    				return -1;
    			}
    			// set default value for attribute
    			if( get_default_value(tokens[7], attr_name, type, str_len, &default_val) == -1 ){ return -1; }
    			return add_attribute_table( cat, tokens[2], attr_name, tokens[5], default_val, 1, -1 );
    		}else{ // error
    			fprintf(stderr, "ERROR: invalid number of tokens for int, double, or boolean attributes.\n");
    			fprintf(stderr, "usage: int,double, and boolean statements are layout follows:\nalter table <table_name> add/drop <attr_name> <type> default <defaul_value>\n");
    			return -1;
    		}
    	}
    }else if( strcasecmp(tokens[3], "drop") == 0 ){
    	int name_check = confirm_name(tokens[4]);
    	if( name_check == -1 ){ return -1; }
    	char* attr_name = strdup(tokens[4]);
    	return drop_attribute( cat, tokens[2], attr_name );
    }else{
    	fprintf(stderr, "ERROR: '%s' is not a known command.\n", tokens[2]);
    	return -1;
    }
}

int drop_attribute(catalogs *cat, char *table, char *attribute){
    int table_loc = get_catalog(cat, table);
    if( table_loc == -1 ){
    	fprintf(stderr, "ERROR: '%s' table doesn't exist.\n", table);
        return -1;
    }
    int attribute_loc = get_attr_loc(&(cat->all_tables[table_loc]), attribute);
    if( attribute_loc == -1 ){
    	fprintf(stderr, "ERROR: '%s' attribute doesn't exist.\n", attribute);
        return -1;
    }
    int attribute_position = get_attr_idx(&(cat->all_tables[table_loc]), attribute_loc);
    if( attribute_position == -1 ){
        return -1;
    }
    int success = remove_attribute(cat, &(cat->all_tables[table_loc]), attribute);
    if( success == -1 ){
        return -1;
    }
    union record_item **records;
    int rec_count = get_records(cat->all_tables[table_loc].storage_manager_loc, &records);
    if( rec_count == -1 ){
        return -1;
    }
    success = drop_table(cat->all_tables[table_loc].storage_manager_loc);
    if( success == -1 ){
        return -1;
    }
    int new_attr_count = get_attribute_count_no_deletes(&(cat->all_tables[table_loc]));
    int *data_types = get_table_data_types( &(cat->all_tables[table_loc]) );

    int prim_count = cat->all_tables[table_loc].primary_size;
    int *prim_indices = malloc(sizeof(int) * prim_count);
    for(int i = 0; i < prim_count; i++){
        prim_indices[i] = get_attr_idx(&(cat->all_tables[table_loc]), cat->all_tables[table_loc].primary_tuple[i]);
    }
    int new_id = add_table(data_types, prim_indices, new_attr_count, prim_count);
    if( new_id == -1 ){
        return -1;
    }
    cat->all_tables[table_loc].storage_manager_loc = new_id;
    for(int i = 0; i < rec_count; i++){
    	union record_item *record = malloc(sizeof(union record_item) * new_attr_count);
        int offset = 0;
        for(int j = 0; j < new_attr_count + 1; j++){
            if(j == attribute_position){
                offset = 1;
            } else {
                record[j - offset] = records[i][j];
            }
        }
        insert_record(new_id, record);
    	free( record );
    }
    free( attribute );
    free(data_types);
    free(prim_indices);
    free(records);
    return 1;
}

/* 
 * Add an attribute and its information to the designated table.
 * Return 0 with success and -1 otherwise.
 *
 * @parm: cat - Table Schemas information
 * @parm: table - Name of the Table to add attribute to
 * @parm: name - Name of the new attribute
 * @parm: type - type of the attribute
 * @parm: def_val - holds the default value of the attribute or nothing if there is no default set
 * @parm: default_there - If 1 then def_val holds the default value for the attribute otherwise if 0
 						  the default value is ignored
 * @parm: char_len - Length the char/varchar string for storage.
 * @parm: value - default value for attribute,
 *			if char or varchar then NULL value is saved as "\0" character for size of record
 *			if int or double with NULL are stored as INTMAX
 *			if boolean store false
 */
int add_attribute_table(catalogs *cat, char *table, char *name, char *type, union record_item def_val, int default_there, int char_len){
    int table_loc = get_catalog(cat, table);
    if( table_loc == -1 ){
    	fprintf(stderr, "ERROR: '%s' table doesn't exist.\n", table);
        return -1;
    }

    union record_item **records;
    int rec_count = get_records(cat->all_tables[table_loc].storage_manager_loc, &records);
    if( rec_count == -1 ){
        return -1;
    }
    int constraints[3] = { 0 };
    int success = add_attribute(&(cat->all_tables[table_loc]),name,type,constraints, char_len, default_there, def_val);
    if( success == -1 ){
        return -1;
    }

    success = drop_table(cat->all_tables[table_loc].storage_manager_loc);
    if( success == -1 ){
        return -1;
    }
    int new_attr_count = get_attribute_count_no_deletes(&(cat->all_tables[table_loc]));
    int *data_types = get_table_data_types( &(cat->all_tables[table_loc]) );

    int prim_count = cat->all_tables[table_loc].primary_size;
    int *prim_indices = malloc(sizeof(int) * prim_count);
    for(int i = 0; i < prim_count; i++){
        prim_indices[i] = get_attr_idx(&(cat->all_tables[table_loc]), cat->all_tables[table_loc].primary_tuple[i]);
    }
    int new_id = add_table(data_types, prim_indices, new_attr_count, prim_count);
    if( new_id == -1 ){
        return -1;
    }
    cat->all_tables[table_loc].storage_manager_loc = new_id;
    
    union record_item new_record = def_val;
    for(int i = 0; i < rec_count; i++){
    	union record_item *record = malloc(sizeof(union record_item) * new_attr_count);
        for(int j = 0; j < new_attr_count; j++){
            if(j == new_attr_count - 1){
                record[j] = new_record;
            } else {
                record[j] = records[i][j];
            }
        }
        insert_record(new_id, record);
    	free(record);
    }
    free( name );
    free(data_types);
    free(prim_indices);
    free(records);
    return 1;
}

// Helper Functions
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
