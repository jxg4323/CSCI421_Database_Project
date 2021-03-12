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

	if(strncmp(token, "create", 6) == 0 || strncmp(token, "alter", 5) == 0 || strncmp(token, "drop", 4) == 0){
		token = strtok(NULL, delimit);
		if(strncmp(token, "table", 5) == 0){
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
		fprintf(stderr,"ERROR: either bad query or query was meant for the DML parser.\n");
		free( newStr );
		return -1;
		//either dml or bad query
	}
	return 0;
}

// This function will be used when executing database queries that return tables of data.
// It is just stubbed out for now.
int execute_query(char * query, union record_item *** result){
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
	for( int i = 0; i < strlen(str); i++ ){
		if( !isdigit(str[i]) ){ isNum = false; }
	}
	return isNum;
}

/*
 * Returns true 1 if the arguments are valid, false 0 if not.
 */
int arg_manager(bool restart, char *argv[], int argc){
	if(restart){
		//if restart is true
		//doesn't matter if args 3 & 4 are given bc they get ignored
		switch( argc ){
			case 2:
				break;
			case 3: 
				break;
			case 4:
				break;
			default:
				usage( true );
	            return 0;
				break;
		}
		if(argc >= 2 && argc <= 4){
			printf("ARGUMENTS VALID....\n");
			printf("ATTEMPTING DATABASE RESTART...\n");
		}else{
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
				printf("INVALID ARGUMENTS...\n");
				return 0;
			}
		}else{
			printf("INVALID NUMBER OF ARGUMENTS.\n");
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
		int size = 0;
		if( access( meta_loc, F_OK ) == 0 ){ // File exists
			FILE* fp = fopen(meta_loc, "rb");
			fseek( fp, 0, SEEK_END );
			size = ftell( fp );
			restart = (size > 0) ? 1 : 0; // only restart if metadata file has contents
		}
		free( meta_loc );
	}else if (ENOENT == errno){
		fprintf(stderr, "ERROR: Directory in path %s doesn't exist.\n", db_path );
		restart = -1;
	}
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
		tokenString = (char *)malloc(charRead*sizeof(char));
		strcpy(tokenString, input);

		//ignore whitespace by tokenizing
		char delims[] = DELIMITER;
		char *token = strtok(tokenString, delims);
    	if((quit = strcasecmp(token, "quit")) == 0){
			printf("EXITING PROGRAM....\n");
			free( tokenString );
            break;
		}else{
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

	int result = 0;
	int consts[3] = {NOTNULL, PRIMARYKEY, UNIQUE};
	//char *db_loc = "/home/stu2/s17/jxg4323/Courses/CSCI421/Project/TestDb/";
	char *db_loc = argv[1];

	run(argc,argv);

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
    init_catalog(&(cat->all_tables[table_index]), 0, 0, 0, 0, 0, tokens[current]);
    current = 4; // skip over empty string
    while(current < token_count && valid ){
        if(strcmp(tokens[current], "primarykey") == 0){
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
        } else if(strcmp(tokens[current], "unique") == 0){
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
        } else if(strcmp(tokens[current], "foreignkey") == 0){
            current++;
            int foreign_count = 1;
            char **foreigns = (char **)malloc(sizeof(char *));
            int key_count = 0;
            while(strcmp(tokens[current], "references") != 0){
                foreign_count++;
                key_count++;
                foreigns = (char **)realloc(foreigns, foreign_count * sizeof(char *));
                foreigns[foreign_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( foreigns[foreign_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(foreigns[foreign_count-1], tokens[current]);
                current++;
            }
            current++;
            strcpy(foreigns[0], tokens[current]);
            while(strcmp(tokens[current], "") != 0){
                foreign_count++;
                foreigns = (char **)realloc(foreigns, foreign_count * sizeof(char *));
                foreigns[foreign_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( foreigns[foreign_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(foreigns[foreign_count-1], tokens[current]);
                current++;
            }
            int added = add_foreign_data(cat, &(cat->all_tables[table_index]), foreigns, key_count);
            if(added == -1){
                valid = false;
            }
            current++;
            for( int i = 0; i < foreign_count; i++ ){
            	free( foreigns[i] );
            }
            free( foreigns );
        } else {
            int name_idx = current++;
            int type_idx = current++;
            int constraints[3] = { 0 };
            while(strcmp(tokens[current], "") != 0 && valid != false){
                if(strcmp(tokens[current], "notnull") == 0){
                    constraints[0] = 1;
                    current++;
                } else if(strcmp(tokens[current], "primarykey") == 0){
                    constraints[1] = 1;
                    current++;
                } else if(strcmp(tokens[current], "unique") == 0){
                    constraints[2] = 1;
                    current++;
                } else {
                    valid = false;
                }
            }
            int added = -1;
            if(valid != false) {
                added = add_attribute(&(cat->all_tables[table_index]), tokens[name_idx], tokens[type_idx], constraints);
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
    int success = drop_table( cat->all_tables[idx].id );
    if( success == -1 ){ return success; }
    delete_table( &(cat->all_tables[idx]) );
    return 1;
}

int alter_table(catalogs *cat, int token_count, char **tokens){
    if(token_count != 6 && token_count != 7 && token_count != 9) {
        fprintf( stderr, "ERROR: wrong amount of tokens for alter table.\n" );
        return -1;
    }
    if( strcasecmp(tokens[3], "add") == 0 ){
        if(token_count == 9){ // add attribute with default value
        	if( strcasecmp(tokens[7],"null") == 0 ){
        		tokens[7] = NULL;
        	}
            return add_attribute_table(cat, tokens[2], tokens[4], tokens[5], tokens[7]);
        } else { // add
            return add_attribute_table(cat, tokens[2], tokens[4], tokens[5], NULL);
        }
    }else if( strcasecmp(tokens[3], "drop") == 0 ){
    	return drop_attribute( cat, tokens[2], tokens[4] );
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
    int rec_count = get_records(cat->all_tables[table_loc].id, &records);
    if( rec_count == -1 ){
        return -1;
    }
    success = drop_table(cat->all_tables[table_loc].id);
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
    cat->all_tables[table_loc].id = new_id;
    union record_item *record = malloc(sizeof(union record_item) * new_attr_count);
    for(int i = 0; i < rec_count; i++){
        int offset = 0;
        for(int j = 0; j < new_attr_count + 1; j++){
            if(j == attribute_position){
                offset = 1;
            } else {
                record[j - offset] = records[i][j];
            }
        }
        insert_record(new_id, record);
    }
    free(record);
    free(data_types);
    free(prim_indices);
    for(int i = 0; i < rec_count; i++){
        free(&(records[i]));
    }
    free(records);
    return 1;
}

int add_attribute_table(catalogs *cat, char *table, char *name, char *type, char *value){
    int table_loc = get_catalog(cat, table);
    if( table_loc == -1 ){
    	fprintf(stderr, "ERROR: '%s' table doesn't exist.\n", table);
        return -1;
    }

    union record_item **records;
    int rec_count = get_records(cat->all_tables[table_loc].id, &records);
    if( rec_count == -1 ){
        return -1;
    }
    int constraints[3] = { 0 };
    int success = add_attribute(&(cat->all_tables[table_loc]),name,type,constraints);
    if( success == -1 ){
        return -1;
    }

    success = drop_table(cat->all_tables[table_loc].id);
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
    cat->all_tables[table_loc].id = new_id;
    int type_int = type_conversion(type);
    union record_item new_record;
    if(value == NULL){
        new_record.d = LONG_MAX;
    } else if(type_int == 0){
        new_record.i = atoi(value);
    } else if(type_int == 1){
    	char *eptr;
        new_record.d = strtod( value,&eptr ); 
    } else if(type_int == 2){
        if(strcasecmp(value, "true") == 0){
            new_record.b = true;
        } else {
            new_record.b = false;
        }
    } else if(type_int == 3){
        memcpy(new_record.c, value, strlen(value));
    } else if(type_int == 4){
        memcpy(new_record.v, value, strlen(value));
    }
    union record_item *record = malloc(sizeof(union record_item) * new_attr_count);
    for(int i = 0; i < rec_count; i++){
        for(int j = 0; j < new_attr_count; j++){
            if(j == new_attr_count - 1){
                record[j] = new_record;
            } else {
                record[j] = records[i][j];
            }
        }
        insert_record(new_id, record);
    }
    free(record);
    free(data_types);
    free(prim_indices);
    for(int i = 0; i < rec_count; i++){
        free(&(records[i]));
    }
    free(records);
    return 1;
}
