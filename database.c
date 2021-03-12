#include <string.h>
#include "database1.h"
#include "database.h"
#include "string.h"
#include "storagemanager.h"
#include "ddl_parser.h"
#include <dirent.h>
#include <errno.h>

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
	char newStr[1024];
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
				printf("ERROR\n");
			}
			return 0;
		}else{
			printf("ERROR: BAD QUERY. \n");
			return -1;
			//bad query.
		}
	}else{
		printf("ERROR\n");
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
	//terminate_logs();
	terminate_database();
	return 0;
}


/*
 * Returns true if the arguments are valid, false if not.
 */
int arg_manager(bool restart, char *argv[], int argc){
	printf("-------------VALIDATING ARGUMENTS-----------\n");
	if(restart){
		//if restart is true
		//doesn't matter if args 3 & 4 are given bc they get ignored
		if(argc >= 2 && argc <= 4){
			printf("ARGUMENTS VALID....\n");
			printf("ATTEMPTING DATABASE RESTART...\n");
		}else{
			printf("INCORRECT NUMBER OF ARGUMENTS FOR RESTART.\n");
			return 0;
		}
	}else{
		if(argc == 4){
			printf("ARGUMENTS VALID....\n");
                        printf("ATTEMPTING NEW DATABASE START...\n");
		}else{
			printf("INCORRECT NUMBER OF ARGUMENTS FOR NEW START.\n");
                        return 0;
		}
	}
	return 1;
}


int run(int argc, char *argv[]){
	//check db path
	DIR* directory = opendir(argv[1]);
	if(directory){
		//if directory exists, call restart
		int results = arg_manager(true, argv, argc);
		if(results){
			printf("------------RESTARTING DATABASE----------------\n");
                        create_database(argv[1], 0, 0, true); //create_db calls restart in storagemanager
		}else{
			printf("TERMINATING PROGRAM.\n");
                        return 0;
		}
	}else if(ENOENT == errno){
		//if directory does not exist, call start
		int results = arg_manager(false, argv, argc);
		if(results){
			printf("------------CREATING DATABASE----------------\n");
                        create_database(argv[1], atoi(argv[2]), atoi(argv[3]), false);
			printf("DATABASE CREATED SUCCESSFULLY.\n");
		}else{
			printf("TERMINATING PROGRAM.\n");
                        return 0;
		}
	}else{
		//some other problem with opendir happened
		printf("PROBLEM OPENING DIRECTORY: %s.\nTERMINATING PROGRAM.\n", argv[1]);
		return 0;
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
		strcpy(tokenString, input);

		//ignore whitespace by tokenizing
		char delims[] = " \n\t\0\r";
		char *token = strtok(tokenString, delims);
        	if((quit = strcmp(token, "quit;")) == 0){
			printf("EXITING PROGRAM....\n");
                        break;
		}else{
			execute_non_query(input);
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
		fprintf( stderr, "./database <db_loc> <page_size> <buffer_size>\n" );
	}else{
		fprintf( stdout, "./database <db_loc> <page_size> <buffer_size>\n" );
	}
}


int main(int argc, char *argv[])
{

	int result = 0;
	int consts[3] = {NOTNULL, PRIMARYKEY, UNIQUE};
//	char *db_loc = "/home/stu2/s17/jxg4323/Courses/CSCI421/Project/TestDb/";
	char *db_loc = argv[1];
	char *prim = (char *)malloc(10*sizeof(char));
	char *sprim = (char *)malloc(10*sizeof(char));
	char **uns = (char **)malloc(2*sizeof(char *));
	char **row = (char **)malloc(3*sizeof(char *));
	row[0] = "second_table\0";
	row[1] = "TEST\0";
	row[2] =  "SECONDS\0";
	prim = "FUN_ID\0";
	uns[0] = "TEST\0";
	uns[1] = "NEXT\0";

	sprim = "SEC_ID\0";
	char for_row[3][15] = {"second_table\0", "TEST\0",  "SECONDS\0"};
	catalogs* logs = initialize_catalogs();
	result = new_catalog( logs, "first_table" );
	result = add_attribute( &(logs->all_tables[0]), "FUN_ID", INTEGER, consts );
	result = add_attribute( &(logs->all_tables[0]), "TEST", CHAR, consts );
	result = add_attribute( &(logs->all_tables[0]), "NEXT", VARCHAR, consts );
	result = add_primary_key( &(logs->all_tables[0]), &prim, 1 );
	result = add_unique_key( &(logs->all_tables[0]), uns, 2 );
	result = new_catalog( logs, "second_table" );
	result = add_attribute( &(logs->all_tables[1]), "SEC_ID", INTEGER, consts );
	result = add_attribute( &(logs->all_tables[1]), "SECONDS", CHAR, consts );
	result = add_attribute( &(logs->all_tables[1]), "FAV_COLOR", VARCHAR, consts );
	result = add_primary_key( &(logs->all_tables[1]), &sprim, 1 );

	// add foreign table after tables defined
	result = add_foreign_data( logs, &(logs->all_tables[0]), row, 1);

	// Before delete
	pretty_print_catalogs( logs );

	printf("--------REMOVE & WRITE CHECKS--------\n");
	result = new_catalog( logs, "first_table" );
	//result = remove_unique_key( &(logs->all_tables[0]), uns, 2 );
	//result = remove_primary_key( &(logs->all_tables[0]) );
	//result = remove_foreign_data( logs, &(logs->all_tables[0]), row, 1 );
	//result = remove_attribute( logs, &(logs->all_tables[0]), "TEST" );

	write_catalogs( db_loc, logs );

	pretty_print_catalogs( logs );

	terminate_catalog( logs );
	printf("--------READ CATALOGS---------\n");
	logs = initialize_catalogs();
	read_catalogs( db_loc, logs );

	pretty_print_catalogs( logs );

	printf("-------NEW FUNCTIONS---------\n");
	printf("%s primary key attribute is %s\n", logs->all_tables[0].table_name, get_attr_name( logs, logs->all_tables[0].table_name, 0));
	int *tmp = get_table_data_types( &(logs->all_tables[0]) );
	printf("first_table data_types: [");
	for( int i = 0; i<logs->all_tables[0].attribute_count; i++){
		printf("%d, ", tmp[i]);
	}
	printf("]\n");
	free( tmp );

	char** temp = (char **)malloc(26*sizeof(char));
	temp[0] = "create\0";
	temp[1] = "table\0";
	temp[2] = "Third\0";
	temp[3] = "\0";
	temp[4] = "ID\0";
	temp[5] = "integer\0";
	temp[6] = "notnull\0";
	temp[7] = "primarykey\0";
	temp[8] = "\0";
	temp[9] = "NAME\0";
	temp[10] = "varchar\0";
	temp[11] = "\0";
	temp[12] = "AGE\0";
	temp[13] = "integer\0";
	temp[14] = "notnull\0";
	temp[15] = "\0";
	temp[16] = "FUN\0";
	temp[17] = "boolean\0";
	temp[18] = "\0";
	temp[19] = "primarykey\0";
	temp[20] = "ID\0";
	temp[21] = "\0";
	temp[22] = "unique\0";
	temp[23] = "NAME\0";
	temp[24] = "AGE\0";
	temp[25] = "\0";

	create_table( logs, 26, temp );
	drop_table_ddl( logs, "Third" );

	free( temp );

	char* statement = "create table fourth( \n ID integer notnull, \n primarykey (ID) \n);";

	parse_ddl_statement( statement );	

	statement = "drop table fourth";

	parse_ddl_statement( statement );

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
