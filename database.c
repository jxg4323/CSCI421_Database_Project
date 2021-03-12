#include "database1.h"
#include "database.h"

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
	//TODO: multiline queries -> loop user input until semicolon
	char *token = strtok(statement, " ");
	if(strcmp(token, "create") == 0 || strcmp(token, "alter") == 0 || strcmp(token, "drop") == 0){
		token = strtok(NULL, " ");
		if(strcmp(token, "table") == 0){
			parse_ddl_statement(statement); //assume this works
			printf("SUCCESS\n");
			//int result = parse_ddl_statement(input);
			//if(result == 0){
			//	prinf("SUCCESS\n");
			//}else{
			//      printf("ERROR\n");
			//}
			return 0;
		}else{
			printf("ERROR\n");
			//ignore for now. either send to dml or bad query.
		}
	}else{
		printf("ERROR\n");
	}
	return 1;
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
	//TODO: Free up heap memory
	printf("-------TERMINATING DATABASE-----------\n");
	//terminate_database();
	return 0;
}


int * arg_manager(bool restart, char const *argv[], int argc){
	// retreive and verify arguments validity
	//incorrect number of arguments
	//if(argc != 3){
	//	printf("Incorrect number of arguments");
	//}
	//return 0;

	// if this is a new database ignore the args and return null
	// if arguments pass requirements return as array of page size and buffer 
}


int run(char *argv[]){
	// keep complexity out of main loop
	// continuously pass string lines to the ddl parser 
	// ignoring whitespace (/s, /n, /r, /t) and only end lines with ';'

	printf("PATH: %s\n", *argv);
	//TODO: add logic for restart db
	printf("------------CREATING DATABASE----------------\n");
	create_database(argv[1], atoi(argv[2]), atoi(argv[3]), false);

	//loop for user input
	char input[1024];
	int quit = strcmp(input, "quit;");
	while(quit != 0){
		printf("Enter SQL query: ");
		fgets(input, 1024, stdin); //get user input
		quit = strncmp(input, "quit;", 5); //check if quit

		//if not quit, tokenize
		if(quit != 0){
			execute_non_query(input);
		}
	}
	shutdown_database();
	return 0;
}

void usage(bool error){
	if( error ){
		fprintf( stderr, "./database <db_loc> <page_size> <buffer_size>\n" );
	}else{
		fprintf( stdout, "./database <db_loc> <page_size> <buffer_size>\n" );
	}
}

int main(int argc, char const *argv[])
{
	int result = 0;
	int consts[3] = {NOTNULL, PRIMARYKEY, UNIQUE};
	char *db_loc = "/home/stu2/s17/jxg4323/Courses/CSCI421/Project/TestDb/";
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

	statement = "alter table fourth add FURTHER char default \"NULL\";";

	parse_ddl_statement( statement );

	// statement = "drop table fourth;";

	// parse_ddl_statement( statement );

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