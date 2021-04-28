#include "ddl_parser.h"
#include "ddlparse.h"

// stores catalog information about tables
static catalogs* logs = NULL;
 /*
  * This function handles the parsing of DDL statments
  *
  * @param statement - the DDL statement to execute
  * @return 0 on sucess; -1 on failure
  */
int parse_ddl_statement( char * statement ){
	// Tokenize the command into sections with ',' then 
	//	spaces & check token for keywords if not a digit
	// Keywords to check for : 'primarykey', 'unique', 'foreignkey', 'references', 'notnull', 'integer', 'double', 'boolean', 'char(x)', 'varchar(x)'
	// 
    char *temp = strdup( statement );
    char *command = strtok( temp,DELIMITER );
    // if logs empty then create new otherwise leave alone
	if( logs == NULL ){
        logs = initialize_catalogs();
        manage_catalogs( logs, 0, false );
    }

    // switch statements checking for what the string starts with
	if ( strcasecmp(command,"create") == 0 ){
        free( temp );
		return parse_create_statement(statement);
	}
	else if ( strcasecmp(command,"drop") == 0 ){
        free( temp );
		return parse_drop_statement(statement);
	}
	else if ( strcasecmp(command,"alter") == 0 ){
        free( temp );
		return parse_alter_statement(statement);
	}
	else{
        fprintf(stderr, "ERROR: command unknown %s\n", command);
        free( temp );
		return -1;
	}
}

/*
 * This function handles the parsing of the create table statements.
 * 
 * Doesn't allow default value tokens.
 * 
 * @param statement - the create table statement to execute
 * @return 0 on sucess; -1 on failure
 */
int parse_create_statement( char * statement ){
 
    // allow for 100 strings as base
    char **data = (char **)malloc(INIT_NUM_TOKENS*sizeof(char *));
  	int i=0, total=1;
    char *temp = strdup( statement );

    char *end_str = temp;
    char *token;
    while ( (token = strtok_r(end_str, DELIMITER, &end_str)) )
    {
        if( strcmp(token, ",") == 0 ){ // if token is just a comma skip it. 
            // add empty token after
            data[i] = (char *)malloc(sizeof(char));
            memset(data[i], '\0', sizeof(char));
        }else{
            // Have comma check, if comma count >1 or comma at the beggining of statment eject w/ error
            int comma_count = char_occur_count( token, ',' );
            if( comma_count > 1 || (token[0] == ',' && strlen(token)>1) ){
                fprintf(stderr, "ERROR: statement provided had either too many commas or a misplaced comma before %s\n", end_str);
                return -1;
            }
            if ( i >= INIT_NUM_TOKENS ){
                data = (char **)realloc( data, (total+1)*sizeof(char *) );
            }
            if( i == 3 ){ 
                // add empty token after table name
                data[i] = (char *)malloc(sizeof(char));
                memset(data[i], '\0', sizeof(char));
                i++, total++;
            }
            int str_len = strlen(token);
            data[i] = (char *)malloc((str_len+1)*sizeof(char)); // TODO: might have to add +1
            memset(data[i], '\0', (str_len+1)*sizeof(char));
            if( token[str_len-1] == ',' ){
                strncpy(data[i], token, str_len-1);
                // add empty token after
                data[i+1] = (char *)malloc(sizeof(char));
                memset(data[i+1], '\0', sizeof(char));
                i++, total++;
            }else{
                strcpy(data[i], token);
            }
        }
        i++, total++;
    }
    // add empty token at end of data
    data[i] = (char *)malloc(sizeof(char));
    memset(data[i], '\0', sizeof(char));
    
    int res = 0;
    if( total < MIN_CREATE_TOKENS ){
        fprintf( stderr, "ERROR: statement '%s' doesn't contain enough information to create a new table.\n", statement);
        res = -1;
    }else{
        res = create_table( logs, total, data );
    }


    for (i = 0; i < total; i++){
        free(data[i]);
    }
    free( data );
    free( temp );
    return (res >= 0 ) ? 0 : -1;
}

/*
 * This function handles the parsing of the drop table statements.
 * *
  * @param statement - the drop table statement to execute
  * @return 0 on sucess; -1 on failure
  */
int parse_drop_statement( char * statement ){

    int i=0;
    char *name = NULL;
    char *temp = strdup( statement );

    char *end_str = temp;
    char *token;
    while ( (token = strtok_r(end_str, DELIMITER, &end_str)) )
    {
        // Have comma check, if comma count >1 or comma at the beggining of statment eject w/ error
        int comma_count = char_occur_count( token, ',' );
        if( comma_count > 0 ){
            fprintf(stderr, "ERROR: drop statment doesn't allow commas, please remove them from your statement:\n %s\n", statement);
            return -1;
        }else if( i >= 3 ){
            fprintf(stderr, "ERROR: To many tokens in statement: %s\n", statement);
            return -1;
        }else if( i ==2 ){
            int str_len = strlen(token);
            name = (char *)malloc((str_len+1)*sizeof(char));
            memset(name, '\0', (str_len+1)*sizeof(char));
            strcpy(name, token);
        }
        i++;
    }

    int drop_result = drop_table_ddl( logs, name );

    free( name );
    free( temp );
    return (drop_result >= 0) ? 0 : -1; 
}

/*
 * This function handles the parsing of the alter table statements.
 * 
 * @param statement - the alter table statement to execute
 * @return 0 on sucess; -1 on failure
 */
int parse_alter_statement( char * statement ){
    // allow for 100 strings as base
    char **data = (char **)malloc(INIT_NUM_TOKENS*sizeof(char *));
    int i=0, total=1;
    char *temp = strdup( statement );

    char *end_str = temp;
    char *token;
    char *delims = DELIMITER;

    while ( (token = strtok_r(end_str, delims, &end_str)) )
    {
        delims = DELIMITER;
        if ( i >= INIT_NUM_TOKENS ){
            data = (char **)realloc( data, total*sizeof(char *) );
        }

        int str_len = strlen(token);
        data[i] = (char *)malloc((str_len+1)*sizeof(char)); 
        strcpy(data[i], token);

        if( strcasecmp(token, "default") == 0 ){
            int quote_count = char_occur_count( end_str, '\"' ); 
            // needs to be 2 --> 1 = error, 0 ignore
            if( quote_count == 2 ){
                int move = 0;
                while( end_str[move] != '\"' && move < strlen(end_str) ){
                    move++;
                }
                end_str = end_str+move;
                delims = "\"";
            }else if( quote_count == 1 ){
                fprintf(stderr, "ERROR: default value has only 1 '\"' in statement '%s'\n", end_str );
                return -1;
            }
        }
        i++, total++;
    }
    // add empty token at end of data
    data[i] = (char *)malloc(sizeof(char));
    memset(data[i], '\0', sizeof(char));
    
    int alt_res = alter_table( logs, total, data );

    for (i = 0; i < total; i++){
        free(data[i]);
    }
    free( data );
    free( temp );

    return (alt_res >= 0) ? 0 : -1;
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
    return 0;
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
 *          if char or varchar then NULL value is saved as "\0" character for size of record
 *          if int or double with NULL are stored as INTMAX
 *          if boolean store false
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
    return 0;
}

/*
 * Read the logs from the disk into the static variable.
 */
int read_logs( char* db_loc ){
    if( logs == NULL ){
        logs = initialize_catalogs();
    }
    return read_catalogs( db_loc, logs );
}

/*
 * Write the logs to the disk from the static logs and free
 * the used memory.
 */
int terminate_logs( char* db_loc ){
    if( logs != NULL ){
        if( write_catalogs( db_loc, logs ) != 1 ){
            fprintf(stderr, "Parser: Couldn't write the table schemas to %s\n", db_loc);
            return -1;
        }
        terminate_catalog( logs );
        return 0;
    }
    return -1;
}

void print_logs( ){
    if( logs != NULL && logs->table_count != 0 ){
        pretty_print_catalogs( logs );
    }else{
        printf("{EMPTY}\n");
    }
}

void print_tokens( char** tokens, int count ){
    printf("{ ");
    for( int i = 0; i<count; i++ ){
        if( i== count-1){
            printf("%s", tokens[i]);
        }else{
            printf("%s, ", tokens[i]);
        }
    }
    printf("}\n");
}

catalogs* get_schemas( ){
    return logs;
}