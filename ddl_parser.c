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
        create_table( logs, total, data );
    }


    for (i = 0; i < total; i++){
        free(data[i]);
    }
    free( data );
    free( temp );
    return res;
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
    return (drop_result == 1) ? 0 : -1; 
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

    return (alt_res == 1) ? 0 : -1;
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

catalog* get_schemas( ){
    return logs;
}