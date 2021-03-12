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
    char *command = strsep( &temp,DELIMITER );
    // if logs empty then create new otherwise leave alone
	if( logs == NULL ){
        logs = initialize_catalogs();
        manage_catalogs( logs, 0, false );
    }

    // switch statements checking for what the string starts with
	if ( strcasecmp(command,"create") == 0 ){
		return parse_create_statement(statement);
	}
	else if ( strcasecmp(command,"drop") == 0 ){
		return parse_drop_statement(statement);
	}
	else if ( strcasecmp(command,"alter") == 0 ){
		return parse_alter_statement(statement);
	}
	else{
        fprintf(stderr, "ERROR: command unknown %s\n", command);
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
        // Have comma check, if comma count >1 or comma at the beggining of statment eject w/ error
        int comma_count = char_occur_count( token, ',' );
        if( comma_count > 1 || token[0] == ',' ){
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
        data[i] = (char *)malloc(str_len*sizeof(char));
        memset(data[i], '\0', str_len*sizeof(char));
        if( token[str_len-1] == ',' ){
            strncpy(data[i], token, str_len-1);
            // add empty token after
            data[i+1] = (char *)malloc(sizeof(char));
            memset(data[i+1], '\0', sizeof(char));
            i++, total++;
        }else{
            strcpy(data[i], token);
        }
        i++, total++;
    }
    // add empty token at end of data
    data[i] = (char *)malloc(sizeof(char));
    memset(data[i], '\0', sizeof(char));

    create_table( logs, total, data );

    for (i = 0; i < total; i++){
        free(data[i]);
    }
    free( data );

    print_logs();

    return 0;
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

    drop_table_ddl( logs, name );

    print_logs();

    free( name );
    return 0; 
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

    while ( (token = strtok_r(end_str, DELIMITER, &end_str)) )
    {
        if ( i >= INIT_NUM_TOKENS ){
            data = (char **)realloc( data, total*sizeof(char *) );
        }
        // TODO: add empty string check when comma
        int str_len = strlen(token);
        data[i] = (char *)malloc(str_len*sizeof(char));  // NOTE: might have to add 1 to alloc for '\0'
        strcpy(data[i], token);
        i++, total++;
    }
    return 0;
}

void terminate_logs( ){
    terminate_catalog( logs );
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
    if( logs != NULL ){
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