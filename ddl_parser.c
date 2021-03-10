#include "ddl_parser.h"
#include "ddlparse.h"

// stores catalog information about tables
static catalogs* logs = NULL;
// TODO: add terminate parser
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
 * *
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
        if ( i >= INIT_NUM_TOKENS ){
            data = (char **)realloc( data, total*sizeof(char *) );
        }
        // TODO: add empty string check when comma
        int str_len = strlen(token);
        data[i] = (char *)malloc(str_len*sizeof(char));  // NOTE: might have to add 1 to alloc for '\0'
        strcpy(data[i], token);
        i++, total++;
    }

    create_table( logs, total, data ); // seg fault here

    for (i = 0; i < total; i++){
        free(data[i]);
    }
    free( data );
	
    return 0;
}

/*
 * This function handles the parsing of the drop table statements.
 * *
  * @param statement - the drop table statement to execute
  * @return 0 on sucess; -1 on failure
  */
int parse_drop_statement( char * statement ){

	char* table_name;
	char *p = strrchr(statement, ' ');
	if (p && *(p + 1)){
		table_name = p+1;
	}

	// TO-DO:
	// call the drop table name function here

    return 0; 
}

/*
 * This function handles the parsing of the alter table statements.
 * 
 * @param statement - the alter table statement to execute
 * @return 0 on sucess; -1 on failure
 */
int parse_alter_statement( char * statement ){
	// data is a 2d array of strings
  	char *data[100];
  	int i=0;

    char *end_str;
    char *token = strtok_r(statement, " ", &end_str);

    while (token != NULL)
    {
        printf("a = %s\n", token);
        data[i] = (char *)malloc(20);
        strcpy(data[i], token);
        i++;
        token = strtok_r(NULL, " ", &end_str);
    }

	//TODO:
	// call the alter function with data as the parameter

    return 0;
}

void terminate_logs( ){
    terminate_catalog( logs );
}