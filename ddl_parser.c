#include "ddl_parser.h"
#include "ddlparse.h"

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
    char *command = strsep( &statement,DELIMITER );
	// have switch statements checking for what the string starts with
	// if the string starts with 'c' or 'C', we call the parse_create_statement(statement)
	// else if the string starts with 'd' or 'D', we call the parse_drop_statement(statement)
	// else if the string starts with 'a' or 'A', we call the parse_alter_statement(statement)
	char ch = statement[0];

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
int parse_create_statement( catalogs* cat, char * statement ){
 
    // allow for 100 strings as base
    char **data = (char **)malloc(INIT_NUM_TOKENS*sizeof(char *));
  	int i=0;

    char *end_str;
    char *token = strtok_r(statement, " ", &end_str);

    while (token != NULL)
    {
        if ( i >= INIT_NUM_TOKENS ){
            data = (char **)realloc( data, (i+1)*sizeof(char *) );
        }
        int str_len = strlen(token);
        data[i] = (char *)malloc(str_len*sizeof(char));  // NOTE: might have to add 1 to alloc for '\0'
        strcpy(data[i], token);
        i++;
        token = strtok_r(NULL, " ", &end_str);
    }

    for (i = 0; i < 100; ++i){
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

char *str_replace(char *orig, char *rep, char *with) {
    char *result; // the return string
    char *ins;    // the next insert point
    char *tmp;    // varies
    int len_rep;  // length of rep (the string to remove)
    int len_with; // length of with (the string to replace rep with)
    int len_front; // distance between rep and end of last rep
    int count;    // number of replacements

    // sanity checks and initialization
    if (!orig || !rep)
        return NULL;
    len_rep = strlen(rep);
    if (len_rep == 0)
        return NULL; // empty rep causes infinite loop during count
    if (!with)
        with = "";
    len_with = strlen(with);

    // count the number of replacements needed
    ins = orig;
    for (count = 0; tmp = strstr(ins, rep); ++count) {
        ins = tmp + len_rep;
    }

    tmp = result = malloc(strlen(orig) + (len_with - len_rep) * count + 1);

    if (!result)
        return NULL;

    // first time through the loop, all the variable are set correctly
    // from here on,
    //    tmp points to the end of the result string
    //    ins points to the next occurrence of rep in orig
    //    orig points to the remainder of orig after "end of rep"
    while (count--) {
        ins = strstr(orig, rep);
        len_front = ins - orig;
        tmp = strncpy(tmp, orig, len_front) + len_front;
        tmp = strcpy(tmp, with) + len_with;
        orig += len_front + len_rep; // move to next "end of rep"
    }
    strcpy(tmp, orig);
    return result;
}


int check_statement( char * statement ){
	// check validity of structure of statement
	// Parse string and confirm the layout of Create/Alter/Drop
	// 		match the specified latyout
	// Create
		// 'create table <name>(
		// <a_name> <a_type> <constraint_1> ... <constraint_N>'
		// primarykey( <a_1> ... <a_N>),
		// unique( ( <a_1> ... <a_N>),
		// foreignkey( <a_1> ... <a_N> ) references <r_name>( <r_1> ... <r_N>)
		// );
	// Drop
	// Alter
}
