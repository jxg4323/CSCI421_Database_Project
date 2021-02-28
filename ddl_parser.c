#include "ddl_parser.h"
#include "ddlparse.h"

#include "string.h"

 /*
  * This function handles the parsing of DDL statments
  *
  * @param statement - the DDL statement to execute
  * @return 0 on sucess; -1 on failure
  */
int parse_ddl_statement( char * statement ){
	// All Table names are unique !! --> has to be enforced

	// Tokenize the command into sections with ',' then 
	//	spaces & check token for keywords if not a digit
	// Keywords to check for : 'primarykey', 'unique', 'foreignkey', 'references', 'notnull', 'integer', 'double', 'boolean', 'char(x)', 'varchar(x)'
	// 

	// have switch statements checking for what the string starts with
	// if the string starts with 'c' or 'C', we call the parse_create_statement(statement)
	// else if the string starts with 'd' or 'D', we call the parse_drop_statement(statement)
	// else if the string starts with 'a' or 'A', we call the parse_alter_statement(statement)
	char ch = statement[0];

	if (ch=='C' || ch=='c'){
		return parse_create_statement(statement);
	}
	else if (ch=='D' || ch == 'd'){
		return parse_drop_statement(statement);
	}
	else if (ch=='A' || ch=='a'){
		return parse_alter_statement(statement);
	}
	else{
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
