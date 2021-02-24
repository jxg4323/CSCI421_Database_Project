#include "ddl_parser.h"
#include "ddlparse.h"

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
