#include "dml_parse.h"

 /*
  * This function handles the parsing of DML statments
  * that return nothing, such as insertion, deletion, etc.
  *
  * @param statement - the DML statement to execute
  * @return 0 on sucess; -1 on failure
  */
int parse_dml_statement( char * statement ){
	// Check statement type
	// Call parse_dml_query if query type is SELECT, INSERT, UPDATE, DELETE

}

/*
 * This function handles the parsing of DML statments
 * that return data, such as SELECT queries.
 *
 * @param query - the SQL query to execute
 * @param result - a 2d array of record_item (output variable).
 *                This will be used to output the values of the query.
                  This will be a pointer to the first item in the 2d array.
				  The user of the function will be resposible for freeing.
 * @return the number of tuples in the result, -1 if upon error.
 */
int parse_dml_query(char * query, union record_item *** result){

}

/*
 * This function will be used to execute SQL statement that
 * return data. For instance, SQL SELECT queries.
 *
 * @param query - the SQL query to execute
 * @param result - a 2d array of record_item (output variable).
 *                This will be used to output the values of the query.
                  This will be a pointer to the first item in the 2d array.
				  The user of the function will be resposible for freeing.
 * @return the number of tuples in the result, -1 if upon error.
 */
int execute_query(char * query, union record_item *** result){
	
}

/*
 * This function will execute an insert SQL statment,
 * which returns the number of values inserted into the
 * table. 

 * @param table_name - Name of the table to insert into
 * @param values - 2D array of values separated by a '\0'
 * 					to insert. The values need to match
 *					the tables data types and if not print
 *					an error message.
 * @param result - Output parameter which is a pointer to the
 *				   first record_item in the resulting set of data.
 */
int insert_data( char *table_name, char** values, union record_item ***result ){
	// THROWS custome error 'DMLParserException' if tuple tries to insert with same values as Primary key tuple
	
}

/*
 * This function will execute the delete SQL statement,
 * and will return 0 when the data records have been deleted
 * and -1 if there is an issue.
 *
 * @param table_name - Name of the table to delete from
 * @param condition - 2D array of values which contain the conditional
 *						logic for the where clause. They will be tokens
 *						that have to confirm type and logic with 
 *						'>', '<', '<=', '>=', '=', '!=', 'and', 'or'.
 *						If the condition evaluates to true, then delete,
 *						and if there is no where clause then all tuples
 * 						are deleted.
 * @param result - Output parameter which is a pointer to the
 *				   first record_item in the resulting set of data.
 */
int delete_data( char *table_name, char** condition, union record_item ***result ){

}

/*
 * This function will execute the update SQL statement and
 * return the number of records updated with success and -1
 * if there is an error.
 *
 * @param table_name - name of table to update
 * @param set - the attributes and corresponding values to update
 *				Values can contain math operations, constants, or
 *				other attribute names, so data types have to be 
 * 				confirmed for all scenarios.
 *				Ex: ['bar', 5, '', 'baz', 'baz-1.1']
 * @param condition - the tokens which compose the logic to check
 * 					  which records in the table should be updated.
 *					  If where is NULL then all tuples in the table
 *					  are expected to be updated.
 * @param result - Output parameter which is a pointer to the
 *				   first record_item in the resulting set of data.
 */
int update_data( char *table_name, char** set, char** where, union record_item ***result ){

}