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
	// calls ddl parser or dml
}


/*
 * This function is resposible for safely shutdown the database.
 * It will store to hardware any data needed to restart the database.
 * @return 0 on success; -1 on failure.
 */
int shutdown_database(){
	// Free up heap memory
}


int * arg_manager(bool restart, char const *argv[], int argc){
	// retreive and verify arguments validity
	// if this is a new database ignore the args and return null
	// if arguments pass requirements return as array of page size and buffer 
}


void run(){
	// keep complexity out of main loop
	// infinte loop requesting input from user
	// continuously pass string lines to the ddl parser 
	// ignoring whitespace (/s, /n, /r, /t) and only end lines with ';'
	// pass command to ddl parse and based on parser return print 'SUCCESS' or 'ERROR'
	// 'quit' is exit keyword

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
	

	return 0;
}