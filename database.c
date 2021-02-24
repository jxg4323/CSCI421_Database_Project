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
	// 	
	return 0;
}