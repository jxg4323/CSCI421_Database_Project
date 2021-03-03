#include "database1.h"
#include "database.h"
#include "string.h"
#include "storagemanager.h"
#include "ddl_parser.h"

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
	//TODO: multiline queries -> loop user input until semicolon
	char *token = strtok(statement, " ");
	if(strcmp(token, "create") == 0 || strcmp(token, "alter") == 0 || strcmp(token, "drop") == 0){
		token = strtok(NULL, " ");
		if(strcmp(token, "table") == 0){
			parse_ddl_statement(statement); //assume this works
			printf("SUCCESS\n");
			//int result = parse_ddl_statement(input);
			//if(result == 0){
			//	prinf("SUCCESS\n");
			//}else{
			//      printf("ERROR\n");
			//}
			return 0;
		}else{
			printf("ERROR\n");
			//ignore for now. either send to dml or bad query.
		}
	}else{
		printf("ERROR\n");
	}
	return 1;
}

// This function will be used when executing database queries that return tables of data.
// It is just stubbed out for now.
int execute_query(char * query, union record_item *** result){
	return 0;
}

/*
 * This function is resposible for safely shutdown the database.
 * It will store to hardware any data needed to restart the database.
 * @return 0 on success; -1 on failure.
 */
int shutdown_database(){
	//TODO: Free up heap memory
	printf("-------TERMINATING DATABASE-----------\n");
	//terminate_database();
	return 0;
}


int * arg_manager(bool restart, char const *argv[], int argc){
	// retreive and verify arguments validity
	//incorrect number of arguments
	//if(argc != 3){
	//	printf("Incorrect number of arguments");
	//}
	//return 0;
	
	// if this is a new database ignore the args and return null
	// if arguments pass requirements return as array of page size and buffer 
}


int run(char *argv[]){
	// keep complexity out of main loop
	// continuously pass string lines to the ddl parser 
	// ignoring whitespace (/s, /n, /r, /t) and only end lines with ';'

	printf("PATH: %s\n", *argv);
	//TODO: add logic for restart db
	printf("------------CREATING DATABASE----------------\n");
	create_database(argv[1], atoi(argv[2]), atoi(argv[3]), false); 

	//loop for user input
	char input[1024];
	int quit = strcmp(input, "quit;");
	while(quit != 0){
		printf("Enter SQL query: ");
		fgets(input, 1024, stdin); //get user input
		quit = strncmp(input, "quit;", 5); //check if quit

		//if not quit, tokenize
		if(quit != 0){
			execute_non_query(input);
		}
	}
	shutdown_database();
	return 0;
}

void usage(bool error){
	if( error ){
		fprintf( stderr, "./database <db_loc> <page_size> <buffer_size>\n" );
	}else{
		fprintf( stdout, "./database <db_loc> <page_size> <buffer_size>\n" );
	}
}


int main(int argc, char *argv[])
{
	run(argv);
	return 0;
}
