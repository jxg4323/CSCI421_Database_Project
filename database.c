#include <string.h>
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
	
	char** temp = (char **)malloc(26*sizeof(char));
	temp[0] = "create\0";
	temp[1] = "table\0";
	temp[2] = "first\0";
	temp[3] = "\0";
	temp[4] = "ID\0";
	temp[5] = "integer\0";
	temp[6] = "notnull\0";
	temp[7] = "primarykey\0";
	temp[8] = "\0";
	temp[9] = "NAME\0";
	temp[10] = "varchar\0";
	temp[11] = "\0";
	temp[12] = "AGE\0";
	temp[13] = "integer\0";
	temp[14] = "notnull\0";
	temp[15] = "\0";
	temp[16] = "FUN\0";
	temp[17] = "boolean\0";
	temp[18] = "\0";
	temp[19] = "primarykey\0";
	temp[20] = "ID\0";
	temp[21] = "\0";
	temp[22] = "unique\0";
	temp[23] = "NAME\0";
	temp[24] = "AGE\0";
	temp[25] = "\0";

	create_table( logs, 26, temp );

	pretty_print_catalogs( logs );

	free( temp );
	return 0;
}


int create_table( catalogs *cat, int token_count, char** tokens ){
    int current = 2;
    bool valid = true;
    // return -1 if name is in use
    if( check_table_name(cat, tokens[current]) ){ return -1; }
    int table_index = cat->table_count;
    manage_catalogs(cat, cat->table_count+1, true);
    init_catalog(&(cat->all_tables[table_index]), 0, 0, 0, 0, 0, tokens[current]);
    current = 4; // skip over empty string
    while(current < token_count && valid ){
        if(strcmp(tokens[current], "primarykey") == 0){
            current++;
            char ** prims = (char **)malloc(sizeof(char *));
            int prim_count = 0;
            while(strcmp(tokens[current], "") != 0){
                prim_count++;
                prims = (char **)realloc(prims, prim_count * sizeof(char *));
                prims[prim_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( prims[prim_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(prims[prim_count-1], tokens[current]);
                current++;
            }
            int added = add_primary_key(&(cat->all_tables[table_index]), prims, prim_count);
            if(added == -1){
                valid = false;
                cat->all_tables[table_index].deleted = true;
            }
            current++;
            for( int i = 0; i < prim_count; i++ ){
            	free( prims[i] );
            }
            free( prims );
        } else if(strcmp(tokens[current], "unique") == 0){
            current++;
            char ** uniques = (char **)malloc(sizeof(char *));
            int uniq_count = 0;
            while(strcmp(tokens[current], "") != 0){
                uniq_count++;
                uniques = (char **)realloc(uniques, uniq_count * sizeof(char *));
                uniques[uniq_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( uniques[uniq_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(uniques[uniq_count-1], tokens[current]);
                current++;
            }
            int added = add_unique_key(&(cat->all_tables[table_index]), uniques, uniq_count);
            if(added == -1){
                valid = false;
                cat->all_tables[table_index].deleted = true;
            }
            current++;
            for( int i = 0; i < uniq_count; i++ ){
            	free( uniques[i] );
            }
            free( uniques );
        } else if(strcmp(tokens[current], "foreignkey") == 0){
            current++;
            int foreign_count = 1;
            char **foreigns = (char **)malloc(sizeof(char *));
            int key_count = 0;
            while(strcmp(tokens[current], "references") != 0){
                foreign_count++;
                key_count++;
                foreigns = (char **)realloc(foreigns, foreign_count * sizeof(char *));
                foreigns[foreign_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( foreigns[foreign_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(foreigns[foreign_count-1], tokens[current]);
                current++;
            }
            current++;
            strcpy(foreigns[0], tokens[current]);
            while(strcmp(tokens[current], "") != 0){
                foreign_count++;
                foreigns = (char **)realloc(foreigns, foreign_count * sizeof(char *));
                foreigns[foreign_count-1] = (char *)malloc( (strlen(tokens[current])+1) * sizeof( char ));
                memset( foreigns[foreign_count-1], '\0', (strlen(tokens[current])+1)*sizeof(char));
                strcpy(foreigns[foreign_count-1], tokens[current]);
                current++;
            }
            int added = add_foreign_data(cat, &(cat->all_tables[table_index]), foreigns, key_count);
            if(added == -1){
                valid = false;
                cat->all_tables[table_index].deleted = true;
            }
            current++;
            for( int i = 0; i < foreign_count; i++ ){
            	free( foreigns[i] );
            }
            free( foreigns );
        } else {
            int name_idx = current++;
            int type_idx = current++;
            int constraints[3] = { 0 };
            while(strcmp(tokens[current], "") != 0 && valid != false){
                if(strcmp(tokens[current], "notnull") == 0){
                    constraints[0] = 1;
                    current++;
                } else if(strcmp(tokens[current], "primarykey") == 0){
                    constraints[1] = 1;
                    current++;
                } else if(strcmp(tokens[current], "unique") == 0){
                    constraints[2] = 1;
                    current++;
                } else {
                    cat->all_tables[table_index].deleted = 1;
                    valid = false;
                }
            }
            int added = -1;
            if(valid != false) {
                added = add_attribute(&(cat->all_tables[table_index]), tokens[name_idx], tokens[type_idx], constraints);
            }
            if(added == -1){
                cat->all_tables[table_index].deleted = 1;
                valid = false;
            }
            current++;
        }
    }
    if( valid == false ){
        return -1;
    } else {
        return 1;
    }
}

int drop_table_ddl( catalogs *cat, char *name ){
    int idx = get_catalog(cat, name);
    if( idx == -1 ){ return idx; }
    int success = drop_table( cat->all_tables[idx].id );
    if( success == -1 ){ return success; }
    cat->all_tables[idx].deleted = true;
    return 1;
}