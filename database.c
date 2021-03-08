#include <string.h>
#include "database1.h"
#include "database.h"
#include "tableschema.h"

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


int create_table( catalogs *cat, int token_count, char** tokens ){
    int current = 2;
    int validity = false;
    validity = check_table_name(cat, tokens[current]);
    if( validity == false ){ return validity; }
    int table_index = cat->table_count;
    manage_catalogs(cat, cat->table_count+1, true);
    init_catalog(&(cat->all_tables[table_index]), 0, 0, 0, 0, 0, tokens[current]);
    current = 3;
    while(current < token_count && validity != false){
        if(strcmp(tokens[current], "primarykey") == 0){
            current++;
            char ** prims = malloc(sizeof(char *));
            int prim_count = 0;
            while(strcmp(tokens[current], ",") != 0){
                prim_count++;
                realloc(prims, prim_count * sizeof(char *));
                strcpy(prims[prim_count-1], tokens[current]);
                current++;
            }
            int added = add_primary_key(&(cat->all_tables[table_index]), prims, prim_count);
            if(added == -1){
                validity = false;
                cat->all_tables[table_index].deleted = 1;
            }
            current++;
        } else if(strcmp(tokens[current], "unique") == 0){
            current++;
            char ** uniques = malloc(sizeof(char *));
            int uniq_count = 0;
            while(strcmp(tokens[current], ",") != 0){
                uniq_count++;
                realloc(uniques, uniq_count * sizeof(char *));
                strcpy(uniques[uniq_count-1], tokens[current]);
                current++;
            }
            int added = add_unique_key(&(cat->all_tables[table_index]), uniques, uniq_count);
            if(added == -1){
                validity = false;
                cat->all_tables[table_index].deleted = 1;
            }
            current++;
        } else if(strcmp(tokens[current], "foreignkey") == 0){
            current++;
            int foreign_count = 1;
            char **foreigns = malloc(sizeof(char *));
            int key_count = 0;
            while(strcmp(tokens[current], "references") != 0){
                foreign_count++;
                key_count++;
                realloc(foreigns, foreign_count * sizeof(char *));
                strcpy(foreigns[foreign_count-1], tokens[current]);
                current++;
            }
            current++;
            strcpy(foreigns[0], tokens[current]);
            while(strcmp(tokens[current], ",") != 0){
                foreign_count++;
                realloc(foreigns, foreign_count * sizeof(char *));
                strcpy(foreigns[foreign_count-1], tokens[current]);
                current++;
            }
            int added = add_foreign_data(cat, &(cat->all_tables[table_index]), foreigns, key_count);
            if(added == -1){
                validity = false;
                cat->all_tables[table_index].deleted = 1;
            }
            current++;
        } else {
            int name_idx = current++;
            int type_idx = current++;
            int constraints[3] = { 0 };
            while(strcmp(tokens[current], ",") != 0 && validity != false){
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
                    validity = false;
                }
            }
            int added = -1;
            if(validity != false) {
                added = add_attribute(&(cat->all_tables[table_index]), tokens[name_idx], tokens[type_idx], constraints);
            }
            if(added == -1){
                cat->all_tables[table_index].deleted = 1;
                validity = false;
            }
            current++;
        }
    }
    if( validity == false ){
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
    cat->all_tables[idx].deleted = 1;
    return 1;
}