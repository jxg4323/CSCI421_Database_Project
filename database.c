#include <string.h>
#include "database1.h"
#include "database.h"
#include "tableschema.h"
#include <limits.h>

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
            while(strcmp(tokens[current], "") != 0){
                prim_count++;
                realloc(prims, prim_count * sizeof(char *));
                strcpy(prims[prim_count-1], tokens[current]);
                current++;
            }
            int added = add_primary_key(&(cat->all_tables[table_index]), prims, prim_count);
            if(added == -1){
                validity = false;
                cat->all_tables[table_index].deleted = true;
            }
            current++;
        } else if(strcmp(tokens[current], "unique") == 0){
            current++;
            char ** uniques = malloc(sizeof(char *));
            int uniq_count = 0;
            while(strcmp(tokens[current], "") != 0){
                uniq_count++;
                realloc(uniques, uniq_count * sizeof(char *));
                strcpy(uniques[uniq_count-1], tokens[current]);
                current++;
            }
            int added = add_unique_key(&(cat->all_tables[table_index]), uniques, uniq_count);
            if(added == -1){
                validity = false;
                cat->all_tables[table_index].deleted = true;
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
            while(strcmp(tokens[current], "") != 0){
                foreign_count++;
                realloc(foreigns, foreign_count * sizeof(char *));
                strcpy(foreigns[foreign_count-1], tokens[current]);
                current++;
            }
            int added = add_foreign_data(cat, &(cat->all_tables[table_index]), foreigns, key_count);
            if(added == -1){
                validity = false;
                cat->all_tables[table_index].deleted = true;
            }
            current++;
        } else {
            int name_idx = current++;
            int type_idx = current++;
            int constraints[3] = { 0 };
            while(strcmp(tokens[current], "") != 0 && validity != false){
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
                    cat->all_tables[table_index].deleted = true;
                    validity = false;
                }
            }
            int added = -1;
            if(validity != false) {
                added = add_attribute(&(cat->all_tables[table_index]), tokens[name_idx], tokens[type_idx], constraints);
            }
            if(added == -1){
                cat->all_tables[table_index].deleted = true;
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
    cat->all_tables[idx].deleted = true;
    return 1;
}

int alter_table(catalogs *cat, int token_count, char **tokens){
    if(token_count != 5 && token_count != 6 && token_count != 8) {
        fprintf( stderr, "ERROR: wrong amount of tokens for alter table.\n" );
        return -1;
    }
    if(strcmp(tokens[2], "add"){
        if(token_count == 8){
            return add_attribute_table(cat, tokens[2], tokens[4], tokens[5], tokens[7]);
        } else {
            return add_attribute_table(cat, tokens[2], tokens[4], tokens[5], null);
        }
    }

}

int drop_attribute(catalogs *cat, char *table, char *attribute){
    int table_loc = get_catalog(cat, table);
    if( table_loc == -1 ){
        return -1;
    }
    int attribute_loc = get_attr_loc(&(cat->all_tables[table_loc]), attribute);
    if( attribute_loc == -1 ){
        return -1;
    }
    int attribute_position = get_attr_idx(&(cat->all_tables[table_loc]), attribute_loc);
    if( attribute_position == -1 ){
        return -1;
    }
    int success = remove_attribute(cat, &(cat->all_tables[table_loc]), attribute);
    if( success == -1 ){
        return -1;
    }
    union record_item **records;
    int rec_count = get_records(cat->all_tables[table_loc].id, &records);
    if( rec_count == -1 ){
        return -1;
    }
    success = drop_table(cat->all_tables[table_loc].id);
    if( success == -1 ){
        return -1;
    }
    int new_attr_count = get_attribute_count_no_deletes(&(cat->all_tables[table_loc]));
    int *data_types = malloc(sizeof(int) * new_attr_count);
    int position = 0;
    for(int i = 0; i < new_attr_count;;){
        if(cat->all_tables[table_loc].attributes[position].deleted == false){
            data_types[i] = cat->all_tables[table_loc].attributes[position].type;
            i++;
        }
        position++;
    }
    int prim_count = cat->all_tables[table_loc].primary_size;
    int *prim_indices = malloc(sizeof(int) * prim_count);
    for(int i = 0; i < prim_count; i++){
        prim_indices[i] = get_attr_idx(&(cat->all_tables[table_loc]), cat->all_tables[table_loc].primary_tuple[i])
    }
    int new_id = add_table(data_types, prim_indices, new_attr_count, prim_count);
    if( new_id == -1 ){
        return -1;
    }
    cat->all_tables[table_loc].id = new_id;
    union record_item *record = malloc(sizeof(union record_item) * new_attr_count);
    for(int i = 0; i < rec_count; i++){
        int offset = 0;
        for(int j = 0; j < new_attr_count + 1; j++){
            if(j == attribute_position){
                offset = 1;
            } else {
                record[j - offset] = records[i][j];
            }
        }
        insert_record(new_id, record);
    }
    free(record);
    free(data_types);
    free(prim_indices);
    for(int i = 0; i < rec_count; i++){
        free(&(records[i]));
    }
    free(records);
    return 1;
}

int add_attribute_table(catalogs *cat, char *table, char *name, char *type, char *value){
    if(get_attr_loc(cat, name) != -1){
        return -1;
    }
    int table_loc = get_catalog(cat, table);
    if( table_loc == -1 ){
        return -1;
    }
    union record_item **records;
    int rec_count = get_records(cat->all_tables[table_loc].id, &records);
    if( rec_count == -1 ){
        return -1;
    }
    int constraints[3] = { 0 };
    int success = add_attribute(&(cat->all_tables[table_loc]),name,type,constraints);
    if( success == -1 ){
        return -1;
    }
    success = drop_table(cat->all_tables[table_loc].id);
    if( success == -1 ){
        return -1;
    }
    int new_attr_count = get_attribute_count_no_deletes(&(cat->all_tables[table_loc]));
    int *data_types = malloc(sizeof(int) * new_attr_count);
    int position = 0;
    for(int i = 0; i < new_attr_count;;){
        if(cat->all_tables[table_loc].attributes[position].deleted == false){
            data_types[i] = cat->all_tables[table_loc].attributes[position].type;
            i++;
        }
        position++;
    }
    int prim_count = cat->all_tables[table_loc].primary_size;
    int *prim_indices = malloc(sizeof(int) * prim_count);
    for(int i = 0; i < prim_count; i++){
        prim_indices[i] = get_attr_idx(&(cat->all_tables[table_loc]), cat->all_tables[table_loc].primary_tuple[i])
    }
    int new_id = add_table(data_types, prim_indices, new_attr_count, prim_count);
    if( new_id == -1 ){
        return -1;
    }
    cat->all_tables[table_loc].id = new_id;
    int type_int = type_conversion(type);
    union record_item new_record;
    if(value == null){
        new_record.d = DOUBLE_MIN;
    } else if(type_int == 0){
        new_record.i = atoi(value);
    } else if(type_int == 1){
        new_record.d = atod(value);
    } else if(type_int == 2){
        if(strcmp(value, "true") == 0){
            new_record.b = true;
        } else {
            new_record.b = false;
        }
    } else if(type_int == 3){
        memcpy(new_record, value, strlen(value));
    } else if(type_int == 4){
        memcpy(new_record, value, strlen(value));
    }
    union record_item *record = malloc(sizeof(union record_item) * new_attr_count);
    for(int i = 0; i < rec_count; i++){
        for(int j = 0; j < new_attr_count; j++){
            if(j == new_attr_count - 1;){
                record[j] = new_record;
            } else {
                record[j] = records[i][j];
            }
        }
        insert_record(new_id, record);
    }
    free(record);
    free(data_types);
    free(prim_indices);
    for(int i = 0; i < rec_count; i++){
        free(&(records[i]));
    }
    free(records);
    return 1;
}