#include "dml_parser.h"
#include "dmlparser.h"

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
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the select query.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'select', 'from', 'where', and 'orderby'
 *          ex: ['selct', 'id', 'from', 'Person', 'where', 'id', '<', '8', 'orderby', 'id', '\0']
 * @return new select command structure that the user is responsible for freeing 
      if no errors were encountered, o.w. return NULL
 */
select_cmd* build_select( int token_count, char** tokens, catalogs* schemas ){}

int build_set( int token_count, char** tokens, table_catalog* table, set ** set_array){
    int set_count = 1;
    int current_token = 1;
    while( current_token < token_count ){
        if( strcmp(tokens[current_token], "\0") == 0){
            set_count++;
        }
        current_token++;
    }
}

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the update query.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'update', 'set', and 'where'
 *          ex: ['update', 'Person', 'set', 'AGE', '=', 'AGE', '+', '1', 'where', 'id', '<', '8', '\0']
 * @return new update command structure that the user is responsible for freeing 
      if no errors were encountered, o.w. return NULL
 */
update_cmd* build_update( int token_count, char** tokens, catalogs* schemas ){
    if( strcmp(tokens[0],"delete") != 0 || strcmp(tokens[2],"set") !=0){ return null; }
    int current_token = 3;
    int where_idx = -1;
    while(current_token < token_count && where_idx == -1){
        if(strcmp(tokens[current_token], "where") == 0){
            where_idx = current_token;
        }
        current_token++;
    }
    table_catalog* table = get_catalog_p(schemas, tokens[2]);
    if( table == NULL ){ return NULL; }
    set * set_part;
    int num_attr = build_set(where_idx - 2, tokens + (2* sizeof(char *)), table, &set_part);
    if(num_attr == 0){ return NULL; }
    where_cmd * where = NULL;
    if( where_idx != -1 ){
        where = build_where(table->id, false, token_count-where_idx, tokens + (where_idx * sizeof(char *)));
    }
    char ** attributes_to_update = malloc(sizeof(char *) * num_attr);
    for(int i = 0; i < num_attr; i++){
        attributes_to_update[i] = set_part[i].attribute;
    }
    update_cmd * command = malloc(sizeof(update_cmd));
    command->table_id = table->id;
    command->attributes_to_update = attributes_to_update;
    command->conditions = where;
    command->num_attributes = num_attr;
    command->set_attrs = set_part;
    return command;
}


/*
 * Loop through the tokens provided and confirm their validity
 * create a new delete_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the delete query.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'delete', 'from', and 'where'
 *          ex: ['delete', 'from', 'Person', 'where', 'id', '<', '8', '\0']
 * @return new delete command structure that the user is responsible for freeing 
      if no errors were encountered, o.w. return NULL
 */
delete_cmd* build_delete( int token_count, char** tokens, catalogs* schemas ){
    if(token_count < 4){ return null; }
    if( strcmp(tokens[0],"delete") != 0 || strcmp(tokens[1],"from") !=0
        || strcmp(tokens[3],"where") !=0){ return null; }
    table_catalog* table = get_catalog_p(schemas, tokens[2]);
    if( table == NULL ){ return NULL; }
    where_cmd where = build_where(table->id, true, token_count - 3, tokens + ((sizeof char *) * 3));
    if( where == NULL ){ return NULL}
    delete_cmd * command = malloc(sizeof command);
    command->table_id = table->id;
    command->conditions = where;
    return command;
}


/*
 * Loop through the tokens provided and confirm their validity
 * create a new insert_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the insert query as well as a '\0' character to
 * separate different records.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'insert', 'into', and 'values'
 *          ex: ['insert', 'into', 'Person', 'values', '1', '"foo"', 'true', '2.1', 
            '\0', '3', '"baz"', 'true', '4.14', '\0']
 * @return new update command structure that the user is responsible for freeing 
      if no errors were encountered, o.w. return NULL
 */
insert_cmd* build_insert( int token_count, char** tokens, catalogs* schemas ){
    if(token_count < 6){ return null; }
    if( strcmp(tokens[0],"insert") != 0 || strcmp(tokens[1],"into") !=0
        || strcmp(tokens[3],"values") !=0){ return null; }
    table_catalog* table = get_catalog_p(schemas, tokens[2]);
    if( table == NULL ){ return null; }
    bool isValid = true;
    int current_token = 4;
    int current_record = 0;
    int current_attr = 0;
    int attr_count = table->attribute_count;
    if((token_count - 4) % (attr_count + 1) != 0 ){ return NULL; }
    int num_records = (token_count - 4)/(attr_count + 1);
    union record_item** records = malloc(sizeof(union record_item*) * num_records);
    for( int i = 0; i < num_records; i++){
        records[i] = malloc(sizeof(union record_item) * attr_count);
    }
    while(current_token < token_count && current_record < num_records && isValid == true){
        while(current_attr < attr_count && isValid == true){
            union record_item item;
            int attr_type = table->attributes[current_attr].type;
            switch( type ){
                case 0: // int
                    item.i = atoi(tokens[current_token]);
                case 1: // double
                    item.d = atof(tokens[current_token]);
                case 2: // bool
                    if(strcmp("true", tokens[current_token]) == 0){
                        item.b = true;
                    } else if(strcmp("false", tokens[current_token]) == 0){
                        item.b = false;
                    } else {
                        isValid = false;
                    }
                case 3: // char
                    int len = table->attributes[current_attr].char_length;
                    if( len > strlen(tokens[current_token])){
                        isValid = false;
                    } else {
                        item.c = tokens[current_token];
                    }
                case 4: // varchar
                    item.v = tokens[current_token];
            }
            if(isValid == true){
                records[current_record][current_attr] = item;
            }
            current_token++;
        }
        current_token++;
        current_record++;
    }
    if(isValid == true){
        insert_cmd * command = malloc(sizeof(insert_cmd));
        command->table_id = table->id;
        command->records = records;
        command->num_records = num_records;
        return command;
    } else {
        for( int i = 0; i < num_records; i++){
            free(records[i]);
        }
        free(records);
        return NULL;
    }
}

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the where condition mainting a local pointer to
 * the next conditional statement in the where_cmd.
 * 
 * @parm: table_id - Table Schema Id for table the conditional will be on
 * @parm: multi_table - If true then there are multiple tables involved in 
            the conditional statement and variables will be 
            preceeded by their table name in the token. EX:
            ['foo.id', 'bar.NAME']
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'where', 'and', and 'or'
 *          ex: ['where', 'id', '<', '8', 
            'and', 'name', '=', 'blahblah', 
            'or', 'AGE', '>=', '21', '\0']
 * @return new update command structure that the user is responsible for freeing 
      if no errors were encountered, o.w. return NULL
 */
where_cmd* build_where( int table_id, bool multi_table, int token_count, char** tokens ){ 
  // TODO: NEED to deal with multiple tables
}

/*
 * Take a cartesian product of the tables in the table_ids array
 * allocating memory for the resulting 2d array of records.
 *
 * @parm: num_tables - Number of tables to join
 * @parm: table_ids - Table Schema Ids of the tables
 * @return: 2d array of the resulting joins if no issues o.w. NULL
 */
union record_item** cartesian_product( int num_tables, int* table_ids ){}

/*
 * Execute the where conditionals with the values from provided record.
 * The 'and' conditional takes priority and should be executed first. // "Stack maybe"
 *
 * @return: 0 if record evaluates the where conditionals to true and
      -1 otherwirse.
 */
int check_where_statement( where_cmd* where, union record_item* record, int record_size ){}

/*
 * 
 * @return 2d array result with success and
       NULL otherwise and print error statement
 */
union record_item** execute_select( select_cmd* update ){}

/*
 *
 */
int execute_insert( update_cmd* update ){}

/*
 *
 */
int execute_update( update_cmd* update ){}

/*
 *
 */
int execute_delete( delete_cmd* delete );