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

int build_set( int token_count, char** tokens, table_catalog* table, set *** set_array){
    int set_count = 0;
    int current_token = 1;
    int attr_type = -1;
    (*set_array) = malloc(sizeof(set *));
    bool isValid = true;
    while( current_token < token_count && isValid == true ){
        set_count++;
        if( set_count > 1){ (*set_array) = realloc(sizeof((set *) * set_count)); }
        (*set_array)[set_count-1] = malloc(sizeof(set));
        int attr_loc = get_attr_loc(table, tokens[current_token]);
        if(attr_loc == -1 ){ isValid = false; } else {
            (*set_array)[set_count-1]->attribute = tokens[current_token];
            current_token++;
            if(strcmp(tokens[current_token],"=") != 0){isValid = false;}
            else {current_token++;}
        }
        if(isValid == true) {
            attr_type = is_attribute(tokens[current_token]);
            if(attr_type == table->attributes[attr_loc].type){
                (*set_array)[set_count-1]->type = attr_type;
                (*set_array)[set_count-1]->new_value = tokens[current_token];
                current_token++;
            } else {
                isValid = false;
            }
        }
        if(isValid == true && strcmp(tokens[current_token],"") != 0){
            if(attr_type != 0 && attr_type != 1){ isValid = false; }
            else{
                (*set_array)[set_count-1]->math_op = true;
                if(strcmp(tokens[current_token],"+") == 0){
                    (*set_array)[set_count-1]->operation = plus;
                } else if(strcmp(tokens[current_token],"-") == 0){
                    (*set_array)[set_count-1]->operation = minus;
                } else if(strcmp(tokens[current_token],"*") == 0){
                    (*set_array)[set_count-1]->operation = multiply;
                } else if(strcmp(tokens[current_token],"/") == 0){
                    (*set_array)[set_count-1]->operation = divide;
                } else { isValid = false; }
            }
            current_token++;
            if(isValid == true){
                int new_loc = get_attr_loc(table, tokens[token_count]);
                if(new_loc != -1){
                    if(table->attributes[new_loc].type == attr_type) {
                        (*set_array)[set_count - 1]->operand = tokens[current_token];
                        (*set_array)[set_count - 1]->operand_type = 0;
                    }
                    else {
                        isValid = false;
                    }
                } else if(attr_type = is_attribute(tokens[current_token])){
                    (*set_array)[set_count-1]->operand = tokens[current_token];
                    (*set_array)[set_count-1]->operand_type = 1;
                }
            }
        }
        if(isValid == true){
            current_token++;
            if(strcmp(tokens[current_token],"") != 0){ isValid = false; }
            current_token++;
        }
    }
    if(isValid == false){
        for(int i = 0; i < set_count; i ++){
            free((*set_array)[i]);
        }
        free(*set_array);
        return 0;
    } else {
        return set_count;
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
    set ** set_part;
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
            if(attr_type != is_attribute(tokens[current_token])){ isValid = false;}
            else {
                switch (type) {
                    case 0: // int
                        item.i = atoi(tokens[current_token]);
                    case 1: // double
                        item.d = atof(tokens[current_token]);
                    case 2: // bool
                        if (strcmp("true", tokens[current_token]) == 0) {
                            item.b = true;
                        } else if (strcmp("false", tokens[current_token]) == 0) {
                            item.b = false;
                        } else {
                            isValid = false;
                        }
                    case 3: // char
                        int len = table->attributes[current_attr].char_length;
                        if (len > strlen(tokens[current_token])) {
                            isValid = false;
                        } else {
                            item.c = tokens[current_token];
                        }
                    case 4: // varchar
                        item.v = tokens[current_token];
                }
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
 *
 */

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
int execute_delete( delete_cmd* delete ){}

// Helper Functions
void set_condition_info( conditional_cmd* cond, int fTid, int oTid int attrType, comparators c, int fAttr, int oAttr, union record_item v1 ){
    cond->first_table_id = fTid;
    cond->other_table_id = oTid;
    cond->attr_type = attrType;
    cond->first_attr = fAttr;
    cond->other_attr = oAttr;
    cond->comparator = c;
    cond->value = v1;
}

/*
 * Parse the given string expected to be only 1 or 2 characters
 * long and return the corresponding comparator type, if none are
 * found return -1.
 */
comparators get_comparator( char* c ){
    comparators res = neq;
    if( strcmp("=",c) == 0 ){
        res = eq;
    }else if( strcmp("<",c) == 0 ){
        res = lt;
    }else if( strcmp(">",c) == 0 ){
        res = gt;
    }else if( strcmp("<=",c) == 0 ){
        res = lte;
    }else if( strcmp(">=",c) == 0 ){
        res = gte;
    }else if( strcmp("!=",c) == 0 ){
        res = neq;
    }else{
        res = -1;
    }
    return res;
}

/*
 * Check if the string's first and last character are quotes,
 * if it is a true/false or numerical value and if anything of
 * those are true then its a value and return false otherwise it is
 * an attribute and return true. Works for multitable attribute names
 * as well.
 */
bool is_attribute( char* check ){
    char first = check[0];
    char last = check[strlen(check)-1];
    return ( is_number(check) || (first == '"' && last == '"') || strcasecmp("true",check) == 0 || strcasecmp("false",check) == 0)
}

/*
 * Get the type for the value based on what is contained.
 * For example if the value contains quotes then it is a
 * string, or if it is true/false then its boolean. If the
 * type is numeric or string then defer to the attribute type
 * for clarification btw. int and double or char and varchar.
 * Return type with success and -1 otherwise.
 */
int get_value_type( char* value, int attr_type ){
    char first = check[0];
    char last = check[strlen(check)-1];
    int val_type = -1;
    if( is_number(value) ){ // numeric value
        val_type = (attr_type == 0) ? 0 : (attr_type == 1) ? 1 : -1;
    }else if( (first == '"' && last == '"') ){ // string value
        val_type = (attr_type == 3) ? 3 : (attr_type == 4) ? 4 : -1;
    }else if( strcasecmp("true",check) == 0 || strcasecmp("false",check) == 0 ){ // boolean
        val_type = 2;
    }
    return val_type;
}

/*
 * Check the value to confirm the type the value is matches
 * the attribute type and if not return -1. Otherwise based
 * on the type fill the corresponding return parameter with
 * the associated value and return 1 for success.
 */
int set_compare_value( char* value, int attr_id, int type, int table_id, union record_item* result, catalogs* schemas ){
    bool null = (strcasecmp(value,"null") == 0);
    int val_type = -1;
    int result = 0;
    char *eptr;
    if( (val_type = get_value_type( value, type )) == -1 ){
        fprintf(stderr, "'%s' type doesn't match the attribute type.\n", value);
        return -1;
    }
    switch( type ){
        case 0: //int
            if( null ){ record->i = INT_MAX; }
            else{ record->i = atoi(value); }
            break;
        case 1: // double
            if( null ){ record->d = INT_MAX; }
            else{ record->d = strtod( value,&eptr ); }
            break;
        case 2: // boolean
            if( null ){ record->b = INT_MAX; }
            if(strcasecmp(value, "true") == 0){
                record->b = true;
            } else {
                record->b = false;
            }
            break;
        case 3: // char
            if( null ){ memset(record->c, '\0', (str_len)*sizeof(char)); }
            else if( str_len <= strlen(value) ){ // char default value is beyond size of char attribute
                fprintf(stderr, "ERROR: '%s' default value is too large for the '%s' attribue.\n", value, attr_name);
                result = -1;
            }
            else{ strcpy(record->c, value); }
            break;
        case 4: // varchar
            if( null ){ memset(record->v, '\0', (str_len)*sizeof(char)); }
            else{ strcpy(record->v, value); }
            break;
        default:
            fprintf(stderr, "ERROR: unknown type for '%s'\n", attr_name);
            result = -1;
            break;
    }
    return result;
}

/*
 * Check to see if the types match for the attributes. If multitable
 * is set to true then use ot_id as the other attributes table.
 * If they match return True o.w False.
 */
bool check_types( int t_id, int first, int other, bool multitable, int ot_id, catalogs* schemas ){
    int first_type = get_attr_type(schemas->all_tables[t_id], first);
    int other_type = -1;
    if( multitable ){
        other_type = get_attr_type(schemas->all_tables[ot_id], other);
    }else{
        other_type = get_attr_type(schemas->all_tables[t_id], other);
    }
    return (first_type == other_type) && (first_type >= 0) && (other_type >= 0);
}


// Where Condition Stack Functions
/*
 * Allocate memory for a new where command node with the
 * default type being 'COND' and NULL for next.
 * @return pointer to new where_cmd node.
 */
where_cmd* new_where_node(){
    where_cmd* node = (where_cmd*)malloc(sizeof(where_cmd));
    node->link = NULL;
    node->cond = NULL;
    return node;
}

/*
 * Create a new conditional command structure and iniatilize the
 * attribute names to '\0' and other integer values to -1.
 *
 * @return: conditional_cmd* to the new structure the user is
            responsible for freeing.
 */
conditional_cmd* new_condition_cmd(){
    conditional_cmd* cond = (conditional_cmd*)malloc(sizeof(conditional_cmd));
    cond->first_table_id = -1;
    cond->other_table_id = -1;
    cond->first_attr = -1;
    cond->other_attr = -1;
    cond->attr_type = -1;
    cond->comparator = eq; // set '=' as base comparator
    return cond;
}

void destroy_where_node(where_cmd* node){
    free( node->cond );
    free( node );
}

/*
 * Add new condition to the the where stack.
 *
 * @parm: tail - last node in the where stack.
 * @parm: type - type of where node, if AND or OR then the condition
                 empty and the type is the AND, OR command. otherwise
                 the condition is stored in the node
 * @parm: condition - condition statement to execute when compared
                      compared to a record.
 */
int push_where_node(where_cmd** top, where_node_type type, conditional_cmd* condition){
    where_cmd* node = new_where_node();
    node->type = type;
    if( type == COND ){ // condition node
        node->cond = condition;
    }else{ // and/or node
        node->cond = NULL;
    }
    node->link = *top;
    *top = node;
    return 0;
}

/*
 * Delete and free top node of the stack. Set the top link to the
 * next where_node.
 *
 * @parm: top - top node on the stack
 * @return: 0 when successfully removed top element, -1 otherwise.
 */
int pop_where_node(where_cmd** top){
    where_cmd* temp = *top;
    *top = (*top)->link;
    if( temp->type == COND ){
        destroy_where_node( temp );
    }else{
        free( temp );
    }
    return 0;
}

int where_is_empty(where_cmd* top){
    return top == NULL;
}

where_cmd* peek(where_cmd* top){
    if( !where_is_empty(top) ){
        return top;
    }else{
        return NULL;
    }
}