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
update_cmd* build_update( int token_count, char** tokens, catalogs* schemas ){}

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the delete query.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'delete', 'from', and 'where'
 *          ex: ['delete', 'from', 'Person', 'where', 'id', '<', '8', '\0']
 * @return new delete command structure that the user is responsible for freeing 
      if no errors were encountered, o.w. return NULL
 */
delete_cmd* build_delete( int token_count, char** tokens, catalogs* schemas ){}

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the insert query as well as a '\0' character to
 * separate different records.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'inset', 'into', and 'values'
 *          ex: ['insert', 'into', 'Person', 'values', '1', '"foo"', 'true', '2.1', 
            '\0', '3', '"baz"', 'true', '4.14', '\0']
 * @return new update command structure that the user is responsible for freeing 
      if no errors were encountered, o.w. return NULL
 */
insert_cmd* build_insert( int token_count, char** tokens, catalogs* schemas ){}

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
where_cmd* build_where( int table_id, bool multi_table, int token_count, char** tokens, catalogs* schemas ){ 
    // TODO: NEED to deal with multiple tables
    where_cmd* top = NULL;
    if( strcasecmp(tokens[0], "where") != 0 ){
      fprintf(stderr, "'%s' doesn't match proper \"where\" conditions\n", tokens[0]);
      return NULL;
    }
    int token_idx = 1;
    // Loop through each token creating conditional_cmds 
    for(token_idx; token_idx < token_count; token_idx+3){
        // If condition then build condition struct
        // else if command such as 'and','or' then store that value
        if( multi_table ){ // the table name should be in the table prefaced by a period

        }
        int first_attr_loc = token_idx+1;
        int comparator_loc = token_idx+2;
        int other_attr_loc = token_idx+3;
        int fid = -1;
        int type = -1;
        int otherId = -1;
        union record_item value.i = INT_MAX;
        comparator c = -1;
        // check attribute type --> I can assume the first_attr_loc is an attr
        if( (fid = get_attr_loc( schemas->all_tables[table_id], tokens[first_attr_loc])) < 0 ){
            fprintf(stderr, "'%s' isn't an attribute in table %s\n", tokens[first_attr_loc], schemas->all_tables[table_id].table_name );
            return NULL;
        }
        type = get_attr_type( schemas->all_tables[table_id],fid );
        // check comparator
        if( (c = get_comparator( tokens[comparator_loc] )) == -1 ){
            fprintf(stderr,"'%s' isn't a known comparator.\n", tokens[comparator_loc]);
            return NULL;
        }
        // if other has quotes or true/false or is digit then its a value otherwise attr
        if( is_attribute( tokens[other_attr_loc] ) ){
            if( (otherId = get_attr_loc( schemas->all_tables[table_id], tokens[first_attr_loc])) < 0 ){
                fprintf(stderr, "'%s' isn't an attribute in table %s\n", tokens[first_attr_loc], schemas->all_tables[table_id].table_name );
                return NULL;
            }
            // type check, same table
            if( !check_types( table_id, fid, otherId, false, -1, schemas ) ){
                fprintf(stderr, "Attribute type mismatch.\n" );
                return NULL;
            }
            conditional_cmd* temp = new_condition_cmd();
            set_condition_info( temp, table_id, table_id, type, c, fid, otherId, value, value);
            push_where_node(&top, COND, temp);
        }else{ // comparing against a value
            // Use get_default_value to fill value --> ignore char length for comparisons
            get_default_value( tokens[other_attr_loc], tokens[first_attr_loc], type, 255, &record_item );
        }
    }
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