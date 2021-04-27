#include "dml_parser.h"
#include "dmlparser.h"

/*
 * Loop through each token of the statement and fill the
 * data array provided which the user is responsible for
 * freeing.
 *
 * @return: number of tokens with sucess and -1 with failure
 */
int parser( char* statement, char** data, bool use_commas ){
    int i=0, total=1;
    char *temp = strdup( statement );

    char *end_str = temp;
    char *token;
    char* delim;
    bool quotes = false;
    if( use_commas ){ delim = " \t\r\n();"; }
    else{ delim = DELIMITER; }

    while ( (token = strtok_r(end_str, delim, &end_str)) )
    {
        quotes = false;
        if( strcmp(delim, "\"") == 0){
            quotes = true;
        }
        // reset delimiter
        if( use_commas ){ delim = " \t\r\n();"; }
        else{ delim = DELIMITER; }

        int str_len = strlen(token);
        char* peek = strdup( end_str );
        char* next = strtok_r(peek, delim, &peek);

        
        // Have comma check, if comma count >1 or comma at the beggining of statment eject w/ error
        int comma_count = char_occur_count( token, ',' );
        if( comma_count > 1 || (token[0] == ',' && strlen(token)>1) ){
            fprintf(stderr, "ERROR: statement provided had either too many commas or a misplaced comma before %s\n", end_str);
            return -1;
        }
        if ( i >= INIT_NUM_TOKENS ){
            data = (char **)realloc( data, (total+1)*sizeof(char *) );
        }
        if( next != NULL && next[0] == '\"' ){
            int quote_count = char_occur_count( end_str, '\"' ); 
            // needs to be 2 --> 1 = error, 0 ignore
            if( quote_count%2 == 0 ){
                int move = 0;
                while( end_str[move] != '\"' && move < strlen(end_str) ){
                    move++;
                }
                end_str = end_str+move;
                delim = "\"";
            }else if( quote_count == 1 ){
                fprintf(stderr, "ERROR: char/varchar value has only 1 '\"' in statement '%s'\n", end_str );
                return -1;
            }
        }

        if(use_commas && strlen(end_str) > 0 && strcmp(token, ",") == 0 && strcmp(data[i-1],"\0") != 0 ){
            // add empty token 
            data[i] = (char *)malloc(sizeof(char));
            memset(data[i], '\0', sizeof(char));
        }else if( use_commas && strlen(end_str) > 0 && token[str_len-1] == ',' && strcmp(data[i-1],"\0") != 0 ){
            data[i] = (char *)malloc((str_len+1)*sizeof(char));
            memset(data[i], '\0', (str_len+1)*sizeof(char));
            strncpy(data[i], token, str_len-1);
            // add empty token after
            data[i+1] = (char *)malloc(sizeof(char));
            memset(data[i+1], '\0', sizeof(char));
            i++, total++;
        }else if(use_commas && strlen(end_str) > 0 && next!= NULL && strcasecmp(next, "where") == 0 ){
            if( quotes ){
                data[i] = (char *)malloc((str_len+3)*sizeof(char));
                memset(data[i], '\0', (str_len+3)*sizeof(char));
                strcat(data[i], "\"");
                strcat(data[i], token);
                strcat(data[i], "\"");
            }else{
                data[i] = (char *)malloc((str_len+1)*sizeof(char));
                memset(data[i], '\0', (str_len+1)*sizeof(char));
                strcpy(data[i], token);
            }
            // add empty token after
            data[i+1] = (char *)malloc(sizeof(char));
            memset(data[i+1], '\0', sizeof(char));
            i++, total++;
        }else{
            if( quotes ){
                data[i] = (char *)malloc((str_len+3)*sizeof(char));
                memset(data[i], '\0', (str_len+3)*sizeof(char));
                strcat(data[i], "\"");
                strcat(data[i], token);
                strcat(data[i], "\"");
            }else{
                data[i] = (char *)malloc((str_len+1)*sizeof(char));
                memset(data[i], '\0', (str_len+1)*sizeof(char));
                strcpy(data[i], token);
            }
        }
    
        i++, total++;
    }
    if( use_commas ){ // add null character at end
        data[i] = (char *)malloc(sizeof(char));
        memset(data[i], '\0', sizeof(char));
        i++;
    }
    free( temp );
    return i;
}

 /*
  * This function handles the parsing of DML statments
  * that return nothing, such as insertion, deletion, etc.
  *
  * @param statement - the DML statement to execute
  * @return 0 on sucess; -1 on failure
  */
int parse_dml_statement( char * statement, catalogs* schemas ){
  	// Check statement type INSERT, UPDATE, DELETE
    char** data = (char **)malloc(INIT_NUM_TOKENS*sizeof(char *));
    char* temp = strdup( statement );
    char* com_type = strtok(temp, DELIMITER);
    int total = 0;
    int res = 0;

    if( strcasecmp(com_type, "insert") == 0 ){
        if( (total = parser( statement, data, false )) < 0 ){ return -1; }
        insert_cmd* insert = build_insert( total, data, schemas );
        // exec
        if( insert != NULL ){
            res = execute_insert( insert,schemas );
            print_table( &(schemas->all_tables[insert->table_id]) );
        }
    }else if( strcasecmp(com_type, "update") == 0 ){
        if( (total = parser( statement, data, true )) < 0 ){ return -1; }
        update_cmd* update = build_update( total, data, schemas );
        // exec
        if( update != NULL ){
            res = execute_update( update, schemas );
            print_table( &(schemas->all_tables[update->table_id]) );
        }
    }else if( strcasecmp(com_type, "delete") == 0 ){
        if( (total = parser( statement, data, false )) < 0 ){ return -1; }
        delete_cmd* delete = build_delete( total, data, schemas );
        // exec
        if( delete != NULL ){
            res = execute_delete( delete, schemas );
            print_table( &(schemas->all_tables[delete->table_id]) );
        }
    }else{
        fprintf(stderr, "ERROR: '%s' isn't a known command\n", data[0]);
        return -1;
    }

    for (int i = 0; i < total; i++){
        free(data[i]);
    }
    free( data );
    free( temp );
    return (res >= 0 ) ? 0 : -1;
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
int parse_dml_query(char * query, union record_item *** result, catalogs* schemas ){
    // Check statement type select
    char** data = (char **)malloc(INIT_NUM_TOKENS*sizeof(char *));
    int total = 0;
    if( (total = parser( query, data, false )) < 0 ){ return -1; }
    union record_item** record;
    int res = 0;
    if( strcasecmp(data[0], "select") == 0 ){
        select_cmd* select = build_select( total, data, schemas );
        // exec
        if( select != NULL ){
            record = execute_select( select, schemas );
        }
        // print record
    }else{
        fprintf(stderr, "ERROR: '%s' isn't a known command\n", data[0]);
        return -1;
    }

    for (int i = 0; i < total; i++){
        free(data[i]);
    }
    free( data );
    return (res >= 0 ) ? 0 : -1;
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
select_cmd* build_select( int token_count, char** tokens, catalogs* schemas ){
    select_cmd* command;
    int from_idx = -1;
    int where_idx = -1;
    int orderby_idx = -1;
    // get indices of separator tokens
    for(int i = 1; i < token_count; i++){
        if(strcasecmp(tokens[i], "from") == 0 && from_idx == -1){ from_idx = i; }
        else if(strcasecmp(tokens[i], "where") == 0 && where_idx == -1){ where_idx = i; }
        else if(strcasecmp(tokens[i], "orderby") == 0 && orderby_idx == -1){ orderby_idx = i; }
    }
    
    if(from_idx == -1 ){ 
        fprintf(stderr, "ERROR: There wasn't any 'from' keyword in your statement.\n");
        return NULL; 
    }
    if( from_idx <= 1 ){
        fprintf(stderr, "ERROR: There wasn't any attributes after the 'select' keyworkd.\n");
        return NULL;
    }
    if( where_idx <= from_idx && where_idx != -1){
        fprintf(stderr, "ERROR: 'where' statement was before 'from'.\n");
        return NULL;
    }
    if( orderby_idx <= from_idx && orderby_idx != -1){
        fprintf(stderr, "ERROR: 'orderby' statement was before 'from'.\n");
        return NULL;
    }else if( orderby_idx <= where_idx && orderby_idx != -1 && where_idx != -1){
        fprintf(stderr, "ERROR: 'orderby' statement was before 'where'.\n");
        return NULL;
    }
    int num_attrs_to_select = from_idx -1;
    // if where_idx == -1 use the token count if orderby_idx == -1 otherwise use orderbyidx 
    int num_tables_to_select = ((where_idx == -1 ? ((orderby_idx > 0) ? orderby_idx : token_count) : where_idx) - from_idx) - 1;
    table_catalog** tables_to_select = malloc(sizeof(table_catalog*) * num_tables_to_select);
    char** tab_names = malloc(sizeof(char*) * num_tables_to_select); // for where build
    command = init_select_cmd( num_attrs_to_select, num_tables_to_select, 0 ); // orderby not added here
    // get the tables being selected from
    for(int i = 0; i < num_tables_to_select; i++){
        tables_to_select[i] = get_catalog_p(schemas, tokens[i+from_idx+1]);
        if(tables_to_select[i] == NULL) {
            fprintf(stderr, "ERROR: '%s' isn't a table in the database.\n", tokens[i+from_idx+1]);
            free(tab_names);
            free(tables_to_select);
            return NULL;
        }
        command->from_tables[i] = tables_to_select[i]->id;
        tab_names[i] = tokens[i+from_idx+1]; // might have to come back and turn into strcpy
    }
    int current_token = 1;
    bool isValid = true;
    int current_attr = 0;
    bool wildcard = false;
    // determine if wildcard is used
    if(from_idx == 2 && strcmp(tokens[1], "*") ==0){
        wildcard = true;
    }
    // loop through attributes to select from the tables
    while(current_token < from_idx && isValid == true && wildcard == false){
        char** names;
        int names_count = get_names_from_token(tokens[current_token], &names);
        int tab_idx, attr_loc;
        if(names_count < 1){ isValid = false; }
        else{
            if(names_count == 1){ // only attribute name is given
                attr_loc = find_attribute_new(tables_to_select, num_tables_to_select, &tab_idx, names[0]);
                if(attr_loc < 0 ){ 
                    fprintf(stderr, "ERROR: '%s' attribute wasn't in any of the from tables.\n", names[0]);
                    isValid = false; 
                } 
                else { 
                    command->attributes_to_select[current_attr] = realloc(command->attributes_to_select[current_attr], (strlen(names[0])+1)*sizeof(char));
                    strcpy(command->attributes_to_select[current_attr], names[0]);
                    command->selected[current_attr] = attr_loc;
                    command->table_match[current_attr] = tab_idx;
                }
            } else { // table and attribute name given
                tab_idx = -1;
                for(int i = 0; i < num_tables_to_select; i++){
                    if(strcmp(names[0], tables_to_select[i]->table_name) == 0 ){
                        tab_idx = i;
                    }
                }
                if(tab_idx < 0){ 
                    fprintf(stderr, "ERROR: '%s' table not in from clause.\n", names[0]);
                    isValid = false; 
                }
                else{
                    attr_loc = get_attr_loc(tables_to_select[tab_idx], names[1]);
                    if(attr_loc < 0){ 
                        fprintf(stderr,"ERROR: '%s' doesn't exists in table '%s'.\n", names[1], names[0]);
                        isValid = false; 
                    } 
                    else { 
                        command->attributes_to_select[current_attr] = realloc(command->attributes_to_select[current_attr], (strlen(names[1])+1)*sizeof(char));
                        strcpy(command->attributes_to_select[current_attr], names[1]);
                        command->selected[current_attr] = attr_loc;
                        command->table_match[current_attr] = tab_idx;
                    }
                }
            }
            current_token++;
            current_attr++;
            for(int i = 0; i < names_count; i++){
                free(names[i]);
            }
            free(names);
        }
    }
    if(isValid == false){
        free(tables_to_select);
        free(tab_names);
        destroy_select( command );
        return NULL;
    }
    int where_token_count; // includes 'where' token
    int orderby_attrs = 0;
    bool where_set = true;
    if(orderby_idx < 0 ){ where_token_count = token_count - where_idx; } 
    else { 
        where_token_count = orderby_idx - where_idx; 
        orderby_attrs = (token_count-orderby_idx)-1;
        add_orderby( command, orderby_attrs );
    }
    if( where_idx > 0 ){
        char** where_toks = tokens+where_idx;
        command->conditions = build_where(tab_names, num_tables_to_select, where_token_count, where_toks, schemas);
    }else{
        where_set = false;
    }
    // build the orderby array
    if(isValid == true && orderby_idx >= 0){
        current_token = orderby_idx+1;
        for(int i = 0; i < orderby_attrs; i++){
            bool occurs = false;
            for(int j = 0; j < num_attrs_to_select; j++){ // TODO: comeback and deal with wildcard
                if(strcmp(tokens[current_token], command->attributes_to_select[j]) == 0){
                    occurs = true;
                    command->orderby[i] = realloc(command->orderby[i], (strlen(tokens[current_token])+1)*sizeof(char));
                    strcpy(command->orderby[i], tokens[current_token]);
                }
            }
            current_token++;
            if(occurs == false){ 
                fprintf(stderr, "ERROR: '%s' attribute wasn't in the select clause.\n", tokens[current_token]);
                isValid = false; 
            }
        }
    }
    if(isValid == false){
        free(tab_names);
        free(tables_to_select);
        destroy_select( command );
    }
    free( tables_to_select );
    free( tab_names );
    if(wildcard == true){
        command->wildcard = true;
    } else {
        command->wildcard = false;
    }
    return command;
}

int build_set( int token_count, char** tokens, table_catalog* table, set ** set_array, catalogs* schemas ){
    int set_count = 0;
    int current_token = 0;
    int prev = 0;
    int attr_type = -1;
    bool isValid = true;
    // loop through each attribute set command  //TODO: change remove isValid and add helper functions to use returns instead
    while( current_token < token_count-1 && isValid == true ){
        set_count++;
        if( set_count > 1){ (*set_array) = realloc((*set_array), sizeof(set) * set_count); } // TODO: might have to change to use manage_sets

        int equated_loc;
        int equals_loc;
        // get null index
        if( current_token+4 < token_count && strcmp(tokens[current_token+4],"\0") == 0 ){ // attr = val/attr
            equated_loc = current_token+1;
            equals_loc = current_token+2;
            int other_loc = current_token+3;
            // confirm equated attr is attribute
            int attr_loc = get_attr_loc(table, tokens[equated_loc]);
            if(attr_loc == -1 ){ 
                fprintf(stderr, "ERROR: '%s' isn't an attribute in table %s.\n", tokens[equated_loc], table->table_name);
                isValid = false; 
            } else {
                (*set_array)[set_count-1].equated_attr = attr_loc;
                if( check_prim_key( table, attr_loc ) ){ // primary key check
                    fprintf(stderr, "ERROR: '%s' is a member of the primary key and can't be altered.\n", tokens[equated_loc]);
                    isValid = false;
                }
                // foreign key check
                int* tmp = (int *)malloc(table->foreign_size*sizeof(int));
                if( check_foreign_relations( table, attr_loc, tmp ) > 0 ){
                    fprintf(stderr, "ERROR: '%s' is apart of a foreign key relation.\n", tokens[equated_loc]);
                    isValid = false;
                }
                free( tmp );
                if(strcmp(tokens[equals_loc],"=") != 0){
                    fprintf(stderr, "ERROR: expected an '=' after '%s' not '%s'.\n", tokens[equated_loc], tokens[equals_loc]);
                    isValid = false;
                }
            }
            int type = get_attr_type( table,(*set_array)[set_count-1].equated_attr );
            (*set_array)[set_count-1].type = type;

            if( isValid ){
                if( is_attribute(tokens[other_loc]) ){ // attribute
                    int left_attr_loc = get_attr_loc(table, tokens[other_loc]);
                    if(left_attr_loc < 0) {
                        fprintf(stderr, "ERROR: '%s' isn't an attribute in table '%s'\n", tokens[other_loc], table->table_name);
                        isValid = false;
                    }
                    if( !check_types( table->id, (*set_array)[set_count-1].equated_attr, left_attr_loc, false, -1, schemas) ){ 
                        fprintf(stderr, "ERROR: '%s' isn't the same type as '%s'.\n", tokens[equated_loc], tokens[other_loc]);
                        isValid = false;
                    }else{
                        (*set_array)[set_count-1].left_attr = left_attr_loc;
                        (*set_array)[set_count-1].left_val_set = false;
                        (*set_array)[set_count-1].use_left_attr = true;
                        (*set_array)[set_count-1].use_right_attr = false;
                        (*set_array)[set_count-1].right_val_set = false;
                    }
                }else{ // set to value
                    union record_item left_item;
                    if( set_compare_value( tokens[other_loc], type, &left_item) >= 0 ){
                        (*set_array)[set_count-1].new_left_value = left_item;
                        (*set_array)[set_count-1].left_val_set = true;
                        (*set_array)[set_count-1].use_left_attr = false;
                        (*set_array)[set_count-1].use_right_attr = false;
                        (*set_array)[set_count-1].right_val_set = false;
                    }else{ // invalid value
                        fprintf(stderr, "ERROR: '%s' is an invalid value.\n", tokens[other_loc]);
                        isValid = false;
                    }
                }
            }
            current_token += 4;
        }else if( current_token+6 < token_count && strcmp(tokens[current_token+6],"\0") == 0){ // attr = val/attr + val/attr
            equated_loc = current_token+1;
            equals_loc = current_token+2;
            int left_loc = current_token+3;
            int op_loc = current_token+4;
            int right_loc = current_token+5;
            union record_item left_val, right_val;
            math_operations operator;
            int attr_loc = get_attr_loc(table, tokens[equated_loc]);
            if(attr_loc == -1 ){ 
                fprintf(stderr, "ERROR: '%s' isn't an attribute in table %s.\n", tokens[equated_loc], table->table_name);
                isValid = false; 
            } else {
                (*set_array)[set_count-1].equated_attr = attr_loc;
                if( check_prim_key( table, attr_loc ) ){
                    fprintf(stderr, "ERROR: '%s' is a member of the primary key and can't be altered.\n", tokens[equated_loc]);
                    isValid = false;
                }
                // foreign key check
                int* tmp = (int *)malloc(table->foreign_size*sizeof(int));
                if( check_foreign_relations( table, attr_loc, tmp ) > 0 ){
                    fprintf(stderr, "ERROR: '%s' is apart of a foreign key relation.\n", tokens[equated_loc]);
                    isValid = false;
                }
                free( tmp );
                if(strcmp(tokens[equals_loc],"=") != 0){
                    fprintf(stderr, "ERROR: expected an '=' after '%s' not '%s'.\n", tokens[equated_loc], tokens[equals_loc]);
                    isValid = false;
                }
            }
            int type = get_attr_type( table,(*set_array)[set_count-1].equated_attr );
            (*set_array)[set_count-1].type = type;
            if( isValid ){
                if( is_attribute(tokens[left_loc]) ){ // equated = left <op> <right attr/val>
                    int left_attr_loc = get_attr_loc(table, tokens[left_loc]);
                    if(left_attr_loc < 0) {
                        fprintf(stderr, "ERROR: '%s' isn't an attribute in table '%s'\n", tokens[left_loc], table->table_name);
                        isValid = false;
                    }
                    if( !check_types( table->id, (*set_array)[set_count-1].equated_attr, left_attr_loc, false, -1, schemas) ){ 
                        fprintf(stderr, "ERROR: '%s' isn't the same type as '%s'.\n", tokens[equated_loc], tokens[left_loc]);
                        isValid = false;
                    }

                    if( is_attribute(tokens[right_loc]) ){ // eqauted = left <op> <right attr>
                        int right_attr_loc;
                        if( (right_attr_loc = get_attr_loc(table, tokens[right_loc])) < 0 ){
                            fprintf(stderr, "ERROR: '%s' isn't an attribute in table '%s'\n", tokens[left_loc], table->table_name);
                            isValid = false;
                        }
                        if( isValid && !check_types( table->id, (*set_array)[set_count-1].equated_attr, right_attr_loc, false, -1, schemas) ){ 
                            fprintf(stderr, "ERROR: '%s' isn't the same type as '%s'.\n", tokens[equated_loc], tokens[right_loc]);
                            isValid = false;
                        }
                        if( (operator = get_op(tokens[op_loc])) >= 0){
                            (*set_array)[set_count-1].left_attr = left_attr_loc;
                            (*set_array)[set_count-1].right_attr = right_attr_loc;
                            (*set_array)[set_count-1].operation = operator;
                            (*set_array)[set_count-1].math_op = true;
                            (*set_array)[set_count-1].left_val_set = false;
                            (*set_array)[set_count-1].use_left_attr = true;
                            (*set_array)[set_count-1].use_right_attr = true;
                            (*set_array)[set_count-1].right_val_set = false;
                        }else{
                            fprintf(stderr, "ERROR: '%s' isn't a valid operator.\n", tokens[op_loc]);
                            isValid = false;
                        }
                    }else{ // equated = left <op> <right val>
                        if( (operator = get_op(tokens[op_loc])) < 0 ){
                            fprintf(stderr, "ERROR: '%s' isn't a valid operator.\n", tokens[op_loc]);
                            isValid = false;
                        }
                        if( isValid && set_compare_value( tokens[right_loc], type, &right_val) >= 0 ){
                            (*set_array)[set_count-1].left_attr = left_attr_loc;
                            (*set_array)[set_count-1].new_right_value = right_val;
                            (*set_array)[set_count-1].operation = operator;
                            (*set_array)[set_count-1].math_op = true;
                            (*set_array)[set_count-1].left_val_set = false;
                            (*set_array)[set_count-1].use_left_attr = true;
                            (*set_array)[set_count-1].use_right_attr = false;
                            (*set_array)[set_count-1].right_val_set = true;
                        }else{
                            isValid = false;
                        }
                    }
                }else{ // equated = value <op> <right attr/val>
                    int type = get_attr_type( table,(*set_array)[set_count-1].equated_attr );
                    if( set_compare_value( tokens[left_loc], type, &left_val) >= 0 ){
                        (*set_array)[set_count-1].new_left_value = left_val;
                        (*set_array)[set_count-1].left_val_set = true;
                        (*set_array)[set_count-1].use_left_attr = false;
                        (*set_array)[set_count-1].use_right_attr = false;
                        (*set_array)[set_count-1].right_val_set = false;
                    }else{ // invalid value
                        fprintf(stderr, "ERROR: '%s' is an invalid value.\n", tokens[left_loc]);
                        isValid = false;
                        break;
                    }
                    if( (operator = get_op( tokens[op_loc])) < 0 ){
                        fprintf(stderr, "ERROR: '%s' is an invalid operator.\n", tokens[op_loc]);
                        isValid = false;
                        break;
                    }
                    if( is_attribute(tokens[right_loc]) ){ // equated = value <op> <right attr>
                        int right_attr_loc;
                        if( (right_attr_loc = get_attr_loc(table, tokens[right_loc])) < 0 ){
                            fprintf(stderr, "ERROR: '%s' isn't an attribute in table '%s'\n", tokens[right_loc], table->table_name);
                            isValid = false;
                        }
                        if( isValid && check_types( table->id, (*set_array)[set_count-1].equated_attr, right_attr_loc, false, -1, schemas) ){
                            (*set_array)[set_count-1].right_attr = right_attr_loc;
                            (*set_array)[set_count-1].new_left_value = left_val;
                            (*set_array)[set_count-1].operation = operator;
                            (*set_array)[set_count-1].math_op = true;
                            (*set_array)[set_count-1].left_val_set = true;
                            (*set_array)[set_count-1].use_left_attr = false;
                            (*set_array)[set_count-1].use_right_attr = true;
                            (*set_array)[set_count-1].right_val_set = false;
                        }else{
                            fprintf(stderr, "ERROR: '%s' isn't the same type as '%s'.\n", tokens[equated_loc], tokens[right_loc]);
                            isValid = false;
                        }
                    }else{ // equated = value <op> <right value>
                        if( set_compare_value(tokens[right_loc], type, &right_val) < 0 ){
                            isValid = false;
                        }else{
                            (*set_array)[set_count-1].new_right_value = right_val;
                            (*set_array)[set_count-1].new_left_value = left_val;
                            (*set_array)[set_count-1].operation = operator;
                            (*set_array)[set_count-1].math_op = true;
                            (*set_array)[set_count-1].left_val_set = true;
                            (*set_array)[set_count-1].use_left_attr = false;
                            (*set_array)[set_count-1].use_right_attr = false;
                            (*set_array)[set_count-1].right_val_set = true;
                        }
                    }
                }
            }
            current_token += 6;
        }else{ // unkonwn set operation
            fprintf(stderr, "ERROR: unkonwn set operation.\n");
            free( *set_array );
            return -1;
        }
    }
    if(isValid == false){
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
    if( strcasecmp(tokens[2],"set") != 0){ 
        fprintf(stderr, "ERROR: update statment expects 'set' after table name not '%s'.\n", tokens[2]);
        return NULL; 
    }
    int current_token = 3;
    int where_idx = -1;
    while(current_token < token_count && where_idx == -1){
        if(strcasecmp(tokens[current_token], "where") == 0){
            where_idx = current_token;
        }
        current_token++;
    }
    table_catalog* table = get_catalog_p(schemas, tokens[1]);
    if( table == NULL ){ 
        fprintf(stderr, "ERROR: '%s' isn't a known table.\n", tokens[1]);
        return NULL; 
    }
    update_cmd* command = init_update_cmd( table->id );
    int set_tok_count = (where_idx > 0) ? where_idx - 2 : token_count - 2;
    command->num_attributes = build_set(set_tok_count, tokens+2, table, &command->set_attrs, schemas);
    if(command->num_attributes == 0){ 
        destroy_update( command );
        return NULL; 
    }
    if( where_idx != -1 ){
        char ** table_name = malloc(sizeof(char*));
        table_name[0] = table->table_name;
        command->conditions = build_where(table_name, 1, token_count-where_idx, tokens+where_idx, schemas);
        free( table_name );
    }
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
    if(token_count < 3){ 
        fprintf(stderr, "ERROR: improper delete command, not enough arguments.\n" );
        return NULL; 
    }
    if( strcasecmp(tokens[1],"from") !=0 ){ 
        fprintf(stderr, "ERROR: expected 'from' after '%s' not '%s'.\n", tokens[0], tokens[1]);
        return NULL; 
    }
    table_catalog* table = get_catalog_p(schemas, tokens[2]);
    if( table == NULL ){ 
        fprintf(stderr, "ERROR: '%s' isn't a known table.\n", tokens[2]);
        return NULL; 
    }
    delete_cmd* command = init_delete_cmd( table->id );
    int where_idx = (token_count>3) ? 3 : -1;
    if( where_idx == 3 && strcasecmp(tokens[3], "where") != 0 ){
        fprintf(stderr, "ERROR: '%s' is an improper 'where' clause.\n", tokens[3]);
        destroy_delete( command );
        return NULL;
    }
    if( where_idx == 3 ){
        char ** table_name = malloc(sizeof(char*));
        table_name[0] = table->table_name;
        command->conditions = build_where(table_name, 1, token_count - 3, tokens+where_idx, schemas);
        free( table_name );
    }
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
    if(token_count < 5){ 
        fprintf(stderr, "ERROR: No values were found in the insert statement.\n");
        return NULL; 
    }
    if( strcasecmp(tokens[1],"into") !=0 ){ 
        fprintf(stderr, "ERROR: expected 'into' after 'insert' not '%s'\n", tokens[1]);
        return NULL; 
    }
    if( strcasecmp(tokens[3],"values") !=0 ){
        fprintf(stderr, "ERROR: expected 'values' after %s not '%s'\n", tokens[2], tokens[3]);
        return NULL; 
    }
    table_catalog* table = get_catalog_p(schemas, tokens[2]);
    if( table == NULL ){ 
        fprintf(stderr, "ERROR: '%s' isn't a known table in the database.\n", tokens[2]);
        return NULL; 
    }
    bool isValid = true;
    int current_token = 4;
    int current_record = 0;
    int current_attr;
    int attr_count = table->attribute_count;
    if((token_count - 4) % (attr_count) != 0 ){ 
        fprintf(stderr, "ERROR: values specified don't contain all values required for table '%s'\n", table->table_name);
        return NULL; 
    }
    int num_records = (token_count - 4)/(attr_count);
    insert_cmd* command = init_insert_cmd( table->id, num_records, attr_count );

    int attr_type;
    // loop through each inserting record and build it
    while(current_token < token_count && current_record < num_records && isValid == true){
        current_attr = 0;
        while(current_attr < attr_count && isValid == true){
            union record_item item;
            attr_type = table->attributes[current_attr].type;
            if( set_compare_value(tokens[current_token], attr_type, &item) < 0 ){
                isValid = false;
                break;
            }
            if(isValid == true){
                // add record to array of records
                command->records[current_record][current_attr] = item;
            }
            current_attr++;
            current_token++;
        }
        //current_token++;
        current_record++;
    }
    if(current_token<token_count-1 || current_record < num_records-1){
        isValid = false;
    }
    if(isValid == true){
        return command;
    } else {
        destory_insert( command );
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
 * @parm: table_names - Array of table names involved in statement
 * @parm: num_tables - Number of tables involved in the statement, the value
                       is assumed to be 1, when value is > 1 then the attribute
                       names have to be checked for uniqueness and if their not
                       unique then the attribute tokens are as follow EX:
                       ['foo.id', 'bar.id']
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'where', 'and', and 'or'
 *          ex: ['where', 'id', '<', '8', 
            'and', 'name', '=', 'blahblah', 
            'or', 'AGE', '>=', '21', '\0']
 * @return new update command structure that the user is responsible for freeing 
           if no errors were encountered, o.w. return NULL
 */
where_cmd* build_where( char** table_names, int num_tables, int token_count, char** tokens, catalogs* schemas ){

    int table_id = -1;
    int* all_tables = (int*)malloc(num_tables*sizeof(int));

    // get list of tables if there are more than one
    for( int i = 0; i<num_tables; i++){
        if( (all_tables[i] = get_catalog( schemas, table_names[i] )) == -1 ){
            fprintf(stderr,"'%s' doesn't exist in this database.\n", table_names[i]);
            free( all_tables );
            return NULL;
        }
    }
    table_id = all_tables[0];

    where_cmd* top = NULL;
    if( strcasecmp(tokens[0], "where") != 0 ){
      fprintf(stderr, "'%s' doesn't match proper \"where\" conditions\n", tokens[0]);
      return NULL;
    }
    int token_idx = 1;
    // Loop through each token creating conditional_cmds
    for(token_idx; token_idx < token_count; token_idx+=3){
        int first_attr_loc = token_idx;
        int comparator_loc = token_idx+1;
        int other_attr_loc = token_idx+2;
        int fid = -1;
        int type = -1;
        int otherId = -1;
        int first_tab_id = -1;
        int other_tab_id = -1;
        union record_item value;
        value.i = INT_MAX;
        comparators c = -1;

        char* first_attr_name = get_attr_name_from_token( tokens[first_attr_loc] );
        // check attribute type --> I can assume the first_attr_loc is an attr
        if( (fid = find_attribute( all_tables, num_tables, &first_tab_id, first_attr_name, schemas)) < 0 ){
            destroy_where_stack( &top );
            return NULL;
        }

        type = get_attr_type( &(schemas->all_tables[first_tab_id]),fid );
        // check comparator
        if( (c = get_comparator( tokens[comparator_loc] )) == -1 ){
            fprintf(stderr,"'%s' isn't a known comparator.\n", tokens[comparator_loc]);
            destroy_where_stack( &top );
            return NULL;
        }
        // if other has quotes or true/false or is digit then its a value otherwise attr
        if( is_attribute( tokens[other_attr_loc] ) ){
            char* other_attr_name = get_attr_name_from_token( tokens[other_attr_loc] );
            if( (otherId = find_attribute( all_tables, num_tables, &other_tab_id, other_attr_name, schemas)) < 0 ){
                destroy_where_stack( &top );
                return NULL;
            }
            // type check, same table
            if( !check_types( first_tab_id, fid, otherId, true, other_tab_id, schemas ) ){
                fprintf(stderr, "Attribute type mismatch.\n" );
                destroy_where_stack( &top );
                return NULL;
            }
            conditional_cmd* temp = new_condition_cmd();
            set_condition_info( temp, first_tab_id, other_tab_id, type, c, fid, otherId, value );
            push_where_node(&top, COND, temp);
            free( other_attr_name );
        }else{ // comparing against a value
            if( set_compare_value( tokens[other_attr_loc], type, &value ) < 0 ){
                destroy_where_stack( &top );
                return NULL;
            }
            conditional_cmd* temp = new_condition_cmd();
            set_condition_info( temp, first_tab_id, -1, type, c, fid, otherId, value );
            push_where_node(&top, COND, temp);
        }

        // next token can be either AND, OR, '\0'
        if( token_idx+3 < token_count && strcasecmp(tokens[token_idx+3], "and") == 0 ){ // add AND node
            push_where_node(&top, AND, NULL);
        }else if( token_idx+3 < token_count && strcasecmp(tokens[token_idx+3], "or") == 0 ){ // add OR node
            push_where_node(&top, OR, NULL);
        }else if( token_idx+3 < token_count && strcasecmp(tokens[token_idx+3], "\0") == 0 ){ break; }
        else if ( token_idx+3 < token_count ){
            fprintf(stderr, "'%s' is an unkonwn relational operator.\n", tokens[token_idx+3]);
            destroy_where_stack( &top );
            return NULL;
        }
        // increment to start of next conditional
        token_idx++;
        free( first_attr_name );
    }

    // frees
    free( all_tables );
    return top;
}

/*
 * Take a cartesian product of the tables in the table_ids array
 * allocating memory for the resulting 2d array of records.
 *
 * @parm: num_tables - Number of tables to join
 * @parm: table_ids - Table Schema Ids of the tables
 * @parm: schemas - Array of table cataglogs
 * @return: 2d array of the resulting joins if no issues o.w. NULL
 */
union record_item** cartesian_product( int num_tables, int* table_ids, int *total_records, catalogs* schemas ){
    union record_tem** cart;
    int cart_size = 0;
    if( num_tables == 1 ){
        *total_records = get_records( schemas->all_tables[table_ids[0]].storage_manager_loc, &cart );
        return cart;
    }
    for( int i = 0; i < num_tables; i++ ){
        cart_size += schemas->all_tables[table_ids[i]].attribute_count;
    }
    cart = (union record_item**)malloc(sizeof(union record_item*));
    for( int i = 0; i<num_tables; i++){
        table_catalog* table = &(schemas->all_tables[table_ids[i]]);

    }
}

/*
 * Execute the where conditionals with the values from provided record.
 * The 'and' conditional takes priority and should be executed first. // "Stack maybe"
 *
 * @parm: record - Array of records involved in the query, if there are multiple
                   tables involved then the record contains cartesian product of
                   the tables so it will be all records from table A + records from
                   table B.
 * @parm: record_size - Number of attributes in the record
 * @parm: table_ids - Array of table schema ids in the order of the records.
 * @parm: num_tables - Number of tables in query.
 * @return: 0 if record evaluates the where conditionals to true and
      -1 otherwirse.
 */
int check_where_statement( where_cmd* where, union record_item* record, int record_size, catalogs* schemas ){
    if( where == NULL ){ // if no where conditions then all records pass.
        return 0;
    }
    // What to do about multiple tables??
    // Assume only one table involved in query
    int table_id = where->cond->first_table_id;
    where_cmd* temp = where;
    bool result_val = false;  // assume record doesn't meet condition
    bool last_val = false;
    while( !where_is_empty(temp) ){
        switch( temp->type ){
            case AND:
                last_val = result_val;
                // move to next condition
                temp = temp->link;
                eval_condition( temp->cond, record, schemas );
                result_val = last_val && temp->cond->result_value;
                break;
            case OR:
                last_val = result_val;
                // move to next condition
                temp = temp->link;
                eval_condition( temp->cond, record, schemas );
                result_val = last_val || temp->cond->result_value;
                break;
            case COND:
                eval_condition( temp->cond, record, schemas );
                result_val = temp->cond->result_value;
                break;
        }
        // get next node
        temp = temp->link;
    }
    return (result_val) ? 0 : -1;
}

/*
 * 
 * @return 2d array result with success and
       NULL otherwise and print error statement
 */
union record_item** execute_select( select_cmd* select, catalogs* shcemas ){
    union record_item** result_table;


}

/*
 * Insert the records in the command structure into the
 * designated table and if any errors occur on insert then
 * back track the insert by removing all of the records that
 * were inserted before the error.
 * 
 * @return 0 with success and -1 otherwise,
 */
int execute_insert( insert_cmd* insert, catalogs* schemas ){
    int result = 0;
    for( int i = 0; i<insert->num_records; i++ ){
        result = insert_record( insert->table_id, insert->records[i] );
        if( result < 0 ){ 
            backtrack_insert( i,insert );
            break; 
        }
    }
    return result;
}

/* 
 * Execute the update command based on the structure given,
 * loop through each record in the table to be updated and 
 * execute the set operations on the records that match the
 * where conditions.
 *
 * @return: 0 with success and -1 for failure.
 */
int execute_update( update_cmd* update, catalogs* schemas ){
    int result = 0;
    int record_size = schemas->all_tables[update->table_id].attribute_count;
    int num_records = 0;
    union record_item** all_records;
    table_catalog* tcat = &(schemas->all_tables[update->table_id]);
    if( (num_records = get_records( update->table_id,&all_records )) < 0 ){ return -1; }
    for( int i = 0; i<num_records; i++ ){
        union record_item* record = all_records[i];
        // TODO: issue when altering a primary/foreign key
        if( check_where_statement( update->conditions, record, record_size, schemas ) == 0 ){
            // loop through each set operation and alter the record
            for(int j = 0; j<update->num_attributes; j++){
                set* setter = &(update->set_attrs[j]);
                if( setter->math_op ){
                    union record_item n_val;
                    int lsize = 0, rsize = 0;
                    if( setter->use_left_attr && setter->use_right_attr ){  // both variables used are attributes
                        lsize = (tcat->attributes[setter->left_attr].char_length > 0) ? tcat->attributes[setter->left_attr].char_length : 0;
                        rsize = (tcat->attributes[setter->right_attr].char_length > 0) ? tcat->attributes[setter->right_attr].char_length : 0;
                        if( exec_math_op( &n_val, record[setter->left_attr], record[setter->right_attr], setter->operation, setter->type, lsize, rsize ) < 0 ){ return -1; }
                        
                    }else if( setter->use_left_attr && setter->right_val_set ){ // attribute + value
                        lsize = (tcat->attributes[setter->left_attr].char_length > 0) ? tcat->attributes[setter->left_attr].char_length : 0;
                        rsize = strlen(setter->new_right_value.c);
                        if( exec_math_op( &n_val, record[setter->left_attr], setter->new_right_value, setter->operation, setter->type, lsize, rsize ) < 0 ){ return -1; }
                        
                    }else if( setter->left_val_set && setter->use_right_attr ){ // value + attribute
                        lsize = strlen(setter->new_left_value.c);
                        rsize = (tcat->attributes[setter->right_attr].char_length > 0) ? tcat->attributes[setter->right_attr].char_length : 0;
                        if( exec_math_op( &n_val, setter->new_left_value, record[setter->right_attr], setter->operation, setter->type, lsize, rsize) < 0){ return -1; }
                        
                    }else if( setter->left_val_set && setter->right_val_set ){  // value + value
                        lsize = strlen(setter->new_left_value.c);
                        rsize = strlen(setter->new_right_value.c);
                        if( exec_math_op( &n_val, setter->new_left_value, setter->new_right_value, setter->operation, setter->type, lsize, rsize) < 0){ return -1; }
                    }
                    // change record
                    record[setter->equated_attr] = n_val;
                }else{  // NO math operation involved in set
                    if( setter->use_left_attr ){ // only left attribute
                        change_record_val( &(record[setter->equated_attr]), record[setter->left_attr], setter->type );
                    }else if( setter->use_right_attr ){ // only right attribute
                        change_record_val( &(record[setter->equated_attr]),record[setter->right_attr], setter->type );
                    }else if( setter->left_val_set ){ // only left value
                        change_record_val( &(record[setter->equated_attr]), setter->new_left_value, setter->type );
                    }else if( setter->right_val_set ){ // only right value
                        change_record_val( &(record[setter->equated_attr]), setter->new_right_value, setter->type );
                    }
                }
            }
            // update the altered record
            if( (result = update_record(tcat->storage_manager_loc,record)) < 0 ){ 
                result = -1;
                break; 
            }  
        }
    }
    for( int i = 0; i<num_records; i++){
        free( all_records[i] );
    }
    free( all_records );
    return result;
}

/*
 * Get all the records for the table in the delete command
 * structure and if the record matches the where clause then
 * remove the record.
 *
 * @return 0 with success and -1 otherwise.
 */
int execute_delete( delete_cmd* delete, catalogs* schemas ){
    int result = 0;
    int record_size = schemas->all_tables[delete->table_id].attribute_count;
    int num_records = 0;
    union record_item** all_records;
    if( (num_records = get_records( delete->table_id,&all_records )) < 0 ){ return -1; }
    for( int i = 0; i<num_records; i++ ){
        if( check_where_statement( delete->conditions, all_records[i], record_size, schemas ) == 0 ){
            result = remove_record( delete->table_id,all_records[i] );
        }
        if( result < 0 ){ break; }
    }
    for( int i = 0; i<num_records; i++){
        free( all_records[i] );
    }
    free( all_records );
    return result;
}

// Helper Functions
void set_condition_info( conditional_cmd* cond, int fTid, int oTid, int attrType, comparators c, int fAttr, int oAttr, union record_item v1 ){
    cond->first_table_id = fTid;
    cond->other_table_id = oTid;
    cond->attr_type = attrType;
    cond->first_attr = fAttr;
    cond->other_attr = oAttr;
    cond->comparator = c;
    cond->value = v1;
}

/*
 * Remove the records that were inserted in the insert
 * command structure before an error.
 */
void backtrack_insert( int rec_count, insert_cmd* insert ){
    for( int i = 0; i < rec_count; i++ ){
        remove_record( insert->table_id, insert->records[i] );
    }
}

/*
 * Change the old value to the new value based on the type.
 * @return 0 with success and -1 with failure.
 */
int change_record_val( union record_item* old_val, union record_item new_val, int type ){
    switch( type ){
        case 0: // int
            old_val->i = new_val.i;
            break;
        case 1: // double
            old_val->d = new_val.d;
            break;
        case 2: // boolean
            old_val->b = new_val.b;
            break;
        case 3: // char
            strcpy(old_val->c, new_val.c);
            break;
        case 4: // varchar
            strcpy(old_val->v, new_val.v);
            break;
    }
    return 0;
}

/*
 * Execute the provided math operation and return the result.
 * If the types are char/varchar then the sizes are used to 
 * create a new string.
 *
 * @return 0 with success and -1 o.w.
 */
int exec_math_op( union record_item* result, union record_item left, union record_item right, math_operations op, int type, int lsize, int rsize ){
    char* concat = (char *)malloc((lsize+rsize+1)*sizeof(char));
    memset(concat, '\0', (lsize+rsize+1)*sizeof(char));
    switch( op ){
        case plus:
            switch( type ){
                case 0: // int
                    result->i = left.i + right.i;
                    break;
                case 1: // double
                    result->d = left.d + right.d;
                    break;
                case 2: // boolean
                    result->b = left.b + right.b;
                    break;
                case 3: // char
                    strncat(concat, left.c, lsize);
                    strncat(concat, right.c, rsize);
                    strcpy(result->c, concat);
                    break;
                case 4: // varchar
                    strncat(concat, left.v, lsize);
                    strncat(concat, right.v, rsize);
                    strcpy(result->v, concat);
                    break;
            }
            break;
        case minus:
            // char and var char types not allowed for this operation
            switch( type ){
                case 0: // int
                    result->i = left.i - right.i;
                    break;
                case 1: // double
                    result->d = left.d - right.d;
                    break;
                case 2: // boolean
                    result->b = left.b - right.b;
                    break;
            }
            break;
        case multiply:
            // char and var char types not allowed for this operation
            switch( type ){
                case 0: // int
                    result->i = left.i * right.i;
                    break;
                case 1: // double
                    result->d = (double)left.d * (double)right.d;
                    break;
            }
            break;
        case divide:
            // char and var char types not allowed for this operation
            switch( type ){
                case 0: // int
                    if( right.i == 0 ){
                        fprintf(stderr, "Floating point exception. Attempted division by zero\n");
                        return -1;
                    }
                    result->i = left.i / right.i;
                    break;
                case 1: // double
                    if( right.d == 0 ){
                        fprintf(stderr, "Floating point exception. Attempted division by zero\n");
                        return -1;
                    }
                    result->d = (double)left.d / (double)right.d;
                    break;
            }
            break;
    }
    free( concat );
    return 0;
}

/*
 * Evaluate conditional_cmd based on a single record, store the
 * result of the condition in the command structure's result_value.
 */
int eval_condition( conditional_cmd* cond, union record_item* record, catalogs* schemas ){
    int size = -1;
    if( cond->attr_type == 3 || cond->attr_type == 4 ){
        size = schemas->all_tables[cond->first_table_id].attributes[cond->first_attr].char_length;
    }
    if( cond->other_attr == -1 ){ // then evaluate with stored cond->value
        cond->result_value = compare_condition( cond->comparator, cond->attr_type, size, record[cond->first_attr], cond->value);
    }else{  // evaluate based on other attribute value in record
        cond->result_value = compare_condition( cond->comparator, cond->attr_type, size, record[cond->first_attr], record[cond->other_attr]);
    }
    return 0;
}

/*
 * Based on the 6 possible comparator types compare the provided record values
 * in the manner that follows:
 *
 *      "left" <-> "comparator" <-> "right"
 * @parm: comp - comparator
 * @parm: type - type of attribute to compare
 * @parm: size - size of char/varchar types if > 0 o.w. -1
 * @parm: left - left value of expression
 * @parm: right - right value of expression
 * @return: true if the expressions evaluates to true and
            false if it evaluates to false.
 */
bool compare_condition( comparators comp, int type, int size, union record_item left, union record_item right ){
    bool result = false;
    switch( comp ){
        case eq:
            switch( type ){
                case 0: // int
                    result = (left.i == right.i);
                    break;
                case 1: // double
                    result = (left.d == right.d);
                    break;
                case 2: // boolean
                    result = (left.b == right.b);
                    break;
                case 3: // char
                    result = (strncasecmp(left.c, right.c, strlen(right.c)) == 0);
                    break;
                case 4: // varchar
                    result = (strncasecmp(left.v, right.v, strlen(right.v)) == 0);
                    break;
            }
            break;
        case lt:
            switch( type ){
                case 0: // int
                    result = (left.i < right.i);
                    break;
                case 1: // double
                    result = (left.d < right.d);
                    break;
                case 2: // boolean
                    result = (left.b < right.b);
                    break;
                case 3: // char
                    result = (strncasecmp(left.c, right.c, size) < 0);
                    break;
                case 4: // varchar
                    result = (strncasecmp(left.v, right.v, size) < 0);
                    break;
            }
            break;
        case gt:
            switch( type ){
                case 0: // int
                    result = (left.i > right.i);
                    break;
                case 1: // double
                    result = (left.d > right.d);
                    break;
                case 2: // boolean
                    result = (left.b > right.b);
                    break;
                case 3: // char
                    result = (strncasecmp(left.c, right.c, size) > 0);
                    break;
                case 4: // varchar
                    result = (strncasecmp(left.v, right.v, size) > 0);
                    break;
            }
            break;
        case lte:
            switch( type ){
                case 0: // int
                    result = (left.i <= right.i);
                    break;
                case 1: // double
                    result = (left.d <= right.d);
                    break;
                case 2: // boolean
                    result = (left.b <= right.b);
                    break;
                case 3: // char
                    result = (strncasecmp(left.c, right.c, strlen(right.c)) <= 0);
                    break;
                case 4: // varchar
                    result = (strncasecmp(left.v, right.v, strlen(right.v)) <= 0);
                    break;
            }
            break;
        case gte:
            switch( type ){
                case 0: // int
                    result = (left.i >= right.i);
                    break;
                case 1: // double
                    result = (left.d >= right.d);
                    break;
                case 2: // boolean
                    result = (left.b >= right.b);
                    break;
                case 3: // char
                    result = (strncasecmp(left.c, right.c, strlen(right.c)) >= 0);
                    break;
                case 4: // varchar
                    result = (strncasecmp(left.v, right.v, strlen(right.v)) >= 0);
                    break;
            }
            break;
        case neq:
            switch( type ){
                case 0: // int
                    result = (left.i != right.i);
                    break;
                case 1: // double
                    result = (left.d != right.d);
                    break;
                case 2: // boolean
                    result = (left.b != right.b);
                    break;
                case 3: // char
                    result = (strncasecmp(left.c, right.c, strlen(right.c)) != 0);
                    break;
                case 4: // varchar
                    result = (strncasecmp(left.v, right.v, strlen(right.v)) != 0);
                    break;
            }
            break;
    }
    return result;
}

/*
 * Tokenize the attribute token based on '.' and assume
 * the attribute name is in the last token.
 * @return the name of the attribute in the token which the user
           is responsible for freeing.
 */
char* get_attr_name_from_token( char* attr_token ){
    int period_count = char_occur_count( attr_token, '.' );
    char* temp = strdup( attr_token );
    char* hold;
    char* attr_name = NULL;
    switch( period_count ){
        case 0:
            hold = strtok( temp, "." );
            attr_name = (char *)malloc(strlen(hold)*sizeof(char));
            strcpy( attr_name, hold );
            break;
        case 1:
            strtok( temp, "." );
            hold = strtok( NULL, "." );
            attr_name = (char *)malloc(strlen(hold)*sizeof(char));
            strcpy( attr_name, hold );
            break;
        default:
            attr_name = NULL;
            fprintf(stderr, "'%s' isn't a valid attribute token.\n", attr_token );
    }
    free( temp );
    return attr_name;
}

/*
 * Tokenize the attribute token based on '.' and assume
 * the attribute name is in the last token.
 * @return the name of the attribute in the token which the user
           is responsible for freeing.
 */
int get_names_from_token( char* attr_token, char*** return_array ){
    int period_count = char_occur_count( attr_token, '.' );
    char* temp = strdup( attr_token );
    char* hold1;
    char* hold2;
    int array_size = -1;
    switch( period_count ){
        case 0:
            hold1 = strtok( temp, "." );
            (*return_array) = malloc(sizeof(char*));
            (*return_array)[0] = (char *)malloc(strlen(hold1)*sizeof(char));
            strcpy( (*return_array)[0], hold1 );
            array_size = 1;
            break;
        case 1:
            hold1 = strtok( temp, "." );
            hold2 = strtok( NULL, "." );
            (*return_array) = malloc(sizeof(char*) * 2);
            (*return_array)[0] = malloc(strlen(hold1)*sizeof(char));
            (*return_array)[1] = malloc(strlen(hold2)*sizeof(char));
            strcpy( (*return_array)[0], hold1 );
            strcpy( (*return_array)[1], hold2 );
            array_size = 2;
            break;
        default:
            return_array = NULL;
            fprintf(stderr, "'%s' isn't a valid attribute token.\n", attr_token );
    }
    free( temp );
    return array_size;
}

/*
 *
 * Search through list of table ids for the provided attribute.
 *
 * @parm: num_tables - Number of tables in the list
 * @parm: par_tab_id - return parameter for table schema id where
                       attribute was found
 * @parm: attr_name - attribute name to search for
 * @parm: schemas - List of Table Schema catalogs
 * @return: attribute location if found in the list of table schema ids
            if attribute not found then return -1 and print error.
            if attribute found in multiple tables return -1 and print error.
 */
int find_attribute( int* table_ids, int num_tables, int* par_tab_id, char* attr_name, catalogs* schemas ){
    int found = -1;
    int find_count = 0;
    for( int i = 0; i<num_tables; i++ ){
        int temp = -1;
        table_catalog* tcat = &(schemas->all_tables[table_ids[i]]);
        if( (temp = get_attr_loc( tcat, attr_name )) >= 0 ){
            find_count++;
            found = temp;
            *par_tab_id = tcat->id;
        }
    }
    if( found < 0 ){
        fprintf(stderr, "None of the tables contain an attribute '%s'\n.", attr_name);
        return -1;
    }
    if( find_count > 1 ){
        fprintf(stderr, "Multiple tables have an attribute named '%s'\n.", attr_name);
        return -1;
    }
    return found;
}

/*
 *
 * Search through list of table ids for the provided attribute.
 *
 * @parm: tables - List of table schemas to search through
 * @parm: num_tables - Number of tables in the list
 * @parm: tab_idx - return parameter for table schema index where
                       attribute was found
 * @parm: attr_name - attribute name to search for
 * @return: attribute location if found in the list of table schema ids
            if attribute not found then return -1 and print error.
            if attribute found in multiple tables return -1 and print error.
 */
int find_attribute_new( table_catalog** tables, int num_tables, int* tab_idx, char* attr_name ){
    int found = -1;
    int find_count = 0;
    for( int i = 0; i<num_tables; i++ ){
        int temp = -1;
        if( (temp = get_attr_loc( tables[i], attr_name )) >= 0 ){
            find_count++;
            found = temp;
            *tab_idx = tables[i]->id;
        }
    }
    if( found < 0 ){
        fprintf(stderr, "None of the tables contain an attribute '%s'\n.", attr_name);
        return -1;
    }
    if( find_count > 1 ){
        fprintf(stderr, "Multiple tables have an attribute named '%s'\n.", attr_name);
        return -1;
    }
    return found;
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
 * Parse the given string which can only be 1 character long
 * and return the corresponding math_operation type. if none
 * return -1;
 */
math_operations get_op( char* c ){
    math_operations op;
    if( strcmp("+", c) == 0 ){
        op = plus;
    }else if( strcmp("-", c) == 0 ){
        op = minus;
    }else if( strcmp("*", c) == 0){
        op = multiply;
    }else if( strcmp("/", c) == 0){
        op = divide;
    }else{
        op = -1;
    }
    return op;
}

/*
 * Check if the string's first and last character are quotes,
 * if it is a true/false or numerical value and if anything of
 * those are true then its a value and return false otherwise it is
 * an attribute and return true. Works for multitable attribute names
 * as well.
 * @return: True if check is an attribute
            False if check is a value
 */
bool is_attribute( char* check ){
    char first = check[0];
    char last = check[strlen(check)-1];
    return !( is_number(check) || (first == '"' && last == '"') || strcasecmp("true",check) == 0 || strcasecmp("false",check) == 0);
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
    char first = value[0];
    char last = value[strlen(value)-1];
    int val_type = attr_type;
    if( is_number(value) ){ // numeric value
        val_type = (attr_type == 0) ? 0 : (attr_type == 1) ? 1 : -1;
    }else if( (first == '"' && last == '"') ){ // string value
        val_type = (attr_type == 3) ? 3 : (attr_type == 4) ? 4 : -1;
    }else if( strcasecmp("true",value) == 0 || strcasecmp("false",value) == 0 ){ // boolean
        val_type = 2;
    }
    return val_type;
}

/*
 * Check the value to confirm the type the value is matches
 * the attribute type and if not return -1. Otherwise based
 * on the type fill the corresponding return parameter with
 * the associated value and return 0 for success.
 */
int set_compare_value( char* value, int type, union record_item* r_value ){
    bool null = (strcasecmp(value,"null") == 0);
    int val_type = -1;
    int str_len = strlen(value);
    int result = 0;
    char *eptr;
    if( (val_type = get_value_type( value, type )) == -1 ){
        fprintf(stderr, "ERROR: '%s' type doesn't match the attribute's type %s.\n", value, type_string( type ));
        return -1;
    }
    switch( type ){
        case 0: //int
            if( null ){ r_value->i = INT_MAX; }
            else{ r_value->i = atoi(value); }
            break;
        case 1: // double
            if( null ){ r_value->d = INT_MAX; }
            else{ r_value->d = strtod( value,&eptr ); }
            break;
        case 2: // boolean
            if( null ){ r_value->b = INT_MAX; }
            if(strcasecmp(value, "true") == 0){ r_value->b = true; }
            else { r_value->b = false; }
            break;
        case 3: // char
            if( null ){ memset(r_value->c, '\0', (str_len)*sizeof(char)); } //TODO: come back and make similar to varchar
            else{ strcpy(r_value->c, value); }
            break;
        case 4: // varchar
            if( null ){ memset(r_value->v, '\0', (str_len)*sizeof(char)); }
            else{ 
                memset(r_value->v, '\0', (str_len)*sizeof(char));
                strncpy(r_value->v, value+1, strlen(value)-2); 
                strcat(r_value->v, "\0");
            }
            break;
        default:
            fprintf(stderr, "ERROR: unknown type for '%s'\n", value);
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
    int first_type = get_attr_type( &(schemas->all_tables[t_id]), first);
    int other_type = -1;
    if( multitable ){
        other_type = get_attr_type( &(schemas->all_tables[ot_id]), other);
    }else{
        other_type = get_attr_type( &(schemas->all_tables[t_id]), other);
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

void destroy_where_stack(where_cmd** node){
    if( !where_is_empty(*node) ){
        pop_where_node( node );
        destroy_where_stack( node );
    }
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

// Builder Helper Functions
select_cmd* init_select_cmd( int num_attrs, int num_tables, int num_orderby ){
    select_cmd* select = (select_cmd*)malloc(sizeof(select_cmd));
    select->attributes_to_select = (char**)malloc(sizeof(char*)*num_attrs);
    // base allocation of attribute names
    for( int i = 0; i<num_attrs; i++ ){
        select->attributes_to_select[i] = malloc(0*sizeof(char)); // allows realloc to use pointer
    }
    select->selected = (int*)malloc(sizeof(int)*num_attrs);
    select->table_match = (int*)malloc(sizeof(int)*num_attrs);
    select->from_tables = (int*)malloc(sizeof(int)*num_tables);
    select->conditions = NULL;  // built by separate command
    select->orderby = (char**)malloc(sizeof(char*)*num_orderby);
    // base allocation of attribute names for orderby
    for( int i = 0; i<num_orderby; i++ ){
        select->attributes_to_select[i] = malloc(0*sizeof(char)); // allows realloc to use pointer
    }
    select->num_attributes = num_attrs;
    select->num_tables = num_tables;
    select->num_orderby = num_orderby;
    return select;
}

/*
 * Increase size of contained arrays in the select command structure
 */
void manage_select( select_cmd* select, int num_attrs, int num_tables ){
    select->attributes_to_select = (char**)realloc(select->attributes_to_select, num_attrs*sizeof(char*));
    select->selected = (int*)realloc(select->selected, num_attrs*sizeof(int));
    select->table_match = (int*)realloc(select->table_match, num_attrs*sizeof(int));
    select->from_tables = (int*)realloc(select->table_match, num_tables*sizeof(int));
    select->num_attributes = num_attrs;
    select->num_tables = num_tables;
}

/*
 * Assumes the orderby array has already been malloc'd.
 */
void add_orderby( select_cmd* select, int num_orderby ){
    select->orderby = (char**)realloc(select->orderby, num_orderby*sizeof(char*));
    select->num_orderby = num_orderby;
}

void destroy_select( select_cmd* select ){
    free( select->selected );
    free( select->table_match );
    free( select->from_tables );
    if( select->conditions != NULL ){
        destroy_where_stack( &(select->conditions) );
    }
    for( int i = 0; i<select->num_attributes; i++){ // free attribute names
        free( select->attributes_to_select[i] );
    }
    free( select->attributes_to_select );
    for( int i = 0; i<select->num_orderby; i++ ){
        free( select->orderby[i] );
    }
    free( select->orderby );
    free( select );
}

update_cmd* init_update_cmd( int table_id ){
    update_cmd* update = (update_cmd*)malloc(sizeof(update_cmd));
    update->table_id = table_id;
    update->conditions = NULL;
    update->set_attrs = (set *)malloc(sizeof(set));
    return update;
}

/* 
 * Increase size of set array
 */
void manage_sets( update_cmd* update, int num_ops ){
    update->set_attrs = (set *)realloc(update->set_attrs, num_ops*sizeof(set));
    update->num_attributes = num_ops;
}

void destroy_update( update_cmd* update ){
    if( update->conditions != NULL ){
        destroy_where_stack( &(update->conditions) );
    }
    free( update->set_attrs );
    free( update );
}

delete_cmd* init_delete_cmd( int table_id ){
    delete_cmd* delete = (delete_cmd*)malloc(sizeof(delete_cmd));
    delete->table_id = table_id;
    delete->conditions = NULL;
    return delete;
}

void destroy_delete( delete_cmd* delete ){
    if( delete->conditions != NULL ){
        destroy_where_stack( &(delete->conditions) );
    }
    free( delete );
}

insert_cmd* init_insert_cmd( int table_id, int num_records, int rec_size ){
    insert_cmd* insert = (insert_cmd*)malloc(sizeof(insert_cmd));
    insert->table_id = table_id;
    insert->num_records = num_records;
    insert->records = (union record_item**)malloc(num_records*sizeof(union record_item*));
    for(int i = 0; i<num_records; i++){
        insert->records[i] = (union record_item*)malloc(rec_size*sizeof(union record_item));
    }
    return insert;
}

void destory_insert( insert_cmd* insert ){
    for(int i = 0; i<insert->num_records; i++){
        free(insert->records[i] );
    }
    free( insert->records );
    free( insert );
}

void print_table( table_catalog* table ){
    union record_item** all_records;
    int num_records = get_records( table->storage_manager_loc, &all_records );
    // print header
    printf("Table: %s\n", table->table_name);
    for(int i = 0; i<table->attribute_count; i++){
        printf("%20s", table->attributes[i].name);
    }
    printf("\n");
    //print records
    for( int i = 0; i<num_records; i++){
        for( int j = 0; j<table->attribute_count; j++){
            switch( table->attributes[j].type ){
                case 0: //int
                    printf("%20i", all_records[i][j].i);
                    break;
                case 1: //double
                    printf("%20f", all_records[i][j].d);
                    break;
                case 2: //boolean
                    printf("%20s", all_records[i][j].b ? "TRUE" : "FALSE");
                    break;
                case 3: //char
                    printf("%20s", all_records[i][j].c);
                    break;
                case 4: //varchar
                    printf("%20s", all_records[i][j].v);
                    break;
            }
        }
        printf("\n");
    }
    // free
    for(int i = 0; i<num_records; i++){
        free( all_records[i] );
    }
    free( all_records );
}