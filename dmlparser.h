#include "database.h"

#ifndef __DMLPARSER_H__
#define __DMLPARSER_H__
#define DELIMITER " \t\r\n();,"
#define INIT_NUM_TOKENS 100

/*
 * Logical Comparator Types stored as integers.
 * '='  : 0
 * '<'  : 1
 * '>'  : 2
 * '<=' : 3
 * '>=' : 4
 * '!=' : 5
 */
typedef enum {eq, lt, gt, lte, gte, neq} comparators;
/*
 * Mathematical Operations Types stored as integers.
 * '+'  : 0
 * '-'  : 1
 * '*'  : 2
 * '/'  : 3
 */
typedef enum {plus, minus, multiply, divide} math_operations;

/*
 * Type of conditional command stored as integers
 * 'AND'  : 0
 * 'OR'   : 1
 * 'COND' : 2
 */
typedef enum {AND, OR, COND} where_node_type;

/* Hold conditional statement, if attr is NULL ignore
 * if attr isn't null then value for it is place in corresponding valNUM
 * if both attr's are null then compare vals
 */
typedef struct conditional_cmd_struct{
	int first_table_id;
	int other_table_id;
	int attr_type;
	int first_attr; // base attribute to check types with
	int other_attr;
	comparators comparator; 
	union record_item value;
	bool result_value;  // TODO: consider adding a bool value "use_value"
} conditional_cmd;

/*
 * If the left or right attribute values are the same sa the equated attribute set
 * that value for the left or right int.
 *
 */
typedef struct set_attr{
	int equated_attr; 
	int left_attr;
	int right_attr;
	int type;
	bool math_op; // True: execute operation on data
	bool use_left_attr; // True: use left side attribute in operation
	bool use_right_attr; // True: use right side attribute in operation 
	bool left_val_set; // True: new_left_value was set
	bool right_val_set; // True: new_right_value was set
	math_operations operation; // '+', '-', '*', '/'
	union record_item new_left_value, new_right_value;
} set;

typedef struct where_node{
	where_node_type type; // if "AND" or "OR" then ignore cond o.w. eval condition
	conditional_cmd* cond;
	struct where_node* link;
} where_cmd;

typedef struct update_cmd_struct{
	int num_attributes; // number of attributes to update
	int table_id;
	set* set_attrs; // array of set operations to execute per record
	where_cmd* conditions; // Have to be executed on per record basis
} update_cmd;

typedef struct select_cmd_struct{
	int num_attributes;
	int num_tables; // number of tables to select from
	int num_orderby;
	char** attributes_to_select; // not neccessarilly from the same table
	int* selected;
	int* table_match; // same length as 'selected' to inform which attribute goes to which table
	bool wildcard;
	int* from_tables; // if multiple perform cartesian product of tables to then act upon!!
	where_cmd* conditions;
	char** orderby; // TODO: add orderby
} select_cmd;

typedef struct insert_cmd_struct{
	int num_records;
	int table_id;
	union record_item** records; 
} insert_cmd;

typedef struct delete_cmd_struct{
	int table_id;
	where_cmd* conditions;
} delete_cmd;

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the select query.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'select', 'from', 'where', and 'orderby'
 *					ex: ['selct', 'id', 'from', 'Person', 'where', 'id', '<', '8', 'orderby', 'id', '\0']
 * @return new select command structure that the user is responsible for freeing 
 			if no errors were encountered, o.w. return NULL
 */
select_cmd* build_select( int token_count, char** tokens, catalogs* schemas );

int build_set( int token_count, char** tokens, table_catalog* table, set ** set_array, catalogs* schemas );

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the update query.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'update', 'set', and 'where'
 *					ex: ['update', 'Person', 'set', 'AGE', '=', 'AGE', '+', '1', 'where', 'id', '<', '8', '\0']
 * @return new update command structure that the user is responsible for freeing 
 			if no errors were encountered, o.w. return NULL
 */
update_cmd* build_update( int token_count, char** tokens, catalogs* schemas );

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the delete query.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'delete', 'from', and 'where'
 *					ex: ['delete', 'from', 'Person', 'where', 'id', '<', '8', '\0']
 * @return new delete command structure that the user is responsible for freeing 
 			if no errors were encountered, o.w. return NULL
 */
delete_cmd* build_delete( int token_count, char** tokens, catalogs* schemas );

/*
 * Loop through the tokens provided and confirm their validity
 * create a new select_cmd structure and fill in the data accordingly.
 * The token layout should match the statement, using the keywords to
 * separate sections of the insert query as well as a '\0' character to
 * separate different records.
 *
 * @parm: token_count - number of tokens to parse
 * @parm: tokens - Tokens sepereated by 'inset', 'into', and 'values'
 *					ex: ['insert', 'into', 'Person', 'values', '1', '"foo"', 'true', '2.1', 
 						'\0', '3', '"baz"', 'true', '4.14', '\0']
 * @return new update command structure that the user is responsible for freeing 
 			if no errors were encountered, o.w. return NULL
 */
insert_cmd* build_insert( int token_count, char** tokens, catalogs* schemas );

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
 *					ex: ['where', 'id', '<', '8', 
 						'and', 'name', '=', 'blahblah', 
 						'or', 'AGE', '>=', '21', '\0']
 * @return new update command structure that the user is responsible for freeing 
 			if no errors were encountered, o.w. return NULL
 */
where_cmd* build_where( char** table_names, int num_tables, int token_count, char** tokens, catalogs* schemas ); // NEED to deal with multiple tables

/*
 * Take a cartesian product of the tables in the table_ids array
 * allocating memory for the resulting 2d array of records.
 *
 * @parm: num_tables - Number of tables to join
 * @parm: table_ids - Table Schema Ids of the tables
 * @parm: schemas - Array of table cataglogs
 * @return: 2d array of the resulting joins if no issues o.w. NULL
 */
union record_item** cartesian_product( int num_tables, int* table_ids, catalogs* schemas ); 

/*
 * Execute the where conditionals with the values from provided record.
 * The 'and' conditional takes priority and should be executed first. // "Stack maybe"
 *
 * @return: 0 if record evaluates the where conditionals to true and
			-1 otherwirse.
 */
int check_where_statement( where_cmd* where, union record_item* record, int record_size, catalogs* schemas );

/*
 * 
 * @return 2d array result with success and
 		   NULL otherwise and print error statement
 */
union record_item** execute_select( select_cmd* select, catalogs* shcemas );

/*
 *
 */
int execute_insert( insert_cmd* update, catalogs* schemas );

/*
 *
 */
int execute_update( update_cmd* update, catalogs* schemas );

/*
 *
 */
int execute_delete( delete_cmd* delete, catalogs* schemas );

// Helper Functions  --> GOOD
void set_condition_info( conditional_cmd* cond, int fTid, int oTid, int attrType, comparators c, int fAttr, int oAttr, union record_item v1 );
comparators get_comparator( char* c );
math_operations get_op( char* c );
int parser( char* statement, char** data, bool use_commas );
bool is_attribute( char* check );
void backtrack_insert( int rec_count, insert_cmd* insert );
int change_record_val( union record_item* old_val, union record_item new_val, int type );
int exec_math_op( union record_item* result, union record_item left, union record_item right, math_operations op, int type, int lsize, int rsize);
char* get_attr_name_from_token( char* attr_token );
int get_names_from_token( char* attr_token, char*** return_array );
int eval_condition( conditional_cmd* cond, union record_item* record, catalogs* schemas );
bool compare_condition( comparators comp, int type, int size, union record_item left, union record_item right );
bool check_types( int t_id, int first, int other, bool multitable, int ot_id, catalogs* schemas );
int find_attribute( int* table_ids, int num_tables, int* par_tab_id, char* attr_name, catalogs* schemas );
int find_attribute_new( table_catalog** tables, int num_tables, int* tab_idx, char* attr_name );
int set_compare_value( char* value, int type, union record_item* r_value );
int get_value_type( char* value, int attr_type );
where_cmd* new_where_node();
conditional_cmd* new_condition_cmd();
void destroy_where_node(where_cmd* node);
void destroy_where_stack(where_cmd** node);
int push_where_node(where_cmd** top, where_node_type type, conditional_cmd* condition);
int pop_where_node(where_cmd** top);
int where_is_empty(where_cmd* top);
where_cmd* peek(where_cmd* top);
// Builder Helper Functions
void destroy_select( select_cmd* select );
select_cmd* init_select_cmd( int num_attrs, int num_tables, int num_orderby );
void manage_select( select_cmd* select, int num_attrs, int num_tables );
void add_orderby( select_cmd* select, int num_orderby );
update_cmd* init_update_cmd( int table_id );
void manage_sets( update_cmd* update, int num_ops );
void destroy_update( update_cmd* update );
delete_cmd* init_delete_cmd( int table_id );
void destroy_delete( delete_cmd* delete );
insert_cmd* init_insert_cmd( int table_id, int num_records, int rec_size );
void destory_insert( insert_cmd* insert );
// Printing Helper Funcitons
void print_table( table_catalog* table );

#endif