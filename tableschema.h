
#ifndef __TABLESCHEMA_H__
#define __TABLESCHEMA_H__
// Tables Schema Catalog Data
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> 
#include <string.h>
#define TABLE_CATALOG_FILE "table_catalog"
#define TABLE_CATALOG_FILE_LEN 14
#define NOTNULL 6
#define PRIMARYKEY 7
#define UNIQUE 8
#define EMPTY -1
#define MAX_NUM_CONSTRAINTS 3
#define MAX_CONSTRAINT_CHARS 11
#define INTEGER "integer"
#define DOUBLE "double"
#define BOOLEAN "boolean"
#define CHAR "char"
#define VARCHAR "varchar"

// Table Structure Information
typedef struct foreign_table_info{
    char *name;
    bool deleted; // DON'T STORE --> if True then don't write
    int foreign_table_id;
    int tuple_size; // size of tuple --> number of attributes involved
    int *orig_attr_locs; // location of attribute in original table attribute array
    int *for_attr_locs; // location of attribute in foreign table attribute array
} foreign_data;

typedef struct unique_info{
    int tup_size;
    bool deleted; // DON'T STORE --> if True then don't write
    int *attr_tuple;
} unique;

typedef struct attribute_info{
    char *name;
    int type;
    bool deleted; // DON'T STORE --> if True then don't write
    // TODO: add default value --> Maybe??
    // TODO: maybe add char & varch len
    // Constraints stored as 0 or 1  --> if 1 then constrain is in use
    int notnull; 
    int primarykey;
    int unique;
} attr_info;

typedef struct table_catalog{
    int id;
    int storage_manager_loc;
    bool deleted; // DON'T STORE --> if True then don't write
    // Sizes of arrays with deletions
    int attribute_count;
    int foreign_size;
    int primary_size;
    int unique_size;
    // table data
    char *table_name;
    attr_info *attributes; // constraints array and data types
    foreign_data *relations;
    unique *unique_tuples;
    int *primary_tuple;
} table_catalog;

typedef struct table_catalog_array{
    int table_count;
    table_catalog *all_tables;
} catalogs;

// Helper Functions 
int type_conversion(char* type);  //   --> GOOD
char *type_string( int type );  //   --> GOOD
int get_table_count_no_deletes(catalogs* logs);    //   --> GOOD
int get_attribute_count_no_deletes(table_catalog* t_cat);    //   --> GOOD
int get_relation_count_no_deletes(table_catalog* tcat);    //   --> GOOD
int get_unique_count_no_deletes(table_catalog* tcat);    //   --> GOOD
void pretty_print_catalogs(catalogs* logs);    //   --> GOOD
void pretty_print_table(catalogs* logs,table_catalog* tcat);    //   --> GOOD
void pretty_print_attributes( attr_info* attributes, int size );    //   --> GOOD
void pretty_print_relations( catalogs* logs, table_catalog* tcat, foreign_data* relations, int size );    //   --> GOOD
void pretty_print_unique_tuples( table_catalog* tcat, unique* tuples, int size );    //   --> GOOD
void pretty_print_primary_tuples( table_catalog* tcat, int* prim_tup, int size );    //   --> GOOD
char *get_attr_name( catalogs* logs, char *table_name, int attr_id );    //   --> GOOD
int* get_table_data_types( table_catalog* tcat );    //   --> GOOD
void delete_table( table_catalog* tcat );    //   --> GOOD

// Catalog Functions
catalogs* initialize_catalogs();  //   --> GOOD
void init_catalog(table_catalog* catalog, int tid, int meta_id, int a_size, int f_size, int p_size, int u_size, char *table_name);  //   --> GOOD
void init_attribute(attr_info* attr, int type, int notnull, int primkey, int unique, char *name);  //   --> GOOD
void init_foreign_relation(foreign_data* fdata, char* name, int fid, int size, int *orig_attrs, int *for_attrs);   //   --> GOOD
void init_unique_tuple(unique* udata, int tup_size, int *tuple);   //   --> GOOD
void init_primary_tuple(table_catalog *t_cat, int *tuple, int size);  //   --> GOOD

// Manage functions increase sizes of corresponding dynamic arrays
void manage_catalogs(catalogs *logs, int table_count, bool increase);  //   --> GOOD
void manage_attributes(table_catalog* t_cat, int attr_count, bool increase);  //   --> GOOD
void manage_foreign_rels(table_catalog *t_cat, int rel_count, bool increase);   //   --> GOOD
void manage_prim_tuple(table_catalog *t_cat, int prim_size, bool increase);  //   --> GOOD
void manage_unique_tuple(table_catalog *t_cat, int size, bool increase);  //   --> GOOD

// Read And Write Catalogs
int read_catalogs(char *db_loc, catalogs* logs);    //   --> GOOD
int write_catalogs(char *db_loc, catalogs* logs);    //   --> GOOD

// Manage Catalog Functions
/*
 * Create new table catalog with just table name and newly allocated structure 
 * Return location of new catalog in array if success otherwise -1.
 */
int new_catalog(catalogs *logs, char *table_name);    //   --> GOOD

/*
 * Add attribute information to the given table, and increment the attribute 
 * count for the table. 
 * Return 1 for success and -1 for failure.
 */
int add_attribute(table_catalog* t_cat, char *attr_name, char *type, int constraints[3]);   //   --> GOOD

/*
 * Mark an attribute as deleted in the attribute information matrix. If an 
 * attribute is marked as removed then don't write it to the disk.
 * Return 1 for success and -1 for failure.
 */
int remove_attribute(catalogs* logs, table_catalog* t_cat, char *attr_name);   //   --> GOOD

/*
 * Layout of the foreign_row is: ["a_1", "a_2", "r_1", "r_2"]
 * allocate or reallocate memory for the addidtion of the next foreign reference.
 * Confirm the other attributes in the foreign table exist as well as the foreign 
 * table itself, if not return -1.
 * 
 * Return 1 with success of foreign info addition and -1 otherwise.
 */
int add_foreign_data(catalogs* logs, table_catalog* t_cat, char **foreign_row, int f_key_count, char* f_name);   //   --> GOOD

/*
 * The foreign row is the an array of tokens that are as follows:
 *      ["foreign_tabe_name", "a_1", "a_2", "r_1", "r_2"]
 * Change the deleted marker to 1 without decreasing the size of 
 * foreign relations. 
 * Return location of foreign_data in array if successful o.w. -1.
 */
int remove_foreign_data(catalogs* logs, table_catalog* t_cat, char**foreign_row, int f_count);  //   --> GOOD

/*
 * Loop through the attribute information in the table catalog and
 * apply the primary key constraint to the attributes whose names
 * match those in the @param prim_names. Confirm the attributes aren't
 * already apart of a primary key for the table as well as confirm the
 * table doesn't have a primary key already if so return -1.
 *
 * Return 1 with succesful upate and -1 otherwise.
 */
int add_primary_key(table_catalog* t_cat, char **prim_names, int num_keys);  //   --> GOOD

/*
 * Delete the primary key information from the table.
 * Return 1 with success and -1 otherwise.
 */
int remove_primary_key(table_catalog* t_cat);  //   --> GOOD

/*
 * Confirm no other unique tuples have the combination of attributes,
 * if so return -1 and print an error. Otherwise return 1.
 */
int add_unique_key(table_catalog* t_cat, char **unique_name, int size);   //   --> GOOD

/*
 * Remove the unique tuple based on the tuple provided, find the tuple
 * that matches and remove it.
 * If successfull return 1 otherwise return -1.
 */
int remove_unique_key(table_catalog* t_cat, char** unique_name, int size);    //   --> GOOD

/*
 * Search through all catalog information to find the one with
 * the same table name as provided, given that all table names
 * are unique.
 * Return index for the table catalog in logs or -1 if there 
 * isn't one. 
 */
int get_catalog(catalogs *logs, char *tname);

/*
 * Search through all catalog information to find the one with
 * the same table name as provided, given that all table names
 * are unique.
 * Return pointer to the table catalog or NULL if there isn't one. 
 */
table_catalog* get_catalog_p(catalogs* logs, char* tname);  //   --> GOOD

/*
 * Based on the table id retrieve the attribute location
 * in the table catalog.
 * Return location of attribute in table if found, o.w. -1.
 */
int get_attr_loc(table_catalog *tcat, char *attr_name);  //   --> GOOD

/*
 * Based on the table catalog and attribute location in the table catalog
 * Return index of attribute in a record for the table if found, o.w. -1
 */
int get_attr_idx(table_catalog *tcat, int attr_loc);

/*
 * Check if the table name is already in use.
 * Return True if a table name is found, false otherwise.
 */
bool check_table_name(catalogs *logs, char *tname);  //   --> GOOD

/*
 * Look through the primary key to see if the primary
 * key contains the provided attribute ID and if so 
 * return TRUE other wise return FALSE indication the 
 * primary key DOES NOT contain the attribute.
 */
bool check_prim_key( table_catalog* tcat, int attr_id );

/*
 * Search through the foreign relations to find any relations which 
 * contain the provided attribute ID and place the location of the relations
 * in the @param ret_arr (should have allocated num_relations*sizeof(int)) 
 * which assumes all realtions contain the attribute which the user is 
 * responsible for freeing after call. 
 * Return size of the ret_arr if there were nay found and if no relations 
 * contained the attribute then return -1.
 */
int check_foreign_relations( table_catalog* tcat, int attr_id, int *ret_arr );

/*
 * Mark any unique tuples with the provided attribute id as 
 * deleted and return 1 with success, -1 otherwise.
 */
int delete_uniq_tup( table_catalog* tcat, int attr_id );

/*
 * Write catalog information to disk and free pointer.
 */
void terminate_catalog(catalogs *logs); //   --> GOOD



#endif
