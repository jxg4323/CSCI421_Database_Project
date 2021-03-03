
#ifndef __TABLESCHEMA_H__
#define __TABLESCHEMA_H__
// Tables Schema Catalog Data
// TODO: might have to create another one separate
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
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
//TODO: Ask profressor for clarification on tuples for unique, primary, foreign -> tuples or 1-1 correlation of attributes
typedef struct foreign_table_info{
    char *name;
    int foreign_table_id;
    int tuple_size; // size of tuple --> number of attributes involved
    int *orig_attr_locs; // location of attribute in original table attribute array
    int *for_attr_locs; // location of attribute in foreign table attribute array
} foreign_data;

typedef struct unique_info{
    int tup_size;
    int *attr_tuple;
} unique;

typedef struct attribute_info{
    char *name;
    int type;
    bool deleted; // DON'T STORE --> if True then don't write
    // Constraints stored as 0 or 1  --> if 1 then constrain is in use
    int notnull; 
    int primarykey;
    int unique;
} attr_info;

typedef struct table_catalog{
    int id;
    bool deleted; // DON'T STORE --> if True then don't write
    // Sizes of arrays with deletions
    int attribute_count;
    int foreign_size;
    int primary_size;
    int unique_size;
    // table data
    char *table_name;
    attr_info *attributes; // constraints array and data types
    foreign_data *relation;
    unique *unique_tuple;
    int *primary_tuple;
} table_catalog;

typedef struct table_catalog_array{
    int table_count;
    table_catalog *all_tables;
} catalogs;

// Catalog Functions

catalogs* initialize_catalogs();
void manage_catalogs(catalogs *logs, int table_count, bool increase);
void init_catalog(table_catalog* catalog, int tid, char *table_name );
void init_attribute(attr_info* attr, int type, int notnull, int primkey, int unique, int rel_count, char *name);3
// Manage functions increase sizes of corresponding dynamic arrays
void manage_attributes(table_catalog* t_cat, int attr_count);
void manage_foreign_rels(table_catalog *t_cat, int rel_coutn);
void manage_prim_tuple(table_catalog *t_cat, int prim_size);
void manage_unique_tuple(table_catalog *t_cat, int size);

int type_conversion(char* type);
// Read And Write Catalogs
int read_catalogs(char *db_loc, catalogs* logs);
int write_catalogs(char *db_loc, catalogs* logs);
/*
 * Create new table catalog with just table name and newly allocated structure
 * Return location of new catalog in array if success otherwise -1.
 */
int new_catalog(catalogs *logs, char *table_name); 

/*
 * Add attribute information to the given table, and increment the attribute
 * count for the table. 
 * Return 1 for success and -1 for failure.
 */
int add_attribute(table_catalog* t_cat, char *attr_name, char *type, int constraints[3]);

/*
 * Mark an attribute as deleted in the attribute information matrix. If an 
 * attribute is marked as removed then don't write it to the disk.
 * Return 1 for success and -1 for failure.
 */
int remove_attribute(table_catalog* t_cat, char *attr_name);

/*
 * Layout of the foreign_row is: ["foreign_tabe_name", "a_1", "a_2", "r_1", "r_2"]
 * allocate or reallocate memory for the addidtion of the next foreign reference.
 * Confirm the other attributes in the foreign table exist as well as the foreign 
 * table itself, if not return -1.
 * 
 * Return 1 with success of foreign info addition and -1 otherwise.
 */
int add_foreign_data(table_catalog* t_cat, char **foreign_row, int f_key_count);

/*
 * The foreign row is the an array of tokens that are as follows:
 *      ["foreign_tabe_name", "a_1", "a_2", "r_1", "r_2"]
 * Change the deleted marker to 1 without decreasing the size of 
 * foreign relations. 
 * Return location of foreign_data in array if successful o.w. -1.
 */
int remove_foreign_data(table_catalog* t_cat, char**foreign_row, int f_count);

/*
 * Loop through the attribute information in the table catalog and
 * apply the primary key constraint to the attributes whose names
 * match those in the @param prim_names. Confirm the attributes aren't
 * already apart of a primary key for the table as well as confirm the
 * table doesn't have a primary key already if so return -1.
 *
 * Return 1 with succesful upate and -1 otherwise.
 */
int add_primary_key(table_catalog* t_cat, char **prim_names, int num_keys);

/*
 * Delete the primary key information from the table.
 * Return 1 with success and -1 otherwise.
 */
int remove_primary_key(table_catalog* t_cat);

/*
 * Confirm no other unique tuples have the combination of attributes,
 * if so return -1 and print an error. Otherwise return 1.
 */
int add_unique_key(table_catalog* t_cat, char **unique_name, int size);

/*
 * Search through all catalog information to find the one with
 * the same table name as provided, given that all table names
 * are unique.
 * Return pointer to the table catalog or NULL if there isn't one. 
 */
table_catalog* get_catalog(catalogs *logs, char *tname);

/*
 * Based on the table id retrieve the attribute location
 * in the table catalog.
 * Return location of attribute in table if found, o.w. -1.
 */
int get_attr_loc(catalogs *logs, int tid, char *attr_name);

/*
 * Check if the table name is already in use.
 * Return True if a table name is found, false otherwise.
 */
bool check_table_name(catalogs *log, char *tname);

/*
 * Write catalog information to disk and free pointer.
 */
void terminate_catalog(catalogs *logs);

#define __TABLESCHEMA_H__