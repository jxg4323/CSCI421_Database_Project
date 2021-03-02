
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

// Table Structure Information

typedef struct foreign_table_info{
    int num_foreign_attributes;
    char **info; // ["a_1", "a_2","foreign_tabe_name", "r_1", "r_2"]
} foreign_data;

typedef struct table_catalog{
    int id;
    // Sizes of arrays
    int attribute_count;
    int key_indices_size;
    int num_prim_keys;
    int num_foreign_keys;
    int table_name_size;
    int foreign_table_count;
    // table data
    char *table_name;
    char** attribute_names; // column names
    // [type, constraintA, constraintB, constraintC, deleted]
    int (* attr_info)[5]; // constraints array and data types
    char ** foreign_key_names; // foreignkeys: array of foreign information
} table_catalog;

typedef struct table_catalog_array{
    int last_made_id;
    int table_count;
    table_catalog *catalogs;
} catalogs;

// Catalog Functions

catalogs* allocate_catalogs(int table_count);
void read_catalogs(char *db_loc, catalogs* logs);
void write_catalogs(char *db_loc, catalogs* logs);
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
 * Layout of the foreign_row is: ["a_1", "a_2","foreign_tabe_name", "r_1", "r_2"]
 * allocate or reallocate memory for the addidtion of the next foreign reference.
 * 
 * Return 1 with success of foreign info addition and -1 otherwise.
 */
int add_foreign_data(table_catalog* t_cat, char **foreign_row, int f_key_count);

#define __TABLESCHEMA_H__