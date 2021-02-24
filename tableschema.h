
#ifndef __TABLESCHEMA_H__
#define __TABLESCHEMA_H__
// Tables Schema Catalog Data
// TODO: might have to create another one separate
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#define TABLE_CATALOG_FILE "table_catalog"
#define TABLE_CATALOG_FILE_LEN 14

// Table Structure Information

typedef struct table_data{
    int id;
    int data_types_size;
    int key_indices_size;
    int *data_types;
    int *key_indices;
    // TODO: add names column
    // TODO: constraints --> unique to data type & name --> equal to column number
    // TODO: primarykey: array of what attributes make up 
    // TOOD: foreignkeys: array of keys
    // TODO: foreighnkey tables: array of table names
} table_desc;

typedef struct table_schema_array{
    int last_made_id;
    int table_count;
    table_desc **tables;
} table_catalog;

// Page Functions


#define __TABLESCHEMA_H__