/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#ifndef __STORAGE_H__
#define __STORAGE_H__

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>     /* read library call comes from here */
#include <string.h>

#define BASE_TABLE_PAGES_SIZE 20
#define MIN_ALLOC 50
#define DATABASE_CONFIG_FILE "database_config"
#define DATABASE_FILE_NAME_LEN 15
#define TABLE_METADATA_FILE "table_metadata"
#define TABLE_METADATA_FILE_LEN 14

table_schema_array *all_table_schemas;
db_config *db_data;
lookup_table *table_l;

// Database Config Information

typedef struct db_config{
	int page_size;  // Page size should be a variable stored
	int buffer_size;
	char * db_location;
	char * page_buffer;
} db_config;

// Table Structure Information

typedef struct table_data{
    int id;
    int data_types_size;
    int key_indices_size;
    int *data_types;
    int *key_indices;
} table_data;

typedef struct table_schema_array{
    int last_made_id;
    int table_count;
    table_data *tables;
} table_schema_array;

// Database Config Functions

int get_db_config( char * db_loc, db_config* config );
int update_db_config( char * db_loc, char * buffer );
void pretty_print_db_config( db_config *config);
int allocate_db_data(int page_size, int buf_size, char *db_loc);
void free_config( db_config *config );

// Table Functions

int get_all_schemas(char * db_loc);
int write_all_schemas(char * db_loc);
void allocate_all_schemas();

#endif