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

// Database Config Information

typedef struct table_info{
	int table_id;
	union record_item *** table;
	int * page_ids;
} table_info;

typedef struct page_info{
	int page_id;
	union record_item *** page_data;
} page_info;

typedef struct db_config{
	int page_size;  // Page size should be a variable stored
	int buffer_size;
	char * db_location;
	char * page_buffer;
} db_config;

typedef struct table_data{
    int id;
    int data_types_size;
    int *data_types;
    int key_indices_size;
    int *key_indices;
} table_data;

typesdef struct table_schema_array{
    int last_made_id;
    int table_count;
    table_data *tables;
} table_schema_array;

//TODO: need to create a global for db_config * & maniuplate that data
db_config *db_data;
lookup_table *table_l;

int get_db_config( char * db_loc, db_config* config );
int update_db_config( char * db_loc, char * buffer );
void pretty_print_db_config( db_config *config);
int allocate_db_data(int page_size, int buf_size, char *db_loc);
void free_config( db_config *config );

#endif