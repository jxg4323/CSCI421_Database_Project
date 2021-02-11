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

int get_db_config( char * db_loc, db_config* config );
int update_db_config( char * db_loc, char * buffer );
void pretty_print_db_config( db_config *config);
void allocate_db_config(db_config* config, int db_loc_len, int buf_size);
void free_config( db_config *config );

#endif