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
#include <limits.h>
#include <math.h>

#define BASE_TABLE_PAGES_SIZE 20
#define MIN_ALLOC 50
#define DATABASE_CONFIG_FILE "database_config"
#define DATABASE_FILE_NAME_LEN 15
#define TABLE_METADATA_FILE "table_metadata"
#define TABLE_METADATA_FILE_LEN 14
#define PAGE_FILENAME_BEGIN "page" // all pages will start with 
#define PAGE_FILE_LEN 4
#define MAX_PAGES_FILE_CHARS 3

typedef union record_item r_item;

// Page Buffer Information 

typedef struct page_layout{
	int page_id;
	int num_of_records; // floor(page_size/record_size)
	int req_count;
	r_item * page_records;
} page_info;

typedef struct buffer_manager{
	int last_id;
	int num_of_pages;
	page_info *pages;
} buffer_manager;

// Database Config Information

typedef struct db_config{
	int page_size;  // Number of bytes in page
	int buffer_size; // number of pages to read
	char * db_location;
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

// Globals

table_schema_array *all_table_schemas;
db_config *db_data;
lookup_table *table_l;
buffer_manager * page_buffer; 


// Database Config Functions

int get_db_config( char * db_loc, db_config* config );
int write_db_config( char * db_loc );
void pretty_print_db_config( db_config *config);
int allocate_db_data(int page_size, int buf_size, char *db_loc);
void free_config( db_config *config );

// Table Functions

int get_all_schemas(char * db_loc);
int write_all_schemas(char * db_loc);
void allocate_all_schemas();
void manage_all_schema_array(int count, bool increase_size);
table_data* get_table_schema( int table_id );
void pretty_print_table_schemas( table_schema_array *schemas );
void init_table_schema(int t_id, int types_len, int key_len, table_data *t_schema);
void free_table_schemas();

// Page Functions
void init_page_buffer( );
void init_page_layout( int pid, int record_num, page_info* page );
int request_page_in_buffer( int page_id );
int add_page_to_buffer( int page_id, int loc );
bool check_buffer_status( );
int remove_least_used_page( );
int get_open_spot( );
int write_page( page_info* page );
int read_page( int page_id, page_info* page );
void free_buffer( );

#endif