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
#define LOOKUP_TUPLE_SIZE 3
#define DATABASE_CONFIG_FILE "database_config"
#define LOOKUP_TABLE_FILE "lookup_file"
#define DATABASE_FILE_NAME_LEN 15
#define LOOKUP_FILE_NAME_LEN 11


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

// Lookup table 

typedef struct table_page_locs{
	int table_id;
	int bin_size;
	// <page_id>,<start_byte>,<end_byte>
	int ** byte_info;  // bin that stores the page ids as well as the blocks in the page where data for the table is stored
} table_pages;

typedef struct lookup_table{
	int table_count;  // Number of tables in lookup file
	table_pages* table_data;  // array of table_pages
} lookup_table;

void print_table_bin(table_pages *table_data);
int initialize_lookup_table(int num_of_tables, lookup_table* table);
void init_table_pages(int arr_size, int t_id, table_pages * t_data);
int read_lookup_file(char* db_loc, lookup_table* l_table);
int write_lookup_table(lookup_table* lookup_table, char* db_loc);
int update_lookup_table(lookup_table* l_table, int table_id, int page_id, int s_byte, int e_byte);
int get_table_info(lookup_table* l_table, int table_id);
void free_lookup_table(lookup_table* l_table);
void free_table_pages(table_pages* t_data);
void print_lookup_table(lookup_table *table);


#endif