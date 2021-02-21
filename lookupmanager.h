/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#ifndef LOOKUPMANAGER_H
#define LOOKUPMANAGER_H
// Lookup table 
#include <stdbool.h>
#define LOOKUP_FILE_NAME_LEN 11
#define LOOKUP_TUPLE_SIZE 3
#define LOOKUP_TABLE_FILE "lookup_file"
#define MIN_TABLE_COUNT 2
#define MIN_BIN_SIZE 1

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
table_pages *get_table_struct( lookup_table* l_table, int table_id );
void free_lookup_table(lookup_table* l_table);
void free_table_pages(table_pages* t_data);
void print_lookup_table(lookup_table *table);
lookup_table* delete_table_info(lookup_table *l_table, int table_id); 
int add_table_info(lookup_table *l_table, int table_id); 
int clear_table_bin(lookup_table *l_table, int table_id);
void manage_table_array(int count, lookup_table *table, bool increase);

#endif