/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#ifndef __STORAGE_H__
#define __STORAGE_H__
// TODO: confirm theses aren't in use w/ the system
#define START_TABLE '/5' 
#define END_TABLE '/6'

int page_size;  // Page size should be a variable stored
char * page_buffer;
char * db_location;
// beginning of file (first byte = page id)

typedef struct table_info{
	int table_id;
	union record_item *** table;
	int * page_ids;
} table_info;

typedef struct page_info{
	int page_id;
	union record_item *** page_data;
} page_info;

#endif