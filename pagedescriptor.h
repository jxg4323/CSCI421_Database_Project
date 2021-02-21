/*
Authors: Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#ifndef __PAGEDESCRIPTOR_H__
#define __PAGEDESCRIPTOR_H__
// Page Descriptor File
#define PAGE_TUPLE_SIZE 4
#define PAGE_FILENAME_BEGIN "page" // all pages will start with 
#define PAGE_FILE_LEN 4
#define MAX_PAGES_FILE_CHARS 3
#define PAGE_MANAGER_FILE "page_descriptor"
#define PAGE_MANAGER_FILE_LEN 15

typedef struct page_description{
	int page_id;
	int num_records;
	int free_loc; // spot in 2d array of record items that is free
} page_desc;

typedef struct page_manager{
	int last_id;
	int num_of_pages;  // Number of tables in lookup file
	page_desc* table_data;  // array of page_desc *
} page_manager;

void print_page_manager( page_manager * manager );
int init_page_manager(int num_of_pages, page_manager *manager );
int read_page_manager(char *db_loc, page_manager *manager );
int write_page_manager(char *db_loc, page_manager *manager );
int update_page(int page_id, int num_records, int free_loc, page_manager *manager );
page_desc* get_page_desc( int page_id, page_manager *manager );
void free_page_manager( page_manager *manager );
int delete_page(int page_id, page_manager *manager );
int add_page( int page_id, page_manager *manager );


#endif