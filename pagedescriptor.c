/*
Authors: Kelsey Dunn, Alex Frankel, James Green, Varnit Tewari
Assigment: Phase 1
Professor: Scott Johnson 
*/
#include "pagedescriptor.h"
#include "storage.h"


/*
 * Print page manager.
 */
void print_page_manager( page_manager * manager ){

}

/*
 * Initialize passed pointer with the given number of pages 
 * as page_description which will hold information
 * on the page records.
 */
int init_page_manager(int num_of_pages, page_manager *manager ){

}

/* 
 * If the page manager has already been written to the disk 
 * then read the information to volatile memeory stored in 
 * the passed pointer.
 */
int read_page_manager(char *db_loc, page_manager *manager ){

}

/*
 * Write the contents of the page_manager structure to the
 * disk.
 */
int write_page_manager(char *db_loc, page_manager *manager ){

}

/*
 * Update the given page in the passed page manager with the new 
 * record count and free location.
 */
int update_page(int page_id, int num_records, int free_loc, page_manager *manager ){

}

/*
 * Loop through the page manager in search of the given page_id
 * and if found return the struct pointer otherwise return null.
 */
page_desc* get_page_desc( int page_id, page_manager *manager ){

}

/*
 * Free the allocated memory for the provided page_manager.
 */
void free_page_manager( page_manager *manager ){

}

/*
 * Loop through the page manager and free the struct pointer
 * for the given page if found.
 */
int delete_page(int page_id, page_manager *manager ){

}

/*
 * Add a new page to the page manager.
 */
int add_page( int page_id, page_manager *manager ){

}