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
	printf("Page Descriptor:\n lastId: %d num_of_pages: %d\n", manager->last_id, manager->num_of_pages);
	for( int i = 0; i < manager->num_of_pages; i++ ){
		page_desc *tmp = manager->page_data[i];
		printf("Page ID: %d, Record Count: %d, Free Location: %d, Record Size: %d\n", tmp->page_id, tmp->num_records, tmp->free_loc, tmp->record_size);
	}	
}

/*
 * Initialize passed pointer with the given number of pages 
 * as page_description which will hold information
 * on the page records.
 */
int init_page_manager(int num_of_pages, int l_id, page_manager *manager ){
	manager->last_id = l_id;
	manager->num_of_pages = num_of_pages;
	manager->page_data = (page_desc **)malloc(num_of_pages*sizeof(page_desc *));
}

/*
 * Initialize page descriptor with provided id and allocate 
 * array of record sizes in page.
 */
void init_page_desc(int pid, int num_records, int free_loc, int rec_size, page_desc *desc ){
	desc->page_id = pid;
	desc->num_records = num_records;
	desc->free_loc = free_loc;
	desc->record_size = rec_size;
}

void manage_page_descriptors( int n_count, page_manager *manager, bool increase ){
	if( increase ){
		manager->page_data = (page_desc **)realloc(manager->page_data, n_count*sizeof(page_desc *));
	}else{
		manager->page_data = (page_desc **)malloc(n_count*sizeof(page_desc *));
	}
}
/* 
 * If the page manager has already been written to the disk 
 * then read the information to volatile memeory stored in 
 * the passed pointer.
 */
int read_page_manager(char *db_loc, page_manager *manager ){
	int page_count;
	int last_id;
	int indx = 0;
	FILE* fp;

	int file_len = strlen(db_loc) + PAGE_MANAGER_FILE_LEN;
	char * manager_file = (char *)malloc(file_len*sizeof(char));
	memset(manager_file, 0, file_len*sizeof(char));
	strcat(manager_file, db_loc);
	strcat(manager_file, PAGE_MANAGER_FILE);

	fp = fopen(manager_file, "rb");
	if( fp == NULL ){
		fprintf(stderr, "ERROR: read_page_manager, invalid page descriptor file %s\n",manager_file);
		return -1;
	}
	fread(&(last_id), sizeof(int), 1, fp);
	fread(&(page_count), sizeof(int), 1, fp);
	init_page_manager( page_count, last_id, manager );
	while( 1 ){
		int id = -1;
		int num_rec = -1;
		int free_loc = -1;
		fread(&(manager->page_data[indx]), sizeof(page_desc),1,fp);

		if(feof(fp) || indx == (page_count-1)){ //read until end of file character or there isn't any more page_desc
			break;
		}
		indx++;
	}

	free( manager_file );
	fclose( fp );
	return 0;
}

/*
 * Write the contents of the page_manager structure to the
 * disk.
 */
int write_page_manager(char *db_loc, page_manager *manager ){
	FILE* fp;

	int file_len = strlen(db_loc) + PAGE_MANAGER_FILE_LEN;
	char * manager_file = (char *)malloc(file_len*sizeof(char));
	memset(manager_file, 0, file_len*sizeof(char));
	strcat(manager_file, db_loc);
	strcat(manager_file, PAGE_MANAGER_FILE);

	fp = fopen(manager_file, "rb");
	if( fp == NULL ){
		fprintf(stderr, "ERROR: write_page_manager, invalid page descriptor file %s\n",manager_file);
		return -1;
	}
	fwrite(&(manager->last_id), sizeof(int), 1, fp);
	fwrite(&(manager->num_of_pages), sizeof(int), 1, fp);
	for( int i = 0; i < manager->num_of_pages; i++){
		fwrite(&(manager->page_data),sizeof(page_desc),1,fp);
	}

	free( manager_file );
	fclose( fp );
	return 0;
}

/*
 * Update the given page in the passed page manager with the new 
 * record count and free location. Increments the record count as
 * well as increases the size of the record_sizes array to accomodate
 * the new record.
 * Return 0 with success and -1 otherwise.
 */
int update_page(int page_id, int free_loc, int rec_size, page_manager *manager ){
	page_desc *tmp = get_page_desc(page_id, manager);
	tmp->record_size = rec_size;
	tmp->free_loc = free_loc;
	tmp->num_records++;
}

/*
 * Loop through the page manager in search of the given page_id
 * and if found return the struct pointer otherwise return null.
 */
page_desc* get_page_desc( int page_id, page_manager *manager ){
	for( int i = 0; i < manager->num_of_pages; i++){
		if( (manager->page_data[i])->page_id == page_id ){
			return manager->page_data[i];
		}
	}
	return NULL;
}

/*
 * Free the allocated memory for the provided page_manager.
 */
void free_page_manager( page_manager *manager ){
	for(int i = 0; i<manager->num_of_pages; i++){
		free( (manager->page_data[i]) );
	}
	free( manager->page_data );
}


/*
 * Loop through the page manager and free the struct pointer
 * for the given page if found.
 */
page_manager* delete_page(int page_id, page_manager *manager ){
	page_manager* new_manager = (page_manager *)malloc(sizeof(page_manager));
	init_page_manager(manager->num_of_pages-1, manager->last_id, new_manager);
	int j = 0;
	for( int i = 0; i < manager->num_of_pages; i++){
		if( (manager->page_data[i])->page_id != page_id ){
			(new_manager->page_data[j])->page_id = (manager->page_data[i])->page_id;
			(new_manager->page_data[j])->num_records = (manager->page_data[i])->num_records;
			(new_manager->page_data[j])->free_loc = (manager->page_data[i])->free_loc;
			(new_manager->page_data[j])->record_size = (manager->page_data[i])->record_size;
			j++;
		}
	}
	free_page_manager( manager );
	return new_manager;
}

/*
 * Add a new page to the page manager with an empty record array.
 * Return 0 for success and -1 o.w.
 */
int add_page( int page_id, page_manager *manager ){
	if( get_page_desc(page_id,manager) != NULL ){ // if page is already in manager return -1
		return -1; 
	}
	manage_page_descriptors( manager->num_of_pages+1, manager, true );
	init_page_desc( page_id, 0, 0, 0, manager->page_data[manager->num_of_pages] );
	manager->num_of_pages++;
	manager->last_id++;
	return 0;
}

/*
 * Based on the page descriptor information for the provided page
 * calculate the allowed amount of records that should be in the 
 * page and if the page is less than the value by 1 or more then
 * return number of record_items available, otherwise 0.
 */
int is_page_full( page_desc* desc,int page_size ){
	int total = 0;
	int left = page_size;
	int record_size = 256;
	total += desc->num_records * record_size;
	left -= total;
	total = (int)floor( left / record_size );
	return total;
}

