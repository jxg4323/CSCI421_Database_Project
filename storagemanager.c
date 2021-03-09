#include "storagemanager.h"
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <time.h>

//struct to hold metadata about a table
struct table_data{
    int table_num;
    int table_size;
	int tuples_per_page;
    int num_attr;
    int num_key_attr;
    int num_pages;
    int * attr_types;
    int * key_indices;
    int * pages;
};

//struct to hold metadata/data about a page
struct page_data{
	int page_id;
	int num_tuples;
	int num_attr;
	int tuples_per_page;
	time_t timestamp;
	union record_item ** data;
};

// array to hold all table metadata
static struct table_data ** table_data = NULL;
static int num_tables = 0;

//metadata about database
static int db_page_size;
static int db_buffer_size;
static char * db_db_loc;

static int next_page = 0;

//page buffer
static struct page_data ** page_buffer = NULL;

//static private functions

/**
 * Will read a page from hardware or the buffer.
 * @param p_out - output param for the page
 * @param page_id - the id of the page to read
 * @param t_data - the metadata for the table this page belongs to
 * @return the number of tuples in the page, -1 on error
 **/
static int read_page( struct page_data ** p_out,
                      int page_id, 
					  struct table_data * t_data);

/**
 * Will write a page to hardware.
 * @param page - the data to write out
 * @param page_id - the id of the page to read
 * @param num_record - the number of records in the page
 * @param tuples_per_page - the max number of records in the page (empty included)
 * @param record_size - the number of attributes in a record
 * @return the number of records written to the page; -1 on error
 **/					  
static int write_page( union record_item** page, 
                       int page_id, int num_records, 
					   int tuples_per_page, int record_size);

/**
  * gets the next avaiable table number. Allows for reuse of numbers.
  * @return the next table number
  **/  
static int get_table_num();

/**
  * Will match two keys to see if they match
  * @param key_attr_types - the types of the attributes that make up the key
  * @param key - the key we are trying to match
  * @param key_values - the key we are trying to match to
  * @param key_indices_size - the size of the keys
  * @return 1 if they match; 0 otherwise
  **/
static int key_match(int * key_attr_types, union record_item * key, 
                     union record_item * key_values, int key_indices_size);

/**
  * Will create an empty page for the provided table
  * @param t_data - the metadat of the table to create the page for
  * @return the new page metadata/data
**/  
static struct page_data * create_new_page(struct table_data * t_data);

/**
  * Creates a generic table
  * @param table - the output param to return the created table
  * @param num_records - the max number of records this table can hold
  * @param num_attr - the number of attributes in a reocrd
  **/
static void create_empty_table(union record_item*** table, 
                               int num_records, int num_attr);

/**
  * This will compare records of the same table by key ordering
  * @param t_data - the table metadata that the records belong to
  * @param r1 - the first record to compare
  * @param r2 - the second record to compare
  * @return < 0 if record 1 comes before record 2
  *         > 0 if record 1 comes after record 2
  *         0 otherwise
  **/
static int compare_record(struct table_data * t_data, 
                          union record_item * r1, union record_item * r2);

/**
  * Will attempt to insert the provided record into the requested page.
  * Ordering will be based on key; duplicate keys are rejected.
  * If there is room in the page it will place the record in the proper ordering.
  * If there is not room in the page, it will split the page.
  * @param page_id - the if of the page to insert the record into
  * @param record - the record to insert into the page
  * @param t_data - the tabkle metadata for the page
  * @return 0 on success; -1 on failure
  **/
static int insert_into_page(int page_id, union record_item * record, 
                            struct table_data * t_data);

/**
  * Will delete the ruquested page
  * @param page_id - the iod of the page to delete
  **/  
static void delete_page(int page_id);

/**
  * Will write the metadata to hardware that is needed to restart the database
  * @return 0 on success; -1 on failure
  **/
static int write_metadata();

/**
  * Will read the metadata from hardware that is needed to restart the database
  * @return 0 on success; -1 on failure
  **/
static int read_metadata();

/**
  * Will write table metadata to hardware that is needed to restart the database
  **/
static void write_table_metadata(struct table_data * t_data, FILE * meta_file);

/**
  * Will read table metadata from hardware that is needed to restart the database
  **/
static void read_table_metadata(FILE * meta_file);

// header functions
int create_database( char * db_loc, int page_size, int buffer_size, bool restart){
	if(restart)
		return restart_database( db_loc );
	else
		return new_database( db_loc, page_size, buffer_size );
}

int restart_database( char * db_loc ){
	db_db_loc = malloc(strlen(db_loc) + 1);
	strcpy(db_db_loc, db_loc);
	if(read_metadata() != 0){
		fprintf(stderr, "Failed to restart database\n");
		return -1;
	}
	printf("Here1\n");
	page_buffer = malloc(sizeof(struct page_data *) * db_buffer_size);
	for(int i = 0; i < db_buffer_size; i++){
		page_buffer[i] = NULL;
	}
	return 0;
}

int new_database( char * db_loc, int page_size, int buffer_size ){
	db_page_size = page_size;
	db_buffer_size = buffer_size;
	db_db_loc = malloc(strlen(db_loc) + 1);
	strcpy(db_db_loc, db_loc);
	page_buffer = malloc(sizeof(struct page_data *) * buffer_size);
	for(int i = 0; i < buffer_size; i++){
		page_buffer[i] = NULL;
	}
	return 0;
}

int get_records( int table_id, union record_item *** table ){
    if(table_id >= num_tables){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    struct table_data * t_data = table_data[table_id];
    if(t_data == NULL){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    if(t_data->table_size == 0){
        *table = NULL;
        return 0;
    }

	fflush(NULL);
	*table = malloc(sizeof(union record_item**) * t_data->table_size);
	
    int index = 0;
    for(int i = 0; i < t_data->num_pages; i++){
        struct page_data * p_data = NULL;
        int num_records = read_page(&p_data, t_data->pages[i], t_data);
        
        if(num_records == -1){
            fprintf(stderr, "Error reading page: %d\n", i);
            return -1;
        }
        
        for(int j = 0; j < num_records; j++){
            (*table)[index] = malloc(sizeof(union record_item) * t_data->num_attr);
            memcpy((*table)[index], p_data->data[j], 
			       sizeof(union record_item) * t_data->num_attr);
			index++;
        }
    }
	
	return t_data->table_size;
}

int get_record( int table_id, 
                union record_item * key_values, 
                union record_item ** data ){
    if(table_id >= num_tables){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    struct table_data * t_data = table_data[table_id];
    if(t_data == NULL){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    if(t_data->table_size == 0){
        *data = NULL;
        return -1;
    }
    *data = NULL;
    
    for(int i = 0; i < t_data->num_pages; i++){
        struct page_data * p_data = NULL;
        int num_records = read_page(&p_data, t_data->pages[i], t_data);
        
        if(num_records == -1){
            fprintf(stderr, "Error reading page: %d\n", i);
            return -1;
        }
        
        for(int j = 0; j < num_records; j++){
            union record_item * temp = malloc(sizeof(union record_item) * t_data->num_attr);
            for(int k = 0; k < t_data->num_key_attr; k++){
				int index = t_data->key_indices[k];
                temp[index] = key_values[k];
            }
            if(!compare_record(t_data, temp, p_data->data[j])){
                (*data) = malloc(sizeof(union record_item) * t_data->num_attr);
                memcpy(*data, p_data->data[j], 
				       sizeof(union record_item) * t_data->num_attr);
                return 0;
            }
        }
    }
    fprintf(stderr, "No such record");
    return -1;
}

int insert_record( int table_id, union record_item * record ){
	if(table_id >= num_tables){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    struct table_data * t_data = table_data[table_id];
    if(t_data == NULL){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
	
	if(t_data->table_size == 0){
		struct page_data * p_data = create_new_page(t_data);
		memcpy(p_data->data[0], record, 
		       sizeof(union record_item)*t_data->num_attr);
		
		int page_num = p_data->page_id;
		p_data->num_tuples++;
		t_data->pages = malloc(sizeof(int));
		t_data->pages[0] = page_num;
		t_data->num_pages++;
		t_data->table_size++;
		
		return 0;
    }
	
	for(int i = 0; i < t_data->num_pages; i++){
		struct page_data * p_data = NULL;
        int num_records = read_page(&p_data, t_data->pages[i], t_data);
		
		if(num_records < 0){
			fprintf(stderr, "Error reading page\n");
			return -1;
		}
		
		int rs = compare_record(t_data, record, p_data->data[num_records-1]);
		if(rs < 0){
			return insert_into_page(t_data->pages[i], record, t_data);
		}	
	}
	
	return insert_into_page(t_data->pages[t_data->num_pages-1], record, t_data);
}
	

int update_record( int table_id, union record_item * record ){
    if(table_id >= num_tables){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    struct table_data * t_data = table_data[table_id];
    if(t_data == NULL){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    if(t_data->table_size == 0){
        return -1;
    }
    
    for(int i = 0; i < t_data->num_pages; i++){
        struct page_data * p_data = NULL;
        int num_records = read_page(&p_data, t_data->pages[i], t_data);
        
        if(num_records == -1){
            fprintf(stderr, "Error reading page: %d\n", i);
            return -1;
        }
        
        for(int j = 0; j < num_records; j++){
            if(!compare_record(t_data, record, p_data->data[j])){
                memcpy(p_data->data[j], 
				       record, sizeof(union record_item) * t_data->num_attr);
                return 1;
            }
        }
    }
    fprintf(stderr, "No such record");
    return 0;
}
    
int remove_record( int table_id, union record_item * key_values ){
    if(table_id >= num_tables){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    struct table_data * t_data = table_data[table_id];
    if(t_data == NULL){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    if(t_data->table_size == 0){
        return -1;
    }
    
    for(int i = 0; i < t_data->num_pages; i++){
        struct page_data * p_data = NULL;
        int num_records = read_page(&p_data, t_data->pages[i], t_data);
        
        if(num_records == -1){
            fprintf(stderr, "Error reading page: %d\n", i);
            return -1;
        }
        
        for(int j = 0; j < num_records; j++){
            union record_item * key = malloc(sizeof(union record_item) * t_data->num_key_attr);
            int * key_attr_types = malloc(sizeof(int) * t_data->num_key_attr);
            for(int k = 0; k < t_data->num_key_attr; k++){
                key[k] = p_data->data[j][t_data->key_indices[k]];
                key_attr_types[k] = t_data->attr_types[t_data->key_indices[k]];
            }
            if(key_match(key_attr_types, key, key_values, t_data->num_key_attr)){
				if(num_records == 1){
					delete_page(t_data->pages[i]);
					for(int k = i; k < t_data->num_pages; k++){
						t_data->pages[k] = t_data->pages[k+1];
					}
					t_data->pages = realloc(t_data->pages, t_data->num_pages);
					t_data->num_pages--;
				}
				else{
					for(int k = j; k < t_data->tuples_per_page-1; k++){
						memcpy(p_data->data[k], p_data->data[k+1], 
						       sizeof(union record_item) * t_data->num_attr);
					}
				}
				t_data->table_size--;
                return 0;
            }
        }
    }
    fprintf(stderr, "No such record");
    return -1;
}

int add_table( int * data_types, int * key_indices, 
               int data_types_size, int key_indices_size ){
    int table_num = get_table_num();
    
    struct table_data * t_data = malloc(sizeof(struct table_data));
    t_data->table_num = table_num;
    t_data->table_size = 0;
	t_data->tuples_per_page = db_page_size/(data_types_size*sizeof(union record_item));
    t_data->num_attr = data_types_size;
    t_data->num_key_attr = key_indices_size;
    t_data->num_pages = 0;
    t_data->attr_types = malloc(sizeof(int)*data_types_size);
    memcpy(t_data->attr_types, data_types, data_types_size * sizeof(int));
    t_data->key_indices = malloc(sizeof(int)*key_indices_size);
    memcpy(t_data->key_indices, key_indices, key_indices_size * sizeof(int));
    t_data->pages = NULL;
    
    table_data[table_num] = t_data;
    return table_num;
}

int drop_table( int table_id ){
	if(table_id >= num_tables){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    struct table_data * t_data = table_data[table_id];
    if(t_data == NULL){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
	int rs = clear_table(table_id);
	if(rs != 0){
		fprintf(stderr, "Failed to drop table\n");
		return -1;
	}
	table_data[table_id] = NULL;
	return 0;
}

int clear_table( int table_id ){
	if(table_id >= num_tables){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
    struct table_data * t_data = table_data[table_id];
    if(t_data == NULL){
        fprintf(stderr, "Invalid table number: %d\n", table_id);
        return -1;
    }
	
	if(purge_buffer() != 0){
		return -1;
	}
	
	for(int i = 0; i < t_data->num_pages; i++){
		int page_id = t_data->pages[i];
		int length = snprintf(NULL, 0, "%s%d", db_db_loc, page_id);
		char * page_loc = malloc(length+1);
		snprintf(page_loc, length+1, "%s%d", db_db_loc, page_id);
		int rs = remove(page_loc);
		if(rs == -1){
			fprintf(stderr, "Error removing page %d\n", t_data->pages[i]);
			return -1;
		}
	}
	
	t_data->pages = NULL;
	t_data->num_pages = 0;
	t_data->table_size = 0;
	
	return 0;
}

int purge_buffer(){
	for(int i = 0; i < db_buffer_size; i++){
		struct page_data * page = page_buffer[i];
		if(page != NULL){
			if(write_page(page->data, page->page_id, 
			              page->num_tuples, 
						  page->tuples_per_page, page->num_attr) < 0){
				fprintf(stderr, "Failed to write page from buffer.\n");
				return -1;
			}
			page_buffer[i] = NULL;
		}
	}
	return 0;
}

/*
 * This function will safely shutdown the storage manager.
 * @return 0 on success, -1 on failure.
 */
int terminate_database(){
	if(purge_buffer() != 0){
		fprintf(stderr, 
		        "Storage Manager: terminate database failed to purge buffer.\n");
		return -1;
	}
	if(write_metadata() != 0){
		fprintf(stderr, 
		        "Storage Manager: terminate database failed to write metadata\n");
		return -1;
	}
	return 0;
}

//Helper functions

static int key_match(int * key_attr_types, union record_item * key, 
                     union record_item * key_values, int key_indices_size){
    for(int i = 0; i < key_indices_size; i++){
        switch(key_attr_types[i]){
            case 0:; //int
                if(key[i].i != key_values[i].i)
                    return 0;
				break;
            case 1:; //double
                if(key[i].d != key_values[i].d)
                    return 0;
				break;
            case 2:; //boolean
                if(key[i].b != key_values[i].b)
                    return 0;
				break;
            case 3:; // char
                int rs1 = strcmp(key[i].c, key_values[i].c);
                if (rs1 != 0)
                    return 0;
				break;
            case 4:; // varchar
                int rs2 = strcmp(key[i].v, key_values[i].v);
                if (rs2 != 0)
                    return 0;
				break;
        }
    }
    return 1;
}

static int compare_record(struct table_data * t_data, 
                          union record_item * r1, union record_item * r2){
	for(int j = 0; j < t_data->num_key_attr; j++){
		int i = t_data->key_indices[j];
        switch(t_data->attr_types[i]){
            case 0: //int
                if(r1[i].i < r2[i].i)
                    return -1;
				else if(r1[i].i > r2[i].i)
					return 1;
				break;
            case 1:; //double
                if(r1[i].d < r2[i].d)
                    return -1;
				else if(r1[i].d > r2[i].d)
					return 1;
				break;
            case 2:; //boolean
                if(r1[i].b < r2[i].b)
                    return -1;
				else if(r1[i].b > r2[i].b)
					return 1;
				break;
            case 3:; // char
                int rs1 = strcmp(r1[i].c, r2[i].c);
                if (rs1 < 0)
                    return -1;
				else if(rs1 > 0)
					return 1;
				break;
            case 4:; // varchar
                int rs2 = strcmp(r1[i].v, r2[i].v);
                if (rs2 < 0)
                    return -1;
				else if(rs2 > 0)
					return 1;
				break;
        }
    }  
    return 0;
}

static int get_table_num(){
    if(num_tables == 0){
        table_data = malloc(sizeof(struct table_data *));
		num_tables++;
        return 0;
    }
    
    for(int i = 0; i < num_tables; i++){
        if(table_data[i] == NULL){
            return i;
        }
    }
    
    table_data = realloc(table_data, 
	                     sizeof(struct table_data *) * (num_tables + 1));
    num_tables++;
    return num_tables-1;
}

static int read_page( struct page_data ** p_out, int page_id, 
                      struct table_data * t_data){
	//check buffer
	for(int i = 0; i < db_buffer_size; i++){
		if(page_buffer[i] != NULL && page_buffer[i]->page_id == page_id){
			*p_out = page_buffer[i];
			//update the timestamp since it was just used
			page_buffer[i]->timestamp = time(NULL);
			return page_buffer[i]->num_tuples;
		}
	}
	
	//read in page
	int length = snprintf(NULL, 0, "%s%d", db_db_loc, page_id);
	char * page_loc = malloc(length+1);
	snprintf(page_loc, length+1, "%s%d", db_db_loc, page_id);
	
	FILE * page_file = fopen(page_loc, "rb");

	int num_records;
	fread(&num_records, sizeof(int), 1, page_file);

	int record_size;
	fread(&record_size, sizeof(int), 1, page_file);
	
	union record_item** page = NULL;
	create_empty_table(&page, t_data->tuples_per_page, t_data->num_attr);
		
	for(int i = 0; i < t_data->tuples_per_page; i++){
		fread(&(page[i][0]), sizeof(union record_item), record_size, page_file);
	}
	
	fclose(page_file);
	free(page_loc);
	
	struct page_data * p_data = malloc(sizeof(struct page_data));
	p_data->page_id = page_id;
	p_data->num_tuples = num_records;
	p_data->num_attr = record_size;
	p_data->tuples_per_page = t_data->tuples_per_page;
	p_data->timestamp = time(NULL);
	p_data->data = page;
	*p_out = p_data;
	
	for(int i = 0; i < db_buffer_size; i++){
		if(page_buffer[i] == NULL){
			page_buffer[i] = p_data;
			return num_records;
		}
	}
	
	time_t timestamp = page_buffer[0]->timestamp;
	int index = 0;
	for(int i = 1; i < db_buffer_size; i++){
		if(difftime(page_buffer[i]->timestamp, timestamp) < 0){
			timestamp = page_buffer[i]->timestamp;
			index = i;
		}
	}
	
	struct page_data * w_page = page_buffer[index];
	if(write_page(w_page->data, w_page->page_id, w_page->num_tuples, 
	              w_page->tuples_per_page, w_page->num_attr) >= 0){
		fprintf(stderr, "Failed to write page from buffer.\n");
		return -1;
	}
	
	page_buffer[index] = p_data;
	return num_records;
}

static int write_page( union record_item** page, int page_id, 
                       int num_records, int tuples_per_page, int record_size){
	int length = snprintf(NULL, 0, "%s%d", db_db_loc, page_id);
	char * page_loc = malloc(length+1);
	snprintf(page_loc, length+1, "%s%d", db_db_loc, page_id);
	FILE * page_file = fopen(page_loc, "wb");

	fwrite(&num_records, sizeof(int), 1, page_file);
	
	fwrite(&record_size, sizeof(int), 1, page_file);
	
	for(int i = 0; i < tuples_per_page; i++){
		for(int j = 0; j < record_size; j++){
			fwrite(&(page[i][j]), sizeof(union record_item), 1, page_file);
		}
	}
	
	fclose(page_file);
	free(page_loc);
	
	return num_records;
}

static void create_empty_table(union record_item*** table, 
                               int num_records, int num_attr){
	*table = malloc(sizeof(union record_item*) * (num_records+1));
	for(int i = 0; i < num_records; i++)
		(*table)[i] = malloc(sizeof(union record_item) * num_attr);
}

static struct page_data * create_new_page(struct table_data * t_data){
	//populate the page with its metadata
	struct page_data * p_data = malloc(sizeof(struct page_data));
	p_data->page_id = next_page++;
	p_data->num_tuples = 0;
	p_data->num_attr = t_data->num_attr;
	p_data->tuples_per_page = t_data->tuples_per_page;
	p_data->timestamp = time(NULL);
	
	//create the empty data section of the page
	p_data->data = malloc(sizeof(union record_item*) * (t_data->tuples_per_page+1));
	for(int i = 0; i < t_data->tuples_per_page; i++)
		p_data->data[i] = malloc(sizeof(union record_item) * p_data->num_attr);
	
	
	//look fopr a free spot in the buffer to add the page
	for(int i = 0; i < db_buffer_size; i++){
		if(page_buffer[i] == NULL){
			page_buffer[i] = p_data;
			return p_data;
		}
	}
	
	//if no free spot make a free spot by looking for the oldest page.
	time_t timestamp = page_buffer[0]->timestamp;
	int index = 0;
	for(int i = 1; i < db_buffer_size; i++){
		if(difftime(page_buffer[i]->timestamp, timestamp) < 0){
			timestamp = page_buffer[i]->timestamp;
			index = i;
		}
	}
	
	//write out the oldest page
	struct page_data * w_page = page_buffer[index];
	if(write_page(w_page->data, w_page->page_id, w_page->num_tuples, 
	              w_page->tuples_per_page, w_page->num_attr) != 0){
		fprintf(stderr, "Failed to write page from buffer.\n");
		return NULL;
	}
	
	page_buffer[index] = p_data;
	return p_data;
}

static int insert_into_page(int page_id, 
                            union record_item * record, 
							struct table_data * t_data){
	struct page_data * p_data = NULL;
	int num_records = read_page(&p_data, page_id, t_data);
	//check for record with same key 
	for(int i = 0; i < num_records; i++){
		int rs = compare_record(t_data, record, p_data->data[i]);
		if(rs == 0){
			fprintf(stderr, 
			        "Error inserting record: record with key exists\n");
			return -1;
		}
	}

    // there is room in the page
	if(num_records < t_data->tuples_per_page){
		for(int i = 0; i < num_records; i++){
			int rs = compare_record(t_data, record, p_data->data[i]);
			if(rs == 0){
				fprintf(stderr, 
				        "Error inserting record: record with key exists\n");
				return -1;
			}
			else if(rs < 0){
				for(int j = t_data->tuples_per_page-1; j > i; j--){
					memcpy(p_data->data[j], p_data->data[j-1], 
					       sizeof(union record_item) * t_data->num_attr);
				}
				memcpy(p_data->data[i], record, 
				       sizeof(union record_item) * t_data->num_attr);
				t_data->table_size++;
				p_data->num_tuples++;
				return 0;
			}
		}
		memcpy(p_data->data[num_records], record, 
		       sizeof(union record_item) * t_data->num_attr);
		t_data->table_size++;
		p_data->num_tuples++;
		return 0;
	}
	else{ // split page
	    //find the index in the table pages array of the page we are splitting
		int page_index;
		for(int i = 0; i < t_data->num_pages; i++){
			if(t_data->pages[i] == page_id){
				page_index = i;
				break;
			}
		}
		
		//determine how many go in the first page
		int half = ceil((t_data->tuples_per_page+1)/2);
		
		//make two new pages
		struct page_data * p_data1 = create_new_page(t_data);
		struct page_data * p_data2 = create_new_page(t_data);
		int p1_count = 0;
		int p2_count = 0;

		//makes the pages array one bigger to hold the new page id
		t_data->pages = realloc(t_data->pages, 
		                        sizeof(int) * (t_data->num_pages + 1));
		
		//if only one page in the table, replace page index 0 and add page index 1
		if(t_data->num_pages == 1){
			t_data->pages[0] = p_data1->page_id;
			t_data->pages[1] = p_data2->page_id;
			t_data->num_pages++;
		}
		//if page to be split is last page
		else if(page_index == t_data->num_pages-1){
			t_data->pages[t_data->num_pages-1] = p_data1->page_id;
			t_data->pages[t_data->num_pages] = p_data2->page_id;
			t_data->num_pages++;
		}
		//must be a middle page
		else{
			for(int i = t_data->num_pages; i > page_index; i--){
				t_data->pages[i] = t_data->pages[i-1];
			}
			t_data->pages[page_index] = p_data1->page_id;
			t_data->pages[page_index+1] = p_data2->page_id;
			t_data->num_pages++;
		}
		
		int found = 0;
		//copy first half of the data into first new page
		int copied = 0;
		for(int i = 0; i < half; i++){
			if(!found && compare_record(t_data, record, p_data->data[i]) < 0){
				memcpy(p_data1->data[p1_count], record, 
			       sizeof(union record_item) * t_data->num_attr);
			    found = 1;
				p1_count++;
				if(p1_count == half)
					break;
			}
			memcpy(p_data1->data[p1_count], p_data->data[i], 
			       sizeof(union record_item) * t_data->num_attr);
			p1_count++;
			copied++;
		}
		//copy second half of data into second new page
		for(int i = copied; i < t_data->tuples_per_page; i++){
			if(!found && compare_record(t_data, record, p_data->data[i]) < 0){
				memcpy(p_data2->data[p2_count], record, 
			       sizeof(union record_item) * t_data->num_attr);
			    found = 1;
				p2_count++;
			}
			memcpy(p_data2->data[p2_count], p_data->data[i], 
			       sizeof(union record_item) * t_data->num_attr);
			p2_count++;
		}
		
		if(!found){
			memcpy(p_data2->data[p2_count], record, 
			       sizeof(union record_item) * t_data->num_attr);
			p2_count++;
		}
		
		p_data1->num_tuples = p1_count;
		p_data2->num_tuples = p2_count;
		delete_page(page_id);
		t_data->table_size++;
	}
	return 0;
}

static void delete_page(int page_id){
	//look for page in buffer
	for(int i = 0; i < db_buffer_size; i++){
		if(page_buffer[i] != NULL && page_buffer[i]->page_id == page_id){
			page_buffer[i] = NULL;
		}
	}
	//try to delete the page from hardware... okay if it fails due to missing file
	int length = snprintf(NULL, 0, "%s%d", db_db_loc, page_id);
	char * page_loc = malloc(length+1);
	snprintf(page_loc, length+1, "%s%d", db_db_loc, page_id);
	remove(page_loc);
}	

static void write_table_metadata(struct table_data * t_data, FILE * meta_file){
	//write data
	fwrite(&(t_data->table_num), sizeof(int), 1, meta_file);
	fwrite(&(t_data->table_size), sizeof(int), 1, meta_file);
	fwrite(&(t_data->tuples_per_page), sizeof(int), 1, meta_file);
	fwrite(&(t_data->num_attr), sizeof(int), 1, meta_file);
	fwrite(&(t_data->num_key_attr), sizeof(int), 1, meta_file);
	fwrite(&(t_data->num_pages), sizeof(int), 1, meta_file);
	
	//write out attr types
	for(int i = 0; i < t_data->num_attr; i++)
		fwrite(&(t_data->attr_types[i]), sizeof(int), 1, meta_file);
	
	//write out key indicies
	for(int i = 0; i < t_data->num_key_attr; i++)
		fwrite(&(t_data->key_indices[i]), sizeof(int), 1, meta_file);
	
	//write out page numbers
	for(int i = 0; i < t_data->num_pages; i++)
		fwrite(&(t_data->pages[i]), sizeof(int), 1, meta_file);
}

	
static int write_metadata(){ 
	int length = snprintf(NULL, 0, "%smetadata.dat", db_db_loc);
	char * meta_loc = malloc(length+1);
	snprintf(meta_loc, length+1, "%smetadata.data", db_db_loc);
	
	//write page size and buffer size
	FILE * meta_file = fopen(meta_loc, "wb");

	fwrite(&db_page_size, sizeof(int), 1, meta_file);
	fwrite(&db_buffer_size, sizeof(int), 1, meta_file);
	
	//write out number of tables
	fwrite(&num_tables, sizeof(int), 1, meta_file);
	
	//write out table metadata
	for(int i = 0; i < num_tables; i++){
		if(table_data[i] != NULL)
			write_table_metadata(table_data[i], meta_file);
	}
	return 0; 
}

static void read_table_metadata(FILE * meta_file){
	struct table_data * t_data = malloc(sizeof(struct table_data));
	//read table data
	fread(&(t_data->table_num), sizeof(int), 1, meta_file);
	fread(&(t_data->table_size), sizeof(int), 1, meta_file);
	fread(&(t_data->tuples_per_page), sizeof(int), 1, meta_file);
	fread(&(t_data->num_attr), sizeof(int), 1, meta_file);
	fread(&(t_data->num_key_attr), sizeof(int), 1, meta_file);
	fread(&(t_data->num_pages), sizeof(int), 1, meta_file);
	
	//read in attr types
	t_data->attr_types = malloc(sizeof(int) * t_data->num_attr);
	for(int i = 0; i < t_data->num_attr; i++)
		fread(&(t_data->attr_types[i]), sizeof(int), 1, meta_file);
	
	//read in key indicies
	t_data->key_indices = malloc(sizeof(int) * t_data->num_key_attr);
	for(int i = 0; i < t_data->num_key_attr; i++)
		fread(&(t_data->key_indices[i]), sizeof(int), 1, meta_file);
	
	//read in page numbers
	t_data->pages = malloc(sizeof(int) * t_data->num_pages);
	for(int i = 0; i < t_data->num_pages; i++)
		fread(&(t_data->pages[i]), sizeof(int), 1, meta_file);
	
	table_data[t_data->table_num] = t_data;
}

static int read_metadata(){ 
	int length = snprintf(NULL, 0, "%smetadata.dat", db_db_loc);
	char * meta_loc = malloc(length+1);
	snprintf(meta_loc, length+1, "%smetadata.data", db_db_loc);
	
	//read page size and buffer size
	FILE * meta_file = fopen(meta_loc, "rb");

	fread(&db_page_size, sizeof(int), 1, meta_file);
	fread(&db_buffer_size, sizeof(int), 1, meta_file);
	
	//read number of tables
	fread(&num_tables, sizeof(int), 1, meta_file);
	
	//read in table metadata
	table_data = malloc(sizeof(struct table_data *) * num_tables);
	for(int i = 0; i < num_tables; i++){
		table_data[i] = NULL;
		read_table_metadata(meta_file);
	}
	return 0; 
 }	
	