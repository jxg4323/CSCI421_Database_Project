/*
 * Author: Scott C Johnson (scj@cs.rit.edu)
 * Header file for CSCI421 Group Project Storage Manager
 * Outlines the public functionality for the storage manager
 */
 
#ifndef STORAGEMANAGER_H
#define STORAGEMANAGER_H

#include <stdbool.h>

/*
 * Used to store attribute values in the database.
 * A tuple in the database will be a 1d array of these unions.
 * A table in the database will be a 2d array of these unions.
 */
union record_item{
	int i;
	double d;
	bool b;
	char c[255];
	char v[255];	
};

/*
 * create or restarts an instance of the database at the 
 * provided database location.
 * If restart is true this function will call the restart_database function.
 * If restart is false this function will call the new database function.
 * @param db_loc - the absolute path for the database.
 * @param page_size - the pages size for the database.
 * @param buffer_size - the maximum number of pages the 
 *                      database can hold in its page buffer at one time. 
 * @param restart - if true it will attempt to restart the database at the
 *                  provided database location. 
 * @return the result of either the restart_database or new_database function calls.
 */
int create_database( char * db_loc, int page_size, int buffer_size, bool restart);

/*
 * This function will try to restart the database at the provided database location.
 * @param db_loc - the absolute path for the database to restart.
 * @return 0 if the database is restarted successfully, otherwise -1;
 */
int restart_database( char * db_loc );

/*
 * create an instance of the database at the provided database location.
 * @param db_loc - the absolute path for the database.
 * @param page_size - the pages size for the database.
 * @param buffer_size - the maximum number of pages the 
 *                      database can hold in its page buffer at one time.
 * @return 0 if the database is started successfully, otherwise -1;
 */
int new_database( char * db_loc, int page_size, int buffer_size );

/*
 * Returns all of the records in the table with the provided id.
 * The records must be in order that they are stored on hardware.
 * The user of this function is responsible for freeing the data.
 * @param table_id - the id of the table to get the records for.
 * @param table - a 2d array of record_item (output variable)
 *                this will be used to output the values in the table.
                  This will be a pointer to the first item in the 2d array.
 * @return the number of records in the output, -1 upon error
 */
int get_records( int table_id, union record_item *** table );

/*
 * Returns all of the records in the page with the provided id.
 * The records must be in order that they are stored on hardware.
 * The user of this function is responsible for freeing the data.
 * @param page_id - the id of the page to get the records for.
 * @param page - a 2d array of record_item (output variable)
 *                this will be used to output the values in the page.
                  This will be a pointer to the first item in the 2d array.
 * @return the number of records in the output, -1 upon error
 */
int get_page( int page_id, union record_item *** page );

/*
 * This function will return a record from the table with the provided
 * id that matched the key_values provided.
 * @param table_id - the id of the table to find the record in.
 * @param key_values - an array of record_items that make up the key of the
                       record to be found. The order of the values matches
					   the primary key indices order of the table.
 * @param data - a pointer to an array of record_item values that represent the tuple matching
                 the key values provided. (output parameter)
				 The user is responsibile for freeing this.
 * @return 0 if successfull, -1 otherwise
 */
int get_record( int table_id, union record_item * key_values, union record_item ** data );

/*
 * Inserts the provided record into the table with the provided id.
 * Records must be inserted in primary key order.
 * Two records in a table cannot have the same primary key or unique contranints.
 * @param table_id - the id of the table to insert the record into.
 * @param record - the record to insert into the table. 
 * @return 0 if sucessfully inserted, -1 otherwise
 */
int insert_record( int table_id, union record_item * record );

/*
 * Updates the provided record into the table with the provided id.
 * Primary key of a record will never be updated.
 * Two records in a table cannot have the same primary key or unique contranints.
 * @param table_id - the id of the table to update the record in.
 * @param record - the record to update in the table. 
 * @return 0 if sucessfully updated, -1 otherwise
 */
int update_record( int table_id, union record_item * record );

/*
 * Removes the provided record from the table with the provided id.
 * @param table_id - the id of the table to remove the record from.
 * @param key_values - an array of record_items that make up the key of the
                       record to be removed.
 * @return 0 if sucessfully removed, -1 otherwise
 */
int remove_record( int table_id, union record_item * key_values );

/*
 * This function will drop the table with the provided id
 * from the database. This will remove all data as well as information
 * about the table.
 * @param table_id - the id of the table to drop
 * @return 0 if table succesfully dropped, -1 otherwise.
 */
int drop_table( int table_id );

/*
 * This function will clear the table with the provided id
 * from the database. This will remove all data but not the table.
 * @param table_id - the id of the table to clear
 * @return 0 if table succesfully cleared, -1 otherwise.
 */
int clear_table( int table_id );

/*
 * This will add a table to the database with the provided data types and
 * primary key.
 * @param data_types - an integer array representing the data types stored
                       in a tupler in the table.
 * @param key_indices - an interger array representing the indicies that
                        make up the parimary key. The order of the indicies
						in this array deternmine the ordering of the attributes
						in the primary key.
 * @param data_types_size - the size of the data types array
 * @param key_indices_size - the size of the key indicies array.
 * @return the id of the table created, -1 upon error.
 */
int add_table( int * data_types, int * key_indices, int data_types_size, int key_indices_size );

/*
 * This will pruge the page buffer to disk.
 * @return 0 on success, -1 on failure.
 */
int purge_buffer();

/*
 * This function will safely shutdown the storage manager.
 * @return 0 on success, -1 on failure.
 */
int terminate_database();

#endif
