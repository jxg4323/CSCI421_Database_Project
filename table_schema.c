#include "tableschema.h"

/*
 * Allocate memory fo catalogs structure and return the pointer if 
 * success otherwise return null.
 */
catalogs* initialize_catalogs(){
	catalogs* logs = (catalogs *)malloc(sizeof(catalogs));
	logs->table_count = 0;
	return logs;
}
/*
 * Increase size of table_catalogs array or use the original malloc
 * depending on the increase flag. If the increase flag is false, then
 * malloc, otherwise use realloc.
 */
void manage_catalogs(catalogs *logs, int table_count, bool increase){
	if(increase){
		logs->all_tables = (table_catalog *)realloc(logs->all_tables, table_count*sizeof(table_catalog));
		logs->table_count++;
	}else{
		logs->all_tables = (table_catalog *)malloc(table_count*sizeof(table_catalog));
		logs->table_count = table_count;
	}
}

/*
 * Increase size of attribute array in table catalog and increment
 * attribute count in the table catalog.
 */
void manage_attributes(table_catalog* t_cat, int attr_count, bool increase){
	if( increase ){
		t_cat->attributes = (attr_info *)realloc(t_cat->attributes, attr_count*sizeof(attr_info));
		t_cat->attribute_count++;
	}else{
		t_cat->attributes = (attr_info *)malloc(attr_count*sizeof(attr_info));
		t_cat->attribute_count = attr_count;
	}
}

/*
 * Increase size of foreign relations array in table catalog and increment
 * foreign relations count in the table catalog.
 */
void manage_foreign_rels(table_catalog* t_cat, int rel_count, bool increase){
	if( increase ){
		t_cat->relations = (foreign_data *)realloc(t_cat->relations, rel_count*sizeof(foreign_data));
		t_cat->foreign_size++;
	}else{
		t_cat->relations = (foreign_data *)malloc(rel_count*sizeof(foreign_data));
		t_cat->foreign_size = rel_count;
	}
}

/*
 * Increase size of primary array in table catalog and increment
 * primary count in the table catalog.
 */
void manage_prim_tuple(table_catalog *t_cat, int prim_size, bool increase){
	if( increase ){
		t_cat->primary_tuple = (int *)realloc(t_cat->primary_tuple, prim_size*sizeof(int));
		t_cat->primary_size++;
	}else{
		t_cat->primary_tuple = (int *)malloc(prim_size*sizeof(int));
		t_cat->primary_size = prim_size;
	}
}

/*
 * Increase size of unique array in table catalog and increment
 * primary count in the table catalog.
 */
void manage_unique_tuple(table_catalog *t_cat, int size, bool increase){
	if( increase ){
		t_cat->unique_tuples = (unique *)realloc(t_cat->unique_tuples, size*sizeof(unique));
		t_cat->unique_size++;
	}else{
		t_cat->unique_tuples = (unique *)malloc(size*sizeof(unique));
		t_cat->unique_size = size;
	}
}

/*
 * Initialize the table catalog with the provided values and allocate 
 * memory for the array of attributes but don't fill. User is responsible
 * for freeing the table_name pointer after call.
 */
void init_catalog(table_catalog* catalog, int tid, int a_size, int f_size, int p_size, int u_size, char *table_name){
	catalog->id = tid;
	catalog->deleted = false;
	catalog->attribute_count = a_size;
	catalog->foreign_size = f_size;
	catalog->primary_size = p_size;
	catalog->unique_size = u_size;
	catalog->table_name = (char *)malloc(strlen(table_name)*sizeof(char));
	strcpy(catalog->table_name, table_name);
	catalog->attributes = (attr_info*)malloc(a_size*sizeof(attr_info));
	catalog->relations = (foreign_data*)malloc(f_size*sizeof(foreign_data));
	catalog->primary_tuple = (int *)malloc(p_size*sizeof(int));
	catalog->unique_tuples = (unique *)malloc(u_size*sizeof(int));
}

/*
 * Initialize the attribute with the constraints and name provided and 
 * mark the attribute as NOT deleted. 
 */
void init_attribute(attr_info* attr, int type, int notnull, int primkey, int unique, char *name){
	attr->deleted = false;
	attr->type = type;
	attr->name =(char *)malloc(strlen(name)*sizeof(char));
	strcpy(attr->name, name);
	attr->notnull = notnull;
	attr->primarykey = primkey;
	attr->unique = unique;
}

/*
 * Initialize the foreign relations array with the given values and mark 
 * it as NOT deleted. User is responsible for freeing the parameter int
 * arrays.
 */
void init_foreign_relation(foreign_data* fdata, char* name, int fid, int size, int *orig_attrs, int *for_attrs){
	fdata->deleted = false;
	fdata->foreign_table_id = fid;
	fdata->name = (char *)malloc(strlen(name)*sizeof(char));
	strcpy(fdata->name, name);
	fdata->tuple_size = size;
	fdata->orig_attr_locs = (int *)malloc(size*sizeof(int));
	fdata->for_attr_locs = (int *)malloc(size*sizeof(int));
	memcpy(fdata->orig_attr_locs, orig_attrs, size*sizeof(int));
	memcpy(fdata->for_attr_locs, for_attrs, size*sizeof(int));
}

/*
 * Initialize the unique structure with the given values and mark 
 * it as NOT deleted. User is responsible for freeing the parameter 
 * int arrays.
 */
void init_unique_tuple(unique* udata, int tup_size, int *tup){
	udata->deleted = false;
	udata->tup_size = tup_size;
	udata->attr_tuple = (int *)malloc(tup_size*sizeof(int));
	memcpy(udata->attr_tuple, tup, tup_size*sizeof(int));
}

/*
 * Initialize the primary array with the given values and mark 
 * User is responsible for freeing the parameter int arrays.
 */
void init_primary_tuple(table_catalog *t_cat, int *tuple, int size){
	t_cat->primary_size = size;
	t_cat->primary_tuple = (int *)malloc(size*sizeof(int));
	memcpy(t_cat->primary_tuple, tuple, size*sizeof(int));
}

/*
 * Read the catalog information from the disk into memory and if successful
 * return 1 otherwise -1.
 */
int read_catalogs(char *db_loc, catalogs* logs){
	int count = 0;
	int indx = 0;
	FILE* fp;

	int file_len = strlen(db_loc) + TABLE_CATALOG_FILE_LEN;
	char * cat_file = (char *)malloc(file_len*sizeof(char));
	memset(cat_file, 0, file_len*sizeof(char));
	strcat(cat_file, db_loc);
	strcat(cat_file, TABLE_CATALOG_FILE);

	fp = fopen(cat_file, "rb");
	if(fp == NULL){
		fprintf(stderr, "ERROR: read_catalogs, invalid table catalog file %s\n", cat_file);
		return -1;
	}
	fread(&(count), sizeof(int), 1, fp);
	manage_catalogs( logs, count, false ); // assume the table catalogs array hasn't been allocated
	while( 1 ){
		int t_id, a_count, foreign_count, unique_count, prim_count, name_size;
		fread(&t_id, sizeof(int), 1, fp);
		fread(&a_count, sizeof(int), 1, fp);
		fread(&foreign_count, sizeof(int), 1, fp);
		fread(&unique_count, sizeof(int), 1, fp);
		fread(&prim_count, sizeof(int), 1, fp);
		fread(&name_size, sizeof(int), 1, fp);
		char *temp = (char *)malloc((name_size+1)*sizeof(char));
		memset(temp, '\0', (name_size+1)*sizeof(char));
		fread(temp, sizeof(char), name_size, fp);
		init_catalog( &(logs->all_tables[indx]), t_id, a_count, foreign_count, prim_count, unique_count, temp );
		// read attribute info 
		char *a_name;
		for(int i = 0; i < a_count; i++){
			int type, notnull, primkey, unique, rel_count;
			name_size = 0;
			fread(&type, sizeof(int), 1, fp);
			fread(&notnull, sizeof(int), 1, fp);
			fread(&primkey, sizeof(int), 1, fp);
			fread(&unique, sizeof(int), 1, fp);
			fread(&name_size, sizeof(int), 1, fp);
			a_name = (char *)malloc((name_size+1)*sizeof(char)); // TODO: add null character at end
			memset(a_name, '\0', (name_size+1)*sizeof(char));
			fread(a_name, sizeof(char), name_size, fp);
			init_attribute(&(logs->all_tables[indx].attributes[i]), type, notnull, primkey, unique, a_name);
			free(a_name);
		}
		// read foreign relation information
		char *name;
		for(int i = 0; i < foreign_count; i++){
			int fid, size;
			name_size = 0;
			fread(&fid, sizeof(int), 1, fp);
			fread(&size, sizeof(int), 1, fp);
			fread(&name_size, sizeof(int), 1, fp);
			name = (char *)malloc((name_size+1)*sizeof(char));
			memset(name, '\0', (name_size+1)*sizeof(char));
			fread(name, sizeof(char), name_size, fp);
			int *orig_temp = (int *)malloc(size*sizeof(int));
			int *for_temp = (int *)malloc(size*sizeof(int));
			fread(orig_temp, sizeof(int), size, fp);
			fread(for_temp, sizeof(int), size, fp);
			init_foreign_relation( &(logs->all_tables[indx].relations[i]), name, fid, size, orig_temp, for_temp );
			free( name );
			free( orig_temp );
			free( for_temp );
		}
		// read unique tuples
		int *arr;
		for(int i = 0; i < unique_count; i++){
			int size;
			fread(&size, sizeof(int), 1, fp);
			arr = (int *)malloc(size*sizeof(int));
			fread(arr, sizeof(int), size, fp);
			init_unique_tuple( &(logs->all_tables[indx].unique_tuples[i]), size, arr);
			free( arr );
		}
		// read primary key tuple
		int *p_temp = (int *)malloc(prim_count*sizeof(int));
		fread(p_temp, sizeof(int), prim_count, fp);
		init_primary_tuple(&(logs->all_tables[indx]), p_temp, prim_count);

		if(feof(fp) || indx == (count -1)){ // read until end of file or no more tables
			break;
		}
		free(temp);
		indx++;
	}
	free(cat_file);
	fclose(fp);
	return 1;
}

/*
 * Write the catalog information about all of the tables to disk.
 * Return 1 with success and -1 with failure. User is reponsible 
 * for freeing the logs.
 */
int write_catalogs(char *db_loc, catalogs* logs){
	FILE* fp;

	int file_len = strlen(db_loc) + TABLE_CATALOG_FILE_LEN;
	char * cat_file = (char *)malloc(file_len*sizeof(char));
	memset(cat_file, 0, file_len*sizeof(char));
	strcat(cat_file, db_loc);
	strcat(cat_file, TABLE_CATALOG_FILE);

	fp = fopen(cat_file, "wb");
	if(fp == NULL){
		fprintf(stderr, "ERROR: write_catalogs, invalid table catalog file %s\n", cat_file);
		return -1;
	}
	int true_count = get_table_count_no_deletes(logs);
	fwrite(&(true_count), sizeof(int), 1, fp);
	for( int indx = 0; indx < logs->table_count; indx++ ){
		if ( logs->all_tables[indx].deleted ){continue;} // don't write the deleted table info
		int tru_acount, tru_relcount, tru_unicount;
		// get counts of array information without deletes
		tru_acount = get_attribute_count_no_deletes( &(logs->all_tables[indx]) );
		tru_relcount = get_relation_count_no_deletes( &(logs->all_tables[indx]) );
		tru_unicount = get_unique_count_no_deletes( &(logs->all_tables[indx] ));
		int name_size = strlen(logs->all_tables[indx].table_name);
		fwrite(&(logs->all_tables[indx].id), sizeof(int), 1, fp);
		fwrite(&tru_acount, sizeof(int), 1, fp);
		fwrite(&tru_relcount, sizeof(int), 1, fp);
		fwrite(&tru_unicount, sizeof(int), 1, fp);
		fwrite(&(logs->all_tables[indx].primary_size), sizeof(int), 1, fp);
		fwrite(&name_size, sizeof(int), 1, fp);
		fwrite(logs->all_tables[indx].table_name, sizeof(char), name_size, fp);
		// write attribute info 
		for(int i = 0; i < logs->all_tables[indx].attribute_count; i++){
			if( logs->all_tables[indx].attributes[i].deleted ){continue;} // if deleted don't write
			name_size = strlen(logs->all_tables[indx].attributes[i].name);
			fwrite(&(logs->all_tables[indx].attributes[i].type), sizeof(int), 1, fp);
			fwrite(&(logs->all_tables[indx].attributes[i].notnull), sizeof(int), 1, fp);
			fwrite(&(logs->all_tables[indx].attributes[i].primarykey), sizeof(int), 1, fp);
			fwrite(&(logs->all_tables[indx].attributes[i].unique), sizeof(int), 1, fp);
			fwrite(&name_size, sizeof(int), 1, fp);
			fwrite(logs->all_tables[indx].attributes[i].name, sizeof(char), name_size, fp);
		}
		// write foreign relation information
		for(int i = 0; i < logs->all_tables[indx].foreign_size; i++){
			if( logs->all_tables[indx].relations[i].deleted ){continue;}
			name_size = strlen(logs->all_tables[indx].relations[i].name);
			int temp = logs->all_tables[indx].relations[i].tuple_size;
			fwrite(&(logs->all_tables[indx].relations[i].foreign_table_id), sizeof(int), 1, fp);
			fwrite(&(logs->all_tables[indx].relations[i].tuple_size), sizeof(int), 1, fp);
			fwrite(&name_size, sizeof(int), 1, fp);
			fwrite(logs->all_tables[indx].relations[i].name, sizeof(char), name_size, fp);
			fwrite(logs->all_tables[indx].relations[i].orig_attr_locs, sizeof(int), temp, fp);
			fwrite(logs->all_tables[indx].relations[i].for_attr_locs, sizeof(int), temp, fp);
		}
		// write unique tuples
		for(int i = 0; i < logs->all_tables[indx].unique_size; i++){
			if( logs->all_tables[indx].unique_tuples[i].deleted ){continue;}
			int size = logs->all_tables[indx].unique_tuples[i].tup_size;
			fwrite(&(size), sizeof(int), 1, fp); 
			fwrite(logs->all_tables[indx].unique_tuples[i].attr_tuple, sizeof(int), size, fp);
		}
		// write primary key tuple
		fwrite(logs->all_tables[indx].primary_tuple, sizeof(int), logs->all_tables[indx].primary_size, fp);

		if(feof(fp) || indx == (logs->table_count -1)){ // read until end of file or no more tables
			break;
		}
	}
	free(cat_file);
	fclose(fp);
	return 1;
}
/*
 * Create new table catalog with just table name and newly allocated structure
 * Return location of new catalog in array if success otherwise -1.
 */
int new_catalog(catalogs *logs, char *table_name){
	if( check_table_name( logs,table_name ) ){ 
		fprintf(stderr,"ERROR: new_catalog, table_name already in use.\n");
		return -1; 
	}
	int last = logs->table_count;
	manage_catalogs( logs, logs->table_count+1, true );
	init_catalog( &(logs->all_tables[last]),last,0,0,0,0,table_name );
	return last;
}

/*
 * Add attribute information to the given table, and increment the attribute
 * count for the table.
 * @param @constraints: layout of array [<notnull>, <primarykey>, <unique>]
 * Return 1 for success and -1 for failure.
 */
int add_attribute(table_catalog* t_cat, char *attr_name, char *type, int constraints[3]){
	int last = t_cat->attribute_count;
	int type_con = type_conversion(type);
	if ( type_con == -1 ){
		fprintf( stderr, "ERROR: add_attribute, Type not knonw\n" );
		return -1;
	}
	manage_attributes( t_cat, t_cat->attribute_count+1, true );
	init_attribute( &(t_cat->attributes[last]), type_con, constraints[0], constraints[1], constraints[2], attr_name );
	return 1;
}

/*
 * Mark an attribute as deleted in the attribute information matrix. If an 
 * attribute is marked as removed then don't write it to the disk.
 * Check if attribute is apart of foreign key, primary key, or unique tuple.
 * Return 1 for success and -1 for failure.\
 */
int remove_attribute(catalogs* logs, table_catalog* t_cat, char *attr_name){
	int attr_id = get_attr_loc(t_cat, attr_name);
	// confirm attribute exists
	if(attr_id == -1){ 
		fprintf(stderr, "ERROR: Attribute %s doesn't exist\n", attr_name ); 
		return -1; 
	}
	// Check if primary key contains this attribute
	if( check_prim_key(t_cat, attr_id) ){ 
		fprintf(stderr, "ERROR: Attribute: %s is apart of the primary key and CANNOT be removed\n", attr_name);
		return -1;
	}
	int *tmp = (int *)malloc(t_cat->foreign_size*sizeof(int));
	// search through relations array to find if attr_name is apart of a relation & if so delete it
	int num = check_foreign_relations( t_cat, attr_id, tmp );
	for( int i = 0; i<num; i++ ){
		t_cat->relations[tmp[i]].deleted = true;
	}
	free( tmp );
	// loop through all tables to confirm the attribute isn't apart of another foreign relation
	for( int i = 0; i<logs->table_count; i++ ){
		// look at tables relation
		for( int j = 0; j<(logs->all_tables[i].foreign_size); j++ ){
			// if the relation was deleted or the foreign table isn't the same as the provided table continue
			if( logs->all_tables[i].relations[j].deleted || strcmp(t_cat->table_name,logs->all_tables[i].relations[j].name) != 0 ){
				continue;
			}
			int tup_size = logs->all_tables[i].relations[j].tuple_size;
			// loop through attributes
			for( int s = 0; s<tup_size; s++ ){
				// if attributes are the same then mark relation as deleted 
				if(	logs->all_tables[i].relations[j].for_attr_locs[s] == attr_id ){
					logs->all_tables[i].relations[j].deleted = true;
					j = logs->all_tables[i].foreign_size;
					i = logs->table_count;
					break;
				}
			}
		}
	}
	// Check if any unique tuples contiain the attribute
	int result = delete_uniq_tup( t_cat, attr_id );
	// mark attribute as deleted
	t_cat->attributes[attr_id].deleted = true;
	return 1;
}

/*
 * Layout of the foreign_row is: ["foreign_tabe_name", "a_1", "a_2", "r_1", "r_2"]
 * allocate or reallocate memory for the addidtion of the next foreign reference.
 * 
 * Return 1 with success of foreign info addition and -1 otherwise.
 */
int add_foreign_data(catalogs* logs, table_catalog* t_cat, char **foreign_row, int f_key_count){
	int last = t_cat->foreign_size;
	char *f_name = foreign_row[0];
	table_catalog* for_cat = get_catalog_p( logs, f_name );
	if( for_cat == NULL ){
		fprintf(stderr, "Foreign Table: %s doesn't exist.\n", f_name );
		return -1;
	}
	manage_foreign_rels( t_cat, t_cat->foreign_size+1, true );
	int *o_arr = (int *)malloc(f_key_count*sizeof(int));
	int *f_arr = (int *)malloc(f_key_count*sizeof(int));
	int s = (f_key_count);
	int c = 0;
	int t = 0;
	for( int j = 1; j<(2*f_key_count)+1; j++ ){ 
		int tmp = 0;
		if( j <= s ){
			tmp = get_attr_loc( t_cat, foreign_row[j] ); //TODO: add error if attribute doesn't exists
			o_arr[c] = tmp;
			c++;
		}else if( j > s ){
			tmp = get_attr_loc( for_cat, foreign_row[j] );
			f_arr[t] = tmp;
			t++;
		}
	}
	init_foreign_relation( &(t_cat->relations[last]), f_name, for_cat->id, f_key_count, o_arr, f_arr );
	free( o_arr );
	free( f_arr );
	return 1;
}

/*
 * The foreign row is the an array of tokens that are as follows:
 *      ["foreign_tabe_name", "a_1", "a_2", "r_1", "r_2"]
 * Change the deleted marker to 1 without decreasing the size of 
 * foreign relations. 
 * @param f_count: number of attributes in relation, using above example
 * 					f_count would be 2.
 * Return location of foreign_data in array if successful o.w. -1.
 */
int remove_foreign_data(catalogs* logs, table_catalog* t_cat, char**foreign_row, int f_count){
	char *f_name = foreign_row[0];
	table_catalog* for_cat = get_catalog_p( logs, f_name );
	int size = (2*f_count)+1;
	int *arr = (int*)malloc((size-1)*sizeof(int));
	// converts foreign_row to int array
	int s = (f_count);
	for( int j = 1; j<size; j++ ){ 
		int tmp = 0;
		if( j <= s ){
			tmp = get_attr_loc( t_cat, foreign_row[j] );
		}else if( j > s ){
			tmp = get_attr_loc( for_cat, foreign_row[j] );
		}
		arr[j-1] = tmp;
	}
	for( int i = 0; i <t_cat->foreign_size; i++ ){ // compare each foreign relation until equal is found
		if( t_cat->relations[i].deleted || t_cat->relations[i].tuple_size != f_count ){ continue; }
		int count = 0;
		for( int j = 0; j<t_cat->relations[i].tuple_size; j++ ){
			if( arr[i] == t_cat->relations[i].orig_attr_locs[j] ){ count++; }
			if( arr[i] == t_cat->relations[i].for_attr_locs[j] ){ count++; }
		}
		if( count == size-1 && strcmp(f_name, t_cat->relations[i].name) == 0){
			t_cat->relations[i].deleted = true;
			free( arr );
			return i;
		}
	}
	free( arr );
	return -1;
}

/*
 * Loop through the attribute information in the table catalog and
 * apply the primary key constraint to the attributes whose names
 * match those in the @param prim_names. Confirm the attributes aren't
 * already apart of a primary key for the table as well as confirm the
 * table doesn't have a primary key already if so return -1.
 *
 * Return 1 with succesful upate and -1 otherwise.
 */
int add_primary_key(table_catalog* t_cat, char **prim_names, int num_keys){
	// Confirm there isn't already a primary key tuple --> print error msg & return -1
	if( t_cat->primary_size > 0 ){
		fprintf( stderr, "ERROR: there already exists a primary key.\n" );
		return -1;
	}
	int *tmp = (int *)malloc(num_keys*sizeof(int));
	// Find attribute locations of each of provided attributes
	for( int i = 0; i<num_keys; i++ ){
		tmp[i] = get_attr_loc( t_cat, prim_names[i] );
	}
	// Allocate memory for primary key size
	manage_prim_tuple( t_cat, num_keys, false );
	// Copy of attribute locations of each attribute
	init_primary_tuple( t_cat, tmp, num_keys );
	free( tmp );
	return 1;
}

/*
 * Delete the primary key information from the table.
 * Return 1 with success and -1 otherwise.
 */
int remove_primary_key(table_catalog* t_cat){
	// memset the primary key to 0's
	memset( t_cat->primary_tuple, 0, t_cat->primary_size*sizeof(int) );
	// set primary key size ot 0
	t_cat->primary_size = 0;
	// free the pointer
	free( t_cat->primary_tuple );
	return 1;
}

/*
 * Confirm no other unique tuples have the combination of attributes,
 * if so return -1 and print an error. Otherwise return 1.
 */
int add_unique_key(table_catalog* t_cat, char **unique_name, int size){
	int last = t_cat->unique_size;
	int *arr = (int*)malloc((size)*sizeof(int));
	for( int i = 0; i<size; i++ ){ 
		arr[i] = get_attr_loc( t_cat, unique_name[i] );
	}
	for( int i = 0; i<t_cat->unique_size; i++ ){
		int count = 0;
		// if unique tuple is deleted or sizes don't match skip it
		if( t_cat->unique_tuples[i].deleted || t_cat->unique_tuples[i].tup_size != size ){ continue; }
		for( int j = 0; j<t_cat->unique_tuples[i].tup_size; j++ ){
			if( t_cat->unique_tuples[i].attr_tuple[j] == arr[j] ){ count++; }
		}
		if( count == size ){ 
			fprintf(stderr, "ERROR: the unique tuple is already in the table.\n");
			return -1;
		}
	}
	// Create new unique tuple in table
	manage_unique_tuple( t_cat, t_cat->unique_size+1, true);
	init_unique_tuple( &(t_cat->unique_tuples[last]), size, arr );
	free( arr );
	return 1;
}

/*
 * Remove the unique tuple based on the tuple provided, find the tuple
 * that matches and remove it.
 * If successfull return 1 otherwise return -1.
 */
int remove_unique_key(table_catalog* t_cat, char** unique_name, int size){
	// Find locations of attributes in unique_name provided
	int *arr = (int*)malloc((size)*sizeof(int));
	for( int i = 0; i<size; i++ ){ 
		arr[i] = get_attr_loc( t_cat, unique_name[i] );
	}
	// Find tuple that matches it in table
	for( int i = 0; i <t_cat->unique_size; i++ ){
		// ignore deleted and size mismatched tuples
		if(t_cat->unique_tuples[i].deleted || t_cat->unique_tuples[i].tup_size != size ){ continue; }
		// assume mismatch
		int count = 0; 
		for( int j = 0; j<size; j++ ){
			if( arr[j] == t_cat->unique_tuples[i].attr_tuple[j] ){ count++; }
		}
		// if the tuples match mark the unique tuple as deleted and return 1
		if( count == size ){ 
			t_cat->unique_tuples[i].deleted = true;
			free( arr );
			return 1;
		}
	}
	// If for loop concludes w/o return then no matching tuple was found
	fprintf(stderr, "ERROR: provided unique tuple wasn't found in table %s\n", t_cat->table_name );
	free( arr );
	return -1;
}

/*
 * Search through all catalog information to find the one with
 * the same table name as provided, given that all table names
 * are unique.
 * Return index for the table catalog in logs or -1 if there 
 * isn't one. 
 */
int get_catalog(catalogs *logs, char *tname){
	int loc = -1;
	for( int i = 0; i<logs->table_count; i++ ){
		if( strcmp(logs->all_tables[i].table_name, tname) == 0 ){ loc = i; }
	}
	return loc;
}

/*
 * Search through all catalog information to find the one with
 * the same table name as provided, given that all table names
 * are unique.
 * Return pointer to the table catalog or NULL if there isn't one. 
 */
table_catalog* get_catalog_p(catalogs* logs, char* tname){
	for( int i = 0; i<logs->table_count; i++ ){
		if( strcmp(logs->all_tables[i].table_name, tname) == 0 ){ return &(logs->all_tables[i]); }
	}
	return NULL;
}

/*
 * Based on the table id retrieve the attribute location
 * in the table catalog.
 * Return location of attribute in table if found, o.w. -1.
 */
int get_attr_loc(table_catalog *tcat, char *attr_name){
	int loc = -1;
	for( int i = 0; i<tcat->attribute_count; i++ ){
		if( strcmp(tcat->attributes[i].name, attr_name) == 0 ){ loc = i; }
	}
	if( loc == -1 ){
		fprintf(stderr, "ERROR: attribute doesn't exist in %s\n", tcat->table_name);
	}
	return loc;
}

/*
 * Check if the table name is already in use.
 * Return True if a table name is found, false otherwise.
 */
bool check_table_name(catalogs *logs, char *tname){
	bool in_use = false;
	for( int i = 0; i<logs->table_count; i++ ){
		if( logs->all_tables[i].deleted ){ continue; } // deleted tables don't count
		if( strcmp(logs->all_tables[i].table_name, tname) == 0 ){ in_use = true; }
	}
	return in_use;
}

/*
 * Find the attribute listed in the parameters in the table
 * and return the index of the attribute in the array. If
 * the attribute isn't found the return -1.
 */
int find_attr(table_catalog* t_cat, char *attr_name){
	int count = t_cat->attribute_count;
	int loc = -1;
	for( int i = 0; i < count; i++ ){
		if( strcmp(t_cat->attributes[i].name, attr_name) == 0 ){ loc = i; }
	}
	return loc;
}

/*
 * Look through the primary key to see if the primary
 * key contains the provided attribute ID and if so 
 * return true other wise return false indication the 
 * primary key DOES NOT contain the attribute.
 */
bool check_prim_key( table_catalog* tcat, int attr_id ){
	bool result = false; // assume attribute isn't in key tuple
	for( int i = 0; i<tcat->primary_size; i++ ){
		if( tcat->primary_tuple[i] == attr_id ){ 
			result = true; 
			return result;
		}
	}
	return result;
}

/*
 * Search through the foreign relations to find any relations which 
 * contain the provided attribute ID and place the location of the relations
 * in the @param ret_arr (should have allocated num_relations*sizeof(int)) 
 * which assumes all realtions contain the attribute which the user is 
 * responsible for freeing after call. 
 * Return size of the ret_arr if there were nay found and if no relations 
 * contained the attribute then return -1.
 */
int check_foreign_relations( table_catalog* tcat, int attr_id, int *ret_arr ){
	int count = 0; // assume no relations involved
	for( int i = 0; i<tcat->foreign_size; i++ ){
		if( tcat->relations[i].deleted ){ continue; } // ignore deleted foreign relations
		for( int j = 0; j<tcat->relations[i].tuple_size; j++ ){ // only look at attributes in table NOT foreign table
			if( tcat->relations[i].orig_attr_locs[j] == attr_id ){
				ret_arr[count] = i; // add relation location to return array
				count++;
				break; // once attribute found in relation go to next one
			}
		}
	}
	return (count == 0) ? -1 : count;
}

/*
 * Mark any unique tuples with the provided attribute id as 
 * deleted and return 1 with success, -1 otherwise.
 */
int delete_uniq_tup( table_catalog* tcat, int attr_id ){
	bool check = false;
	for( int i = 0; i<tcat->unique_size; i++ ){
		for( int j = 0; j<tcat->unique_tuples[i].tup_size; j++ ){
			if( attr_id == tcat->unique_tuples[i].attr_tuple[j] ){
				check = true;
				tcat->unique_tuples[i].deleted = true;
				break;
			}
		}
	}
	return (check) ? 1 : -1;
}

/*
 * Loop through the tables and free the memory allocated for their
 * arrays and then free the contents of logs, but the user has to 
 * free the parameter logs.
 */ 
void terminate_catalog(catalogs *logs){
	for( int i = 0; i<logs->table_count; i++ ){
		// attribute heap memory
		for(int j = 0; j<logs->all_tables[i].attribute_count; j++){
			free( logs->all_tables[i].attributes[j].name ); // free name of attribute
		}
		// relation heap memory
		for(int j = 0; j<logs->all_tables[i].foreign_size; j++){
			free( logs->all_tables[i].relations[j].name );
			free( logs->all_tables[i].relations[j].orig_attr_locs );
			free( logs->all_tables[i].relations[j].for_attr_locs );
		}
		// unique tuples heap memory
		for(int j = 0; j<logs->all_tables[i].unique_size; j++){
			free( logs->all_tables[i].unique_tuples[j].attr_tuple );
		}
		// table heap memory
		free( logs->all_tables[i].table_name );
		free( logs->all_tables[i].attributes );
		free( logs->all_tables[i].relations );
		// confirm primary key hasn't already been freed
		if( logs->all_tables[i].primary_size > 0){
			free( logs->all_tables[i].primary_tuple );
		}
	}
	free( logs->all_tables );
}

// Helper Functions

/*
 * Convert the character representation of the attribute type to
 * the integer value of 0, 1, 2, 3, 4.
 * If the type doesn't match any known conversion then return -1.
 */
int type_conversion(char* type){
	int result = -1;
	char *temp = strdup(type);
	char *token = strtok(temp, "\s+(");
	if( strcmp(type, INTEGER) == 0){ result = 0; }
	else if( strcmp(type, DOUBLE) == 0){ result = 1; }
	else if( strcmp(type, BOOLEAN) == 0){ result = 2; }
	else if( strcmp(type, CHAR) == 0){ result = 3; }
	else if( strcmp(type, VARCHAR) == 0){ result = 4; }
	return result;
}

/*
 * Convert the integer type to the corresponding
 * string representation and return it.
 */
char *type_string( int type ){
	char *res;
	int size = 0;
	switch( type ){
		case 0: // int
			res = "integer\0";
			break;
		case 1: // double
			res = "double\0";
			break;
		case 2: // bool
			res = "boolean\0";
			break;
		case 3: // char
			res = "char\0";
			break;
		case 4: // varchar
			res = "varchar\0";
			break;
		default:
			res = "UNKNOWN\0";
	}
	return res;
}

/*
 * Loop through all the table catalogs and check for the deleted
 * flag is true and then increment the count.
 * Return count of tables that aren't delted other return 0.
 */
int get_table_count_no_deletes(catalogs* logs){
	int total = 0;
	for( int i = 0; i < logs->table_count; i++ ){
		if( !logs->all_tables[i].deleted ){ // only increment non-deleted tables
			total++;
		}
	}
	return total;
}

/*
 * Loop through all the attributes and check for the deleted
 * flag is true and then increment the count.
 * Return count of attributes that aren't delted other return 0.
 */
int get_attribute_count_no_deletes(table_catalog* t_cat){
	int total = 0;
	for( int i = 0; i < t_cat->attribute_count; i++ ){
		if( !t_cat->attributes[i].deleted ){ // only increment non-deleted tables
			total++;
		}
	}
	return total;
}

/*
 * Loop through all the table catalogs and check for the deleted
 * flag is true and then increment the count.
 * Return count of attributes that aren't delted other return 0.
 */
int get_relation_count_no_deletes(table_catalog* tcat){
	int total = 0;
	for( int i = 0; i < tcat->foreign_size; i++ ){
		if( !tcat->relations[i].deleted ){ // only increment non-deleted tables
			total++;
		}
	}
	return total;
}

/*
 * Loop through all the table catalogs and check for the deleted
 * flag is true and then increment the count.
 * Return count of attributes that aren't delted other return 0.
 */
int get_unique_count_no_deletes(table_catalog* tcat){
	int total = 0;
	for( int i = 0; i < tcat->unique_size; i++ ){
		if( !tcat->unique_tuples[i].deleted ){ // only increment non-deleted tables
			total++;
		}
	}
	return total;
}

/*
 * Return the name of the attribute in the provided table.
 */
char *get_attr_name( catalogs* logs, char *table_name, int attr_id ){
	table_catalog* temp = get_catalog_p( logs, table_name );
	return temp->attributes[attr_id].name;
}

/*
 * Return the data types of the table, the user is responsible
 * for freeing the returning pointer.
 */
int* get_table_data_types( table_catalog* tcat ){
	int *data_types = (int *)malloc(tcat->attribute_count*sizeof(int));
	for( int i = 0; i<tcat->attribute_count; i++ ){
		data_types[i] = tcat->attributes[i].type;
	}
	return data_types;
}

/*
 * Mark the table as deleted.
 */
void delete_table( table_catalog* tcat ){
	tcat->deleted = true;
}

/*
 * Print Catalog information including deletions to stdout.
 */
void pretty_print_catalogs(catalogs* logs){
	for(int i = 0; i < logs->table_count; i++){
		pretty_print_table( &(logs->all_tables[i]) );
	}
}

/*
 * Print Table information including deleted attributes, foreign
 * relations, and unique tupes.to stdout.
 */
void pretty_print_table(table_catalog* tcat){
	printf("Table: \n{name: %s, deleted: %d, attribute_count: %d, "
			"foreign_size: %d, primary_size: %d,"" unique_tuples: %d, " 
			"Attributes: [\n",
			tcat->table_name, tcat->deleted, tcat->attribute_count, 
			tcat->foreign_size, tcat->primary_size, tcat->unique_size );
	// print attributes
	pretty_print_attributes( tcat->attributes, tcat->attribute_count );
	printf("Foreign Relations:\n");
	// print relations
	pretty_print_relations( tcat, tcat->relations, tcat->foreign_size );
	printf("Unique Tuples:\n");
	// print unique tuples
	pretty_print_unique_tuples( tcat, tcat->unique_tuples, tcat->unique_size );
	// print primary key
	pretty_print_primary_tuples( tcat, tcat->primary_tuple, tcat->primary_size );
}

void pretty_print_attributes( attr_info* attributes, int size ){
	for( int i = 0; i<size; i++ ){
		char* str_type = type_string( attributes[i].type );
		printf("\tName: '%s' --> attr_id: %d, type: %s, deleted: %d, Constraints: [notnull: %d, primarykey: %d, unique: %d]\n",
			attributes[i].name, i, str_type, attributes[i].deleted ? 1 : 0,
			attributes[i].notnull, attributes[i].primarykey, attributes[i].unique);
	}
}

void pretty_print_relations( table_catalog* tcat, foreign_data* relations, int size ){
	for( int i = 0; i<size; i++ ){
		int tup_size = relations[i].tuple_size;
		printf("\tForeign Table: '%s', Table ID: %d, Deleted: %d, Num of Attributes: %d,\n\t\tRelation: (",
				relations[i].name, relations[i].foreign_table_id, relations[i].deleted ? 1 : 0,
				relations[i].tuple_size);
		for( int j = 0; j<tup_size; j++ ){
			if(j == tup_size-1){
				printf("'%s'",tcat->attributes[relations[i].orig_attr_locs[j]].name);
			}else{
				printf("'%s', ",tcat->attributes[relations[i].orig_attr_locs[j]].name);
			}
		}
		printf(") --> (");
		for( int j = 0; j<tup_size; j++ ){
			if(j == tup_size-1){
				printf("'%s'",tcat->attributes[relations[i].for_attr_locs[j]].name);
			}else{
				printf("'%s', ",tcat->attributes[relations[i].for_attr_locs[j]].name);
			}
		}
		printf(")\n");
	}
}

void pretty_print_unique_tuples( table_catalog* tcat, unique* tuples, int size ){
	for(int i = 0; i<size; i++){
		if( tuples[i].deleted ){continue;}
		printf("\t( ");
		for(int j = 0; j<tuples[i].tup_size; j++){
			if(j == tuples[i].tup_size-1){
				printf("'%s'", tcat->attributes[tuples[i].attr_tuple[j]].name);
			}else{
				printf("'%s',", tcat->attributes[tuples[i].attr_tuple[j]].name);
			}
		}
		printf(" )\n");
	}
}

void pretty_print_primary_tuples( table_catalog* tcat, int* prim_tup, int size ){
	printf("Primary Key: [ ");
	for( int i = 0; i<size; i++ ){
		if( i == size-1){
			printf("'%s'", tcat->attributes[prim_tup[i]].name);
		}else{
			printf("'%s', ", tcat->attributes[prim_tup[i]].name);
		}
	}
	printf(" ]\n");
}
