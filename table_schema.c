#include "tableschema.h"

/*
 * Allocate memory fo catalogs structure and return the pointer if 
 * success otherwise return null.
 */
catalogs* intialize_catalogs(){
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
		logs->all_tables = (table_catalog *)realloc(table_count*sizeof(table_catalog));
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
		t_cat->attributes = (table_catalog *)realloc(attr_count*sizeof(attr_info));
		t_cat->attribute_count++;
	}else{
		t_cat->attributes = (table_catalog *)malloc(attr_count*sizeof(attr_info));
		t_cat->attribute_count = attr_count;
	}
}

/*
 * Increase size of foreign relations array in table catalog and increment
 * foreign relations count in the table catalog.
 */
void manage_foreign_rels(table_catalog* t_cat, int rel_count, bool increase){
	if( increase ){
		t_cat->relations = (table_catalog *)realloc(rel_count*sizeof(foreign_data));
		t_cat->foreign_size++;
	}else{
		t_cat->relations = (table_catalog *)malloc(rel_count*sizeof(foreign_data));
		t_cat->foreign_size = rel_count;
	}
}

/*
 * Increase size of primary array in table catalog and increment
 * primary count in the table catalog.
 */
void manage_prim_tuple(table_catalog *t_cat, int prim_size, bool increase){
	if( increase ){
		t_cat->primary_tuple = (int *)realloc(prim_size*sizeof(int));
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
		t_cat->unique_tuples = (unique *)realloc(size*sizeof(unique));
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
	catalog->foreign_data = (foreign_data*)malloc(f_size*sizeof(foreign_data));
	catalog->primary_tuple = (int *)malloc(p_size*sizeof(int));
	catalog->unique_tuple = (int *)malloc(u_size*sizeof(int));
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
void init_foreign_relation(foreign_data* fdata, int fid, int size, int *orig_attrs, int *for_attrs){
	fdata->deleted = false;
	fdata->foreign_table_id = fid;
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
	memcpy(udata->attr_tuple, tup);
}

/*
 * Initialize the primary array with the given values and mark 
 * User is responsible for freeing the parameter int arrays.
 */
void init_primary_tuple(table_catalog *t_cat, int *tuple, int size){
	t_cat->primary_size = size;
	t_cat->primary_tuple = (int *)malloc(size*sizeof(int));
	memcpy(t_cat->primary_tuple, tuple);
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
		fread(&tid, sizeof(int), 1, fp);
		fread(&a_count, sizeof(int), 1, fp);
		fread(&foreign_count, sizeof(int), 1, fp);
		fread(&unique_count, sizeof(int), 1, fp);
		fread(&prim_count, sizeof(int), 1, fp);
		fread(&name_size, sizeof(int), 1, fp);
		char *temp = (char *)malloc(name_size*sizeof(char));
		memset(temp, 0, name_size*sizeof(char));
		fread(temp, sizeof(char), name_size, fp);
		init_catalog( &(logs->all_tables[inx]), t_id, a_count, foreign_count, prim_count, unique_count, temp );
		// read attribute info 
		for(int i = 0; i < a_count; i++){
			int type, notnull, primkey, unique, rel_count;
			name_size = 0;
			fread(&type, sizeof(int), 1, fp);
			fread(&notnull, sizeof(int), 1, fp);
			fread(&primkey, sizeof(int), 1, fp);
			fread(&name_size, sizeof(int), 1, fp);
			char *a_name = (char *)malloc(name_size*sizeof(char));
			memset(a_name, 0, name_size*sizeof(char));
			fread(a_name, sizeof(char), name_size, fp);
			init_attribute(&(logs->all_tables[indx].attributes[i]), type, notnull, primkey, unique, a_name);
			free(a_name);
		}
		// read foreign relation information
		for(int i = 0; i < foreign_count; i++){
			int fid, size;
			name_size = 0;
			fread(&fid, sizeof(int), 1, fp);
			fread(&size, sizeof(int), 1, fp);
			fread(&name_size, sizeof(int), 1, fp);
			char *name = (char *)malloc(name_size*sizeof(char));
			memset(name, 0, name_size*sizeof(char));
			int *orig_temp = (int *)malloc(size*sizeof(int));
			int *for_temp = (int *)malloc(size*sizeof(int));
			fread(orig_temp, sizeof(int), size, fp);
			fread(for_temp, sizeof(int), size, fp);
			init_foreign_relation( &(logs->all_tables[indx].relations[i]))
			free( name );
			free( orig_temp );
			free( for_temp );
		}
		// read unique tuples
		for(int i = 0; i < unique_count; i++){
			int size;
			fread(&size, sizeof(int), 1, fp);
			int *arr = (int *)malloc(size*sizeof(int));
			fread(arr, sizeof(int), size, fp);
			init_unique_tuple( &(logs->all_tables[indx].unique_tuples[i]), size, arr);
			free( arr );
		}
		// read primary key tuple
		int p_size;
		fread(&p_size, sizeof(int), 1, fp);
		int *p_temp = (int *)malloc(p_size*sizeof(int));
		fread(p_temp, sizeof(int), p_size, fp);
		init_primary_tuple(&(logs->all_tables[indx]), p_temp, p_size);

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
	// TODO: change to accomodate new structure --> accomodate deleted flags
	// MAKE sure when writing attributes that the length of the name is written
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
		// get counts of array information without deltes
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
			fwrite(&name_size, sizeof(int), 1, fp);
			fwrite(logs->all_tables[indx].attributes[i].name, sizeof(char), name_size, fp);
		}
		// write foreign relation information
		for(int i = 0; i < foreign_count; i++){
			if( logs->all_tables[indx].relations[i].deleted ){continue;}
			name_size = strlen(logs->all_tables[indx].relations[i].name);
			int temp = logs->all_tables[indx].relations[i].tuple_size;
			fwrite(&(logs->all_tables[indx].relations[i].foreign_table_id), sizeof(int), 1, fp);
			fwrite(&(logs->all_tables[indx].relations[i].tuple_size), sizeof(int), 1, fp);
			fwrite(&name_size, sizeof(int), 1, fp);
			fwrite(logs->all_tables[indx].relations[i].orig_attr_locs, sizeof(int), temp, fp);
			fwrite(logs->all_tables[indx].relations[i].for_attr_locs, sizeof(int), temp, fp);
		}
		// write unique tuples
		for(int i = 0; i < unique_count; i++){
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
 TODO: redo
 */
int new_catalog(catalogs *logs, char *table_name){
	int last = logs->table_count;
	manage_catalogs( logs, logs->table_count+1, true );
	//TODO: confirm table name is unique
	init_catalog( &(logs->all_tables[last]),last,0,table_name );
	return last;
}

/*
 * Add attribute information to the given table, and increment the attribute
 * count for the table.
 * @param @constraints: layout of array [<notnull>, <primarykey>, <unique>]
 * Return 1 for success and -1 for failure.
 TODO: redo
 */
int add_attribute(table_catalog* t_cat, char *attr_name, char *type, int constraints[3]){
	int last = t_cat->attribute_count;
	int type_con = type_conversion(type);
	if ( type_con == -1 ){
		fprintf( stderr, "ERROR: add_attribute, Type not knonw\n" );
		return -1;
	}
	manage_attributes( t_cat, t_cat->attribute_count+1 );
	init_attribute( &(t_cat->attributes[last]), type, constraints[0], constraints[1], constraints[2], 0 );
	return 1;
}

/*
 * Mark an attribute as deleted in the attribute information matrix. If an 
 * attribute is marked as removed then don't write it to the disk.
 * Return 1 for success and -1 for failure.
 TODO: redo
 */
int remove_attribute(table_catalog* t_cat, char *attr_name){

}

/*
 * Find the attribute listed in the parameters in the table
 * and return the index of the attribute in the array. If
 * the attribute isn't found the return -1.
 TODO: redo
 */
int find_attr(table_catalog* t_cat, char *attr_name){
	int count = t_cat->attribute_count;
	int loc = -1;
	for( int i = 0; i < count; i++ ){
		if( strcmp(t_cat->attributes[i].name, attr_name) == 0 ){ loc = i; }
	}
}

/*
 * Layout of the foreign_row is: ["foreign_tabe_name", "a_1", "a_2", "r_1", "r_2"]
 * allocate or reallocate memory for the addidtion of the next foreign reference.
 * 
 * Return 1 with success of foreign info addition and -1 otherwise.
 TODO: redo
 */
int add_foreign_data(table_catalog* t_cat, char **foreign_row, int f_key_count){}

/*
 * Search through all catalog information to find the one with
 * the same table name as provided, given that all table names
 * are unique.
 * Return pointer to the table catalog or NULL if there isn't one. 
 TODO: redo
 */
table_catalog* get_catalog(catalogs *logs, char *tname){}

/*
 * Based on the table id retrieve the attribute location
 * in the table catalog.
 * Return location of attribute in table if found, o.w. -1.
 TODO: redo
 */
int get_attr_loc(catalogs *logs, int tid, char *attr_name){}

/*
 * Check if the table name is already in use.
 * Return True if a table name is found, false otherwise.
 TODO: redo
 */
bool check_table_name(catalogs *log, char *tname){}

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
	for( int i = 0; i < t_cat->foreign_count; i++ ){
		if( !t_cat->relations[i].deleted ){ // only increment non-deleted tables
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
	for( int i = 0; i < t_cat->unique_count; i++ ){
		if( !t_cat->unique_relations[i].deleted ){ // only increment non-deleted tables
			total++;
		}
	}
	return total;
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
	printf("Table: {name: %s, deleted: %d, attribute_count: %d, "
			"foreign_size: %d, primary_size: %d,"" unique_tuples: %d, " 
			"Attributes: \n",
			tcat->tables_name, tcat->deleted, tcat->attribute_count, 
			tcat->foreign_size, tcat->primary_size, tcat->unique_size );
	// print attributes
	pretty_print_attributes( tcat->attributes, tcat->attribute_count );
	printf("Foreign Relations:\n");
	// print relations
	pretty_print_relations( tcat->relation, tcat->foreign_size );
	printf("Unique Tuples:\n");
	// print unique tuples
	pretty_print_unique_tuples( tcat->unique_tuples, tcat->unique_size );
	printf("Primary Key:\n");
	// print primary key
	pretty_print_primary_tuples( tcat->primary_tuple, tcat->primary_size );
}

void pretty_print_attributes( attr_info* attributes, int size ){
	for( int i = 0; i<size; i++ ){
		printf("\tName: '%s' --> type: %d, deleted: %d, Constraints: [notnull: %d, primarykey: %d, unique: %d]\n",
			attributes[i].name, attributes[i].type, attributes[i].deleted ? 1 : 0,
			attributes[i].notnull, attributes[i].primarykey, attributes[i].unique);
	}
}

void pretty_print_relations( foreign_data* relations, int size ){
	for( int i = 0; i<size; i++ ){
		int tup_size = relations[i].tuple_size;
		printf("\tForeign Table: '%s', Table ID: %d, Deleted: %d, Num of Attributes: %d, Original Attributes: [",
				relations[i].name, relations[i].foreign_table_id, relations[i].deleted ? 1 : 0,
				relations[i].tuple_size);
		for( int j = 0; j<tup_size; j++ ){
			if(j == tup_size-1){
				printf("%d",relations[i].orig_attr_locs[j]);
			}else{
				printf("%d, ",relations[i].orig_attr_locs[j]);
			}
		}
		printf("], Foreign Atrributes: [");
		for( int j = 0; j<tup_size; j++ ){
			if(j == tup_size-1){
				printf("%d",relations[i].for_attr_locs[j]);
			}else{
				printf("%d, ",relations[i].for_attr_locs[j]);
			}
		}
		printf("]\n");
	}
}

void pretty_print_unique_tuples( unique* tuples, int size ){
	for(int i = 0; i<size; i++){
		printf("\t(");
		for(int j = 0; j<tuples[i].tup_size; j++){
			if(j == tuples[i].tup_size-1){
				printf("%d", tuples[i].attr_tuple[j]);
			}else{
				printf("%d,", tuples[i].attr_tuple[j]);
			}
		}
		printf(")\n");
	}
}

void pretty_print_primary_tuples( int* prim_tup, int size ){
	printf("Primary Key: [");
	for( int i = 0; i<size; i++ ){
		if( i == size-1){
			printf("%d", prim_tup[i]);
		}
		printf("%d, ", prim_tup[i]);
	}
	printf("]\n");
}
