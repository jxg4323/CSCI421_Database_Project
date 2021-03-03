#include "tableschema.h"

catalogs* intialize_catalogs(){
	catalogs* logs = (catalogs *)malloc(sizeof(catalogs));
	logs->last_id = 0;
	logs->table_count = 0;
	return logs;
}

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
 * Initialize the table catalog with the provided values and allocate 
 * memory for the array of attributes but don't fill. User is responsible
 * for freeing the table_name pointer after call.
 */
void init_catalog(table_catalog* catalog, int tid, int attr_count, char *table_name ){
	catalog->id = tid;
	catalog->deleted = false;
	catalog->attribute_count = attr_count;
	catalog->num_prim_keys = prim_keys_count;
	catalog->table_name = (char *)malloc(strlen(table_name)*sizeof(char));
	strcpy(catalog->table_name, table_name);
	catalog->attributes = (attr_info*)malloc(attr_count*sizeof(attr_info));
}

void init_attribute(attr_info* attr, int type, int notnull, int primkey, int unique, int rel_count, char *name){
	attr->deleted = false;
	attr->type = type;
	attr->name =(char *)malloc(strlen(name)*sizeof(char));
	strcpy(attr->name, name);
	attr->notnull = notnull;
	attr->primarykey = primkey;
	attr->unique = unique;
	attr->relation_count = rel_count;
	attr->foreign_relations = (foreign_data *)malloc(rel_count*sizeof(foreign_data));
}

/*
 * Increase size of attribute arrary in table catalog and increment
 * attribute count in the table catalog.
 */
void manage_attributes(table_catalog* t_cat, int attr_count){
	t_cat->attributes = (table_catalog *)realloc(attr_count*sizeof(attr_info));
	t_cat->attribute_count++;
}

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
		int t_id, a_count, prim_count, name_size;
		fread(&tid, sizeof(int), 1, fp);
		fread(&a_count, sizeof(int), 1, fp);
		fread(&name_size, sizeof(int), 1, fp);
		char *temp = (char *)malloc(name_size*sizeof(char));
		memset(temp, 0, name_size*sizeof(char));
		fread(temp, sizeof(char), name_size, fp);
		init_catalog( &(logs->all_tables[inx]), t_id, a_count, temp );

		// read attribute info 
		for(int i = 0; i < a_count; i++){
			int type, notnull, primkey, unique, rel_count;
			fread(&type, sizeof(int), 1, fp);
			fread(&notnull, sizeof(int), 1, fp);
			fread(&primkey, sizeof(int), 1, fp);
			fread(&rel_count, sizeof(int), 1, fp);
			fread(&name_size, sizeof(int), 1, fp);
			char *a_name = (char *)malloc(name_size*sizeof(char));
			memset(a_name, 0, name_size*sizeof(char));
			fread(&a_name, sizeof(char), name_size, fp);
			init_attribute(&(logs->all_tables[indx].attributes[i]), type, notnull, primkey, unique, rel_count, a_name);
			// Read foreign relations info
			for(int j = 0; j <rel_count; j++){
				fread(&(logs->all_tables[indx].attributes[i].foreign_relations[j].foreign_table_id), sizeof(int), 1, fp);
				fread(&(logs->all_tables[indx].attributes[i].foreign_relations[j].foreign_attr_loc), sizeof(int), 1, fp);
			}
			free(a_name);
		}


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

int write_catalogs(char *db_loc, catalogs* logs){
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
	fwrite(&(logs->table_count), sizeof(int), 1, fp);
	for(int i = 0; i < logs->table_count; i++ ){
		int temp = strlen((logs->all_tables[i].table_name));
		fwrite(&(logs->all_tables[i].id), sizeof(int), 1, fp);
		fwrite(&(logs->all_tables[i].attribute_count), sizeof(int), 1, fp);
		fwrite(&temp, sizeof(int), 1, fp);
		fwrite((logs->all_tables[i].table_name), sizeof(char), temp, fp);

		// write attribute info 
		for(int s = 0; s < a_count; s++){
			int type = logs->all_tables[i].attributes[s].type;
			int notnull = logs->all_tables[i].attributes[s].notnull;
			int primkey = logs->all_tables[i].attributes[s].primarykey;
			int unique = logs->all_tables[i].attributes[s].unique;
			int rel_count = logs->all_tables[i].attributes[s].relation_count;
			int name_size = strlen(logs->all_tables[i].attributes[s].name);
			// TODO: continure changing from here down to fwrites

			fwrite(&type, sizeof(int), 1, fp);
			fwrite(&notnull, sizeof(int), 1, fp);
			fwrite(&primkey, sizeof(int), 1, fp);
			fwrite(&rel_count, sizeof(int), 1, fp);
			fwrite(&name_size, sizeof(int), 1, fp);
			fwrite(&a_name, sizeof(char), name_size, fp);
			// Read foreign relations info
			for(int t = 0; t <rel_count; t++){
				fwrite(&(logs->all_tables[indx].attributes[i].foreign_relations[j].foreign_table_id), sizeof(int), 1, fp);
				fwrite(&(logs->all_tables[indx].attributes[i].foreign_relations[j].foreign_attr_loc), sizeof(int), 1, fp);
			}
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
	int last = logs->table_count;
	manage_catalogs( logs, logs->table_count+1, true );
	init_catalog( &(logs->all_tables[last]),last,0,table_name );
	return last;
}

/*
 * Add attribute information to the given table, and increment the attribute
 * count for the table. 
 * Return 1 for success and -1 for failure.
 */
int add_attribute(table_catalog* t_cat, char *attr_name, char *type, int constraints[3]){
	int last = t_cat->attribute_count;
	int type_con = type_conversion(type);
	manage_attributes( t_cat, t_cat->attribute_count+1 );
	init_attribute( &(t_cat->attributes[last]),  );
	return 1;
}

/*
 * Mark an attribute as deleted in the attribute information matrix. If an 
 * attribute is marked as removed then don't write it to the disk.
 * Return 1 for success and -1 for failure.
 */
int remove_attribute(table_catalog* t_cat, char *attr_name){}

/*
 * Layout of the foreign_row is: ["foreign_tabe_name", "a_1", "a_2", "r_1", "r_2"]
 * allocate or reallocate memory for the addidtion of the next foreign reference.
 * 
 * Return 1 with success of foreign info addition and -1 otherwise.
 */
int add_foreign_data(table_catalog* t_cat, char **foreign_row, int f_key_count){}

/*
 * Search through all catalog information to find the one with
 * the same table name as provided, given that all table names
 * are unique.
 * Return pointer to the table catalog or NULL if there isn't one. 
 */
table_catalog* get_catalog(catalogs *logs, char *tname){}

/*
 * Based on the table id retrieve the attribute location
 * in the table catalog.
 * Return location of attribute in table if found, o.w. -1.
 */
int get_attr_loc(catalogs *logs, int tid, char *attr_name){}

/*
 * Check if the table name is already in use.
 * Return True if a table name is found, false otherwise.
 */
bool check_table_name(catalogs *log, char *tname){}
