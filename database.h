
#ifndef __DATABASE_H__
#define __DATABASE_H__
#define CREATE 1
#define ALTER 2
#define DROP 3

#include <string.h>
#include <limits.h>
#include <dirent.h>
#include <errno.h>
#include "tableschema.h"
#include "ddl_parser.h"

static char* db_path;

void usage(bool error);
int arg_manager(bool restart, char *argv[], int argc);
int run(int argc, char *argv[]);
int create_table( catalogs *cat, int token_count, char** tokens );  //   --> GOOD
int drop_table_ddl( catalogs *cat, char *name );
int alter_table( catalogs *cat, int token_count, char** tokens );

//helper functions
int drop_attribute(catalogs *cat, char *table, char *attribute);
int add_attribute_table(catalogs *cat, char *table, char *name, char *type, char *value);
bool health_check( );

#endif
