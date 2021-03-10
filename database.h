
#ifndef __DATABASE_H__
#define __DATABASE_H__
#define CREATE 1
#define ALTER 2
#define DROP 3


#include "tableschema.h"
#include "ddl_parser.h"


void usage(bool error);
int * arg_manager(bool restart, char const *argv[], int argc);
int run(char *argv[]);
int create_table( catalogs *cat, int token_count, char** tokens );  //   --> GOOD
int drop_table_ddl( catalogs *cat, char *name );
int alter_table();
void run();
int create_table(catalogs *cat, int token_count, char** tokens);
int drop_table_ddl(catalogs *cat, char *name);
int alter_table(catalogs *cat, int token_count, char** tokens);
int create_table();
int drop_table();
int alter_table();

#endif