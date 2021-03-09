
#ifndef __DATABASE_H__
#define __DATABASE_H__

#include "tableschema.h"

void usage(bool error);
int * arg_manager(bool restart, char const *argv[], int argc);
void run();
int create_table( catalogs *cat, int token_count, char** tokens );
int drop_table_ddl( catalogs *cat, char *name );
int alter_table();
int create_table();
int drop_table();
int alter_table();

#endif