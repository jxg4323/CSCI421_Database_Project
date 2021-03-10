
#ifndef __DDLPARSER_H__
#define __DDLPARSER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#define DELIMITER " \t\r\n();,"
#define INIT_NUM_TOKENS 100


#include "database.h"

int check_statement( char * statement );

int parse_create_statement( char * statement );
int parse_drop_statement( char * statement );
int parse_alter_statement( char * statement );

#endif