/*
 * Author: Scott C Johnson (scj@cs.rit.edu)
 * Header file for CSCI421 Group Project DDL Parser
 * Outlines the public functionality for the DDL Parser
 */
 
#ifndef DDL_PARSER_H
#define DDL_PARSER_H
 /*
  * This function handles the parsing of DDL statments
  *
  * @param statement - the DDL statement to execute
  * @return 0 on success; -1 on failure
  */
int parse_ddl_statement( char * statement );

#endif