#
# Created by gmakemake (Ubuntu Jul 25 2014) on Thu Apr 22 00:01:05 2021
#

#
# Definitions
#

.SUFFIXES:
.SUFFIXES:	.a .o .c .C .cpp .s .S
.c.o:
		$(COMPILE.c) $<
.C.o:
		$(COMPILE.cc) $<
.cpp.o:
		$(COMPILE.cc) $<
.S.s:
		$(CPP) -o $*.s $<
.s.o:
		$(COMPILE.cc) $<
.c.a:
		$(COMPILE.c) -o $% $<
		$(AR) $(ARFLAGS) $@ $%
		$(RM) $%
.C.a:
		$(COMPILE.cc) -o $% $<
		$(AR) $(ARFLAGS) $@ $%
		$(RM) $%
.cpp.a:
		$(COMPILE.cc) -o $% $<
		$(AR) $(ARFLAGS) $@ $%
		$(RM) $%

CC =		gcc
CXX =		g++

RM = rm -f
AR = ar
LINK.c = $(CC) $(CFLAGS) $(CPPFLAGS) $(LDFLAGS)
LINK.cc = $(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS)
COMPILE.c = $(CC) $(CFLAGS) $(CPPFLAGS) -c
COMPILE.cc = $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c
CPP = $(CPP) $(CPPFLAGS)
########## Default flags (redefine these with a header.mak file if desired)
CXXFLAGS =	-ggdb
CFLAGS =	-ggdb
CLIBFLAGS =	-lm
CCLIBFLAGS =	
########## End of default flags


CPP_FILES =	
C_FILES =	database.c ddl_parser.c dml_parser.c storagemanager.c table_schema.c
PS_FILES =	
S_FILES =	
H_FILES =	database.h database1.h ddl_parser.h ddlparse.h dml_parser.h dmlparser.h storagemanager.h tableschema.h
SOURCEFILES =	$(H_FILES) $(CPP_FILES) $(C_FILES) $(S_FILES)
.PRECIOUS:	$(SOURCEFILES)
OBJFILES =	ddl_parser.o dml_parser.o storagemanager.o table_schema.o 

#
# Main targets
#

all:	database 

database:	database.o $(OBJFILES)
	$(CC) $(CFLAGS) -o database database.o $(OBJFILES) $(CLIBFLAGS)

#
# Dependencies
#

database.o:	database.h database1.h ddl_parser.h ddlparse.h dml_parser.h dmlparser.h storagemanager.h tableschema.h
ddl_parser.o:	database.h ddl_parser.h ddlparse.h dml_parser.h dmlparser.h storagemanager.h tableschema.h
dml_parser.o:	database.h ddl_parser.h dml_parser.h dmlparser.h storagemanager.h tableschema.h
storagemanager.o:	storagemanager.h
table_schema.o:	storagemanager.h tableschema.h

#
# Housekeeping
#

Archive:	archive.tgz

archive.tgz:	$(SOURCEFILES) Makefile
	tar cf - $(SOURCEFILES) Makefile | gzip > archive.tgz

clean:
	-/bin/rm -f $(OBJFILES) database.o core

realclean:        clean
	-/bin/rm -f database 
