#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_NUMBER_OF_PAGES_EXISTS 5
#define RC_FILE_READ_ERROR 6
#define RC_WRITE_OUT_OF_BOUND_INDEX 7
#define RC_FILE_ALREADY_PRESENT 8

#define RC_BUFFER_POOL_NOT_INIT 8
#define RC_BUFFER_POOL_ALREADY_INIT 9
#define RC_BUFFER_POOL_ERROR_IN_PINPAGE 10
#define RC_BUFFER_POOL_PINPAGE_PRESENT 11
#define RC_BUFFER_POOL_PAGE_INUSE 12
#define RC_BUFFER_POOL_ERROR_IN_UNPINPAGE 13
#define RC_BUFFER_POOL_ERROR_IN_FORCEPAGE 14
#define RC_BUFFER_POOL_ERROR_IN_MARKDIRTY 15
#define RC_BUFFER_POOL_NULL 16

#define RC_EMPTY_TABLE 17
#define RC_INVALID_CONDITION 18
#define RC_RM_ERROR_CREATING_TABLE 19
#define RC_RM_NO_RECORD_FOUND 20
#define RC_RM_NO_TABLE_INIT 21

#define RC_RM_PRIMARY_KEY_ALREADY_PRESENT_ERROR 22
#define RC_RM_PRIMARY_KEY_DELETE_ERROR 23
#define RC_RM_UPDATE_ON_DELETE_RECORD_ERROR 24

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
