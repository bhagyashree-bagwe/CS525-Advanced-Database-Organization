#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testSinglePageContent(void);
static void testAppendWriteReadContent(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();
  testSinglePageContent();
  testAppendWriteReadContent();
  return 0;
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
	
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0,&fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");
	
	
  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  
  TEST_DONE();
free(ph);
}

void
testAppendWriteReadContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
 ASSERT_TRUE((readFirstBlock (&fh,ph)==RC_OK),"Reading First block in file is done without error");
	printf("Reading First block operation finished. Block was empty\n");
     
	printf("writing Current block \n");

  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeCurrentBlock (&fh, ph));
  printf("writing Current block complete\n");

  // read back the page containing the string and check that it is correct
  ASSERT_TRUE((readLastBlock (&fh,ph)==RC_OK),"Reading last block in file is done without error");
	printf("Reading last block operation finished.First and last block are same\n");
	
	printf("Again reading Next block. \n");
	ASSERT_TRUE((readNextBlock(&fh,ph) != RC_OK), "Next block cannot be read without appending new block!");
	
	ASSERT_TRUE((appendEmptyBlock (&fh)==RC_OK),"File append operation successful");
	printf("file append operation finished.\n");
		
	 ASSERT_TRUE((readNextBlock (&fh,ph)==RC_OK),"Reading next block in file is done without error");
	printf("Reading Next block operation finished.\n");
	  
	printf("writing Current block \n");
	for (i=0; i < PAGE_SIZE; i++)
	    ph[i] = (i % 10) + '0';
  	TEST_CHECK(writeCurrentBlock (&fh, ph));
 	 printf("writing Current block complete\n");
	
	 ASSERT_TRUE((readPreviousBlock (&fh,ph)==RC_OK),"Reading previous block in file is done without error");
	printf("Reading Previous block operation finished.\n");
	
	ASSERT_TRUE((appendEmptyBlock (&fh)==RC_OK),"File append operation successful");
	printf("file append operation finished.\n");
		
	 ASSERT_TRUE((readLastBlock (&fh,ph)==RC_OK),"Reading last block in file is done without error");
	printf("Reading last block operation finished. last block are same\n");
	
	
	ASSERT_TRUE((ensureCapacity (4, &fh) != RC_OK), "Ensuring the capacity of file, File has desired number of pages");
		
	printf("writing  block 3\n");
	for (i=0; i < PAGE_SIZE; i++)
	    ph[i] = (i % 10) + '0';
  	ASSERT_TRUE(writeBlock (3,&fh, ph) == RC_OK,"writing  block 3 complete\n");
 	 
		
	  TEST_CHECK(destroyPageFile (TESTPF));  
  	 
	ASSERT_TRUE(openPageFile (TESTPF, &fh)!= RC_OK, "File Cannot be opened as it is destroyed\n");
	
  TEST_DONE();
free(ph);
}
