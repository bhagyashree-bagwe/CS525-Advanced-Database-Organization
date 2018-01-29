
CS 525_ Group10 - Assignment 1 - Storage Manager
________________

Team Members:

Bhagyashree Bhimsen Bagwe(Leader) - A20399761 bbagwe@hawk.iit.edu
Nishant Sharma - A20368125 - msharma10.iit.edu
Sunaina Shashikumar - A20376076 - sshashikumar@hawk.iit.edu
Shahid Zubair - A20340484 szubair@hawk.iit.edu
__________

File List:

README.txt
dberror.h
dberror.c
storage_mgr.h
storage_mgr.c
test_helper.h
test_assign1_1.c
test_assign1_2.c
Makefile
____

Aim:

To implement a storage manager which consist of various functions that can of read and write blocks from file on disk to memory and vice-versa. There is a fixed size (referred as PAGE_SIZE) of  pages to consider. 
In addition to this, we have methods for creating, opening, closing and destroying the files. The storage manager has to contain follwing information for an open file: The no. of total pages in the file, the current position of the file (for reading and writing), file name, and a FILE pointer.
_________________________

Installation Instruction:
1) Go to the path where the extracted files are available.
2) Run the command: make
3) Run the command for testing test_assign1.c: make run 
4) To remove object files, run the command: make clean 
5) We have provided additional test cases in file "test_assign1_2.c To run those test cases use the below command: ./test_assign1_2
_______________________

Functions Descriptions:

initStorageManager() - Intiates the storage manager.

createPageFile() - In this function if already a file exists then it asks user to overwrite a file. Or else, a new file is opened in write mode and write the first page with NULLs or a user can choose to overwrite the file. The calloc function is used to fill the first page of the file with NULLs.
 
openPageFile() - In this function it opens a file if exists or else throws an error. Also assigns all the file info to an existing file handle that is used to maintain the file info.

destroyPageFile() - If a file exists this function will either destroy or remove the existing file or else throws an error.

closePageFile() - If there is a open file this function closes it or throws an error.

Read Related Functions:

readBlock() -  This function reads a particular given block in the file and stores the content of the page or block into memory. It also check whether the given page number to be read is less than the total number of pages or else it throws an error. We used malloc function to assign the memory space for the read content. In case of any errors, appropriate errors are returned.

getBlockPos() - This function gives the current block position through file handle using curPagePos member of fileHandle structure or else throws an error.

readFirstBlock() - This function gives input to readBlock() which reads the page number. It then reads that block.

readPreviosBlock() - This function gets the current block position using getBlockPos() function which gives the page number to be read and decrements it by 1 as an input to readBlock() function. It then reads that block.

readCurrentBlock() - This function provides input to readBlock() function which is the page number to be read by getting current block position using getBlockPos() function. It then reads that block.

readNextBlock() - This function gets the current block position using getBlockPos() function which gives the page number to be read and increments it by 1 as an input to readBlock() function. It then reads that block.

readLastBlock() - This function provides input to readBlock() function by givingthe page number to be read by getting total no. of  pages using the fileHandle structure and decrements it by 1 . It then reads that block.

Write Related Functions

writeBlock() - This function provides the page number to write the given page. First it checks if the file handle is initia or not, then it checks if the given page number is within the total no. of pages, then it updates the file stats using the UpdateFileStats() by writing the content from the memory to the file. If any errors, errors are returned.

writeCurrentBlock - This function write the current block with the content stored in memory (memPage). To write the current block is uses writeBlock() function by passing the current position, the file handle structure and the content to be written. IF any errors, errors are returned.

appendEmptyBlock() - This function appends one empty block to the file with NULLs and increments the total no. of pages by 1. The calloc function is used in order to allocate a block = PAGE_SIZE in the file and filling it with NULLs. If any errors, errors are returned.
 
ensureCapacity() - This function checks if the file is able to accomodate the no. of pages. If the total number of pages is < the no. of pages then appendEmptyBlock() function is called which will append required no. of pages that can accomodate the desired no. of pages. Note that the blocks are appended and are filled with NULLs. If any errors, errors are returned.

UpdateFileStats() - This function is used to update the file related information in file handle.
_______________________

Other Error Codes:

#define RC_NUMBER_OF_PAGES_EXISTS 5 - When the total no. of pages in the file is = no. of pages required then this error code will be thrown stating that the no. of pages required is already present.

#define RC_WRITE_OUT_OF_BOUND_INDEX 6 - When the fwrite() function is used to write a block that has its page number > its total no. of pages then this error is thrown stating that the block you are trying to write is out of the bounds of the file.

#define RC_FILE_ALREADY_PRESENT 7 - When any function checks if the file exists or not. if yes, then this error is thrown.
_____________

Extra Credit:

We have implemented an extra test case that is available in the file named "test_assign1_2.c". The description of that test case can be found in the Test Cases section below.

test_assign1_2.c - This file contains additional test cases for testing.

No Memory Leaks - All the test cases are tested with Valgrind tool and no memory leaks are found. 

Test Cases :

testSinglePageContent() - This test case is used to test the functions that are used to read first block and write a block. Firstly, a file is created and opened. The first block is read and is expected to be empty since it is full of NULLs. Then a block is written in the file and is read again. At last, the file is destroyed.

testAppendWriteRead() - This test case reads first block and then writes current block, reads last block and then reads next block. It should return an error since there is no block after the last block.
A page is appended and then the next block is read, the current block is written and the previous block is read and then a page is appended and the last page is read and capacity is ensured to any given number of pages. After this, a block is written and the file is destroyed and opened again. Now, it should throw an error since a file destroyed cannot be opened. 
