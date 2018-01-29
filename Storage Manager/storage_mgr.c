#include<stdio.h>
#include<stdlib.h>
#include"storage_mgr.h"
#include"dberror.h"

FILE *file;
int pagetoberead;

/*---------------------------------------------------------*/
/* Method that prints the message when Storage manager is initialized */
/*---------------------------------------------------------*/
void initStorageManager()
{
	printf("Storage Manager Initialized\n");
}

/*---------------------------------------------------------*/
/* Method to create a new file or override an existing file with \0 */
/*---------------------------------------------------------*/
RC createPageFile(char *fileName)
{	
	int i;
	char userInput;
	//Check if file with given name already exists, if yes, ask User if he wishes to override it
	file=fopen(fileName,"r");
	
	if(file!=NULL) //File already exists
	{
		printf("File with given name already exists in the File System. Would you like to overwrite it?(Enter y/n)");
		scanf("%c",&userInput);
		fclose(file);
	}
	if(file==NULL || userInput=='y') //File doest not exist or user has opted to overwrite an existing file
	{	
		file=fopen(fileName,"w");
		//Creating character pointer of size 4096 and initializing it to '\0'
		char *ptr=(char *)calloc(PAGE_SIZE,sizeof(char));
		//write newly created page to file
		fwrite(ptr,PAGE_SIZE,sizeof(char),file);
		//Go to end of file
		fseek(file,0,SEEK_END);
		//Free the pointer
		free(ptr);
		fclose(file); 
		RC_message="File creation successfull!";
		return RC_OK;
	}
	else //File already exists and user has opted not to overwrite it
	{
		//Return already present file and terminate
		RC_message="File Already Exists!";
		return RC_FILE_ALREADY_PRESENT;
		
	}
	
} 


/*---------------------------------------------------------*/
/* Open existing page file */
/*---------------------------------------------------------*/
RC openPageFile(char *fileName,SM_FileHandle *fHandle)
{
	//Check if file with given name exists or not
	file=fopen(fileName,"r");
	int fPageCount,fSize;
	if(file == NULL)//File does not exist
	{
		RC_message="No File is present.";
		return RC_FILE_NOT_FOUND;
	}
	else//File exists
	{
		//Initialize the file handle with the information of the opened file
		file=fopen(fileName,"r");
		fHandle->fileName=fileName;
		//updating fhandle current page position to the beginning of file
		fHandle->curPagePos=ftell(file)/PAGE_SIZE;
		//Go to end of file
		fseek(file,0,SEEK_END);
		//Retrieve file size 
		fSize=ftell(file);
		//Count the number of pages
		fPageCount=fSize/PAGE_SIZE;
		fHandle->totalNumPages=fPageCount;
		//Update the pointer of file in fhandle
		fHandle->mgmtInfo=file;
		RC_message="Successfully Opened File.";
		return RC_OK;
		
	}
}
/*---------------------------------------------------------*/
/* Delete the page file */
/*---------------------------------------------------------*/
RC destroyPageFile(char *fileName)
{
	//Check if file with given name exists or not
	if(fopen(fileName,"r") != NULL)//File exists
	{
		//Remove the file from memory
		remove(fileName);
		RC_message="Successfully removed the file.";
		return RC_OK;
	}	
	else//File does not exist
	{
		RC_message="No File is present.";
		return RC_FILE_NOT_FOUND;
	}
} 
//--------------------------------------------------------------
//Close the page file
//--------------------------------------------------------------
RC closePageFile(SM_FileHandle *fHandle)
{
	if(fHandle != NULL)//checking if file handle is intialized or not
	{
		if(fopen(fHandle->fileName,"r") != NULL)//if the file handle is intialized,the it closes the file
		{
			if(fclose(fHandle->mgmtInfo) == 0)//checking if the file has been closed successfully.
			{
				RC_message="File is closed successfully";
				return RC_OK;
			}
			else
			{
				RC_message="No file is present.";
				return RC_FILE_NOT_FOUND;//return file is not present
			}
		}
		else 
		{	
			RC_message="No file is present.";
			return RC_FILE_NOT_FOUND;
		}
	}
	else 
	{
		RC_message="File requeseted related data not initialized.";
		return RC_FILE_HANDLE_NOT_INIT;//else display file handle not initialized
	}
}

//--------------------------------------------------------------
//Gets the current block position in the file
//--------------------------------------------------------------
int getBlockPos (SM_FileHandle *fHandle)
{
	if(fHandle != NULL)//checking if file handle is not equal to null
	{
		//if file handle is not equal to null, open the file and print the current block position and return the current block position
		if((fopen(fHandle->fileName,"r")) != NULL)
		{
			printf("%d ",fHandle->curPagePos);
			return fHandle->curPagePos;
		}
		else
		{
			RC_message="No File is present."; 
			return RC_FILE_NOT_FOUND;
		}
	}
	else
	{
		//if file handle is equal to null return this file handle has not been initialized along with file info
		RC_message="Desired File Related Data has not been initialized";
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

//--------------------------------------------------------------
// Reads blocks of the page file
//--------------------------------------------------------------
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle !=NULL)//checking if the file handle is not equal to null
	{
		if(fopen(fHandle->fileName,"r") != NULL)
		{
			//if file handle is not equal to null, it starts reading the content of the file
			if((pageNum)>-1 && (pageNum+1)<=fHandle->totalNumPages)//checking if the requested pageNum is smaller than total pages
			{
				if(memPage !=NULL)
				{
					fseek(fHandle->mgmtInfo,(((pageNum)*PAGE_SIZE)),SEEK_SET);//seeks the requested page number.
					fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);//reads the requested page.
					fHandle->curPagePos=pageNum;//updates the position of the current page
					RC_message="Successfully Read the File.";
					return RC_OK;
				}
			}
			else
			{
				RC_message="Page Requested to be read does not exist.";
				return RC_READ_NON_EXISTING_PAGE;
			}
		}
		else
		{
			RC_message="No File is present.";
			return RC_FILE_NOT_FOUND;
		}
	}
	else
	{
		RC_message="Desired file related data has not been initialized.";
		return RC_FILE_HANDLE_NOT_INIT;
	}	
}

/* ----------------------------------------------------------------- */
//Based on the current position of the block we can read the current block of the file 
/* ----------------------------------------------------------------- */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	pagetoberead=getBlockPos(fHandle);
	readBlock (pagetoberead,fHandle, memPage);
}
/* ------------------------------------------------------------ */
//Based on the current position of the block we can read the current block of the file 
/* ------------------------------------------------------------ */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	pagetoberead=getBlockPos(fHandle)+1;
	readBlock (pagetoberead,fHandle, memPage);
}
/* -------------------------------------------------------------- */
//Based on the current position of the block we can read the current block of the file 
/* -------------------------------------------------------------- */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	pagetoberead=getBlockPos(fHandle)-1;// here we are fetching the current position of the block and sbtracting 1 from it to get the previous page number 
	readBlock (pagetoberead,fHandle, memPage); 
}

/* ------------------------------------------------------------ */
//First block of the file is read
/* ------------------------------------------------------------- */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	pagetoberead=0;//set the value of first page number to be read.
	readBlock(pagetoberead,fHandle, memPage);
}

/* -------------------------------------------------------------- */
//Based on the current position of the block we can read the last block of the file
/* -------------------------------------------------------------- */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	pagetoberead=(fHandle->totalNumPages)-1; 
	readBlock (pagetoberead,fHandle, memPage);
}

/* ---------------------------------------------------------------- */
//Check if no. of pages in file < no. of pages requested, if its less then append pages.
/* ----------------------------------------------------------------- */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	//check if file handle == null and also check whether file can be opened or not
	if ((fHandle != NULL) && (fopen(fHandle->fileName,"r") != NULL))
	{
		//if no. of pages passed < no. of pages in file handle, append required empty pages to the eof
		while (fHandle->totalNumPages < numberOfPages)
		{
			
				appendEmptyBlock(fHandle);
			
		}
		
	}
	else
	{
		if (fHandle == NULL) 
		{		
			RC_message="File data that is required is not initialized";
			return RC_FILE_HANDLE_NOT_INIT;
		}
		else
		{
			RC_message="No File is present.";
			return RC_FILE_NOT_FOUND;
		}
	}
		
}

/* ------------------------------------------------------------ */
//Append the empty page to the eof
/* ------------------------------------------------------------ */
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	
	if ((fHandle != NULL) && (fopen(fHandle->fileName,"r") != NULL))//checking if file handle is empty and whether file can be opened
	{
		//if file handle != null and file is present, append new page to eof
		file = fopen(fHandle->fileName,"a");
		fHandle->totalNumPages+=1;//increment the page number.
		fseek(file, (((fHandle->totalNumPages)*PAGE_SIZE)), SEEK_END);//seeking to particular page number.
		char *stream = (char *)calloc(PAGE_SIZE, sizeof(char));//creating a pointer of PAGE_SIZE
		fwrite(stream, PAGE_SIZE, sizeof(char), file);//writing it to the file
		free(stream);//memory released
		fclose(file);//file is closed
		RC_message="Successfully appended the Block";
		return RC_OK;
	}
	else
	{
		if (fHandle == NULL)  
		{
			RC_message="File related data which is requested is not initialized";
			return RC_FILE_HANDLE_NOT_INIT;//if the handle equals null then return that "file handle is not initialized" along with the file information
		}
		else
		{	
			RC_message="No File is present.";
			return RC_FILE_NOT_FOUND;//if the file is not present, return that No File is Present.
		}
	}
}

/* -------------------------------------------------------------- */
// When the block gets written update the current position of the file and also info of the file in the file handle
/* ---------------------------------------------------------------- */
RC UpdateFileStats(SM_FileHandle *fHandle)
{

    file = fopen(fHandle->fileName,"r");
    fHandle->curPagePos = (ftell(fHandle->mgmtInfo)/PAGE_SIZE);
    fseek(file, (fHandle->curPagePos)*PAGE_SIZE , SEEK_SET);
    fHandle->mgmtInfo = file;
    RC_message="Successfully written the Data.";
	return RC_OK;
}


//--------------------------------------------------------------
//Writes current file block based upon the current block position
//--------------------------------------------------------------
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//checking if file handle is not equal to null
    if(fHandle != NULL)
    {
        if((fopen(fHandle->fileName,"r")) != NULL)
        {
                if((fHandle->curPagePos <= (fHandle->totalNumPages)))//checking for current block position
                {
                    if(memPage != NULL)
                    {
						//if current block position is fetched and memPage is not equal to null, then it writes the memPage in to the file
                        writeBlock(fHandle->curPagePos, fHandle, memPage);
                    }

                }
                else { 
					RC_message="Requested page is not present in the file";
					return RC_WRITE_OUT_OF_BOUND_INDEX;
				}
        }
         else 
		{
			RC_message="No File is present.";
			return RC_FILE_NOT_FOUND;
		}
    }
    else 
	{
		RC_message="Requested File data does not exist;";
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

/*---------------------------------------------------------*/
/* Write the block of the file */
/*---------------------------------------------------------*/
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //Check if handle is null or not
    if(fHandle != NULL)
    {
	//Check if file mentioned in the handle exists or not
        if((fopen(fHandle->fileName,"r")) != NULL)
        {
		//check if page no passed is less than or equal to pages in the file handle
                if((pageNum <= (fHandle->totalNumPages)))
                {
                    if(memPage != NULL)
                    {
                            file = fopen(fHandle->fileName,"r+");
                            fseek(file, pageNum*PAGE_SIZE , SEEK_SET);
			    //Check if file is successfully written with the memPage or not
                            if(fwrite(memPage,PAGE_SIZE,1,file) != 1)
                            { 
								RC_message="Request to write the File has Failed";
								return RC_WRITE_FAILED;
							}
                            else
							{
								//If File is successfuly written then update the file handle
								UpdateFileStats(fHandle);
							}
                    }

                }
                else 
				{
					RC_message="Requested page is not present.";
					return RC_WRITE_OUT_OF_BOUND_INDEX;
				}
        }
        else 
		{
			RC_message="No File is present.";
			return RC_FILE_NOT_FOUND;
		}
    }
    else 
	{
		RC_message="Data of Requested File is not present.";
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

