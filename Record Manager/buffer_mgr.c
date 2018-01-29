#include "mgmt.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dt.h"
#include "dberror.h"
#include "stdio.h"
#include "stdlib.h"
#include "pthread.h"

#define FRAME_SIZE 100

// Mutex variables initializing for the purpose of thread safety
static pthread_mutex_t mutex_init = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_unpinPage = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_pinPage = PTHREAD_MUTEX_INITIALIZER;

//---------------------------------------------------------------
//Bufferpool is being initializedwith all the related BufferPool information
//---------------------------------------------------------------
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
	//BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;

	//checks whether the BufferPool is null or not
	if(bm->mgmtData == NULL)
	{
	
		// locks the mutex when it is returning
		pthread_mutex_lock(&mutex_init); 
		BM_BufferPoolInfo *bpool = (BM_BufferPoolInfo *)malloc(sizeof(BM_BufferPoolInfo));
		bpool->fh = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));

		if(bpool->fh == NULL)
		{
			free(bpool->fh);
			// bpool is assigned to null value
			bpool->fh = NULL; 
			free(bpool);
			bpool = NULL;
			// unlocks the mutex when it is returning
			pthread_mutex_unlock(&mutex_init);
			return RC_FILE_HANDLE_NOT_INIT;
		}
		
		if(openPageFile(pageFileName, bpool->fh) == RC_FILE_NOT_FOUND)
		{
			// unlocks the mutex when it is returning
			pthread_mutex_unlock(&mutex_init);
			return RC_FILE_NOT_FOUND;
		}
		bpool->strtFrame = NULL;
		bpool->currFrame = NULL;
		bpool->node = NULL;
		bpool->search = NULL;
		bpool->numReadIO = 0;
		bpool->numWriteIO = 0;
		bm->pageFile = pageFileName;
		bm->numPages = numPages;
		bm->strategy = strategy;
		bm->mgmtData = bpool;
		// unlocks the mutex when it is returning
		pthread_mutex_unlock(&mutex_init);
		return RC_OK;
	}
	else
	{
		return RC_BUFFER_POOL_ALREADY_INIT;
	}
}

//---------------------------------------------------------------
//BufferPool is shut down
//---------------------------------------------------------------
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;

	//checks whether the Bufferpool is initialized or not
	if(bm->mgmtData != NULL)
	{
		//it frees the bpool, if bpools start frame is equal to null
		if(bpool->strtFrame == NULL)
		{
			
			bpool->fh = NULL;
			free(bpool->fh);
			
			bm->mgmtData = NULL;
			free(bm->mgmtData);
			free(bm);
			return RC_OK;
		}
		bpool->currFrame = bpool->strtFrame;
		while(bpool->currFrame != NULL)
		{
			bpool->currFrame->fixcounts = 0;
			bpool->currFrame = bpool->currFrame->nxtFrame;
		}
		int status = forceFlushPool(bm);

		//Memory is released	
		bpool->fh = NULL;
		free(bpool->fh);
		bm->mgmtData = NULL;
		free(bm->mgmtData);
		free(bm);

		return RC_OK;
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;
	}
}
//--------------------------------------------------------------
//Flush the BufferPool
//--------------------------------------------------------------
RC forceFlushPool(BM_BufferPool *const bm)
{
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;

	//checks whether the Bufferpool is initialized or not
	if(bpool != NULL)
	{
		bpool->node=bpool->strtFrame;
		if(bpool->node != NULL)
		{
			while(bpool->node != NULL)
			{
				//checking whether the page is dirty or not
				if(bpool->node->is_dirty)
				{
					if(bpool->node->fixcounts == 0)
					{
						if(writeBlock(bpool->node->pageNum,bpool->fh,bpool->node->bph->data) == RC_OK)
						{	
							// it changes the page which is dirty
							bpool->node->is_dirty=0; 
							bpool->numWriteIO +=1;
						}
					}
				}
				bpool->node=bpool->node->nxtFrame;
			}
			return RC_OK;
		}
		else
		{
			return RC_BUFFER_POOL_NULL;
		}
	}
	else
	{	
		return RC_BUFFER_POOL_NOT_INIT;		
	}
	
}

//--------------------------------------------------------------
//It finds and returns the position of a particular page
//--------------------------------------------------------------
PageFrame *findPage(BM_BufferPoolInfo *bpool, PageNumber pageNumber)
{
	bpool->node=bpool->strtFrame;
	while(bpool->node != NULL)
	{	
		if(bpool->node->pageNum == pageNumber)
		{
			break;
		}
		bpool->node=bpool->node->nxtFrame;
	}
	return bpool->node;
}

//--------------------------------------------------------------
//MArks the bpool page as dirty
//--------------------------------------------------------------
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;

	//checking whether the Bufferpool is initialized or not		
	if(bm->mgmtData != NULL)
	{
		bpool->search=(PageFrame *)findPage(bm->mgmtData,page->pageNum);
		if(bpool->search != NULL)
		{
			// marking the page as dirty
			bpool->search->is_dirty=1; 
			return RC_OK;		
		}
				
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;
	}	
}
//--------------------------------------------------------------
//Page is unpinned from the bpool
//--------------------------------------------------------------
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;	

	//checking whether the Bufferpool is initialized or not
	if(bpool != NULL)
	{
		pthread_mutex_lock(&mutex_unpinPage);
		//searching for the page
		bpool->search=(PageFrame *)findPage(bm->mgmtData,page->pageNum);

		//checking whether the search is equal to null or not
		if(bpool->search != NULL)
		{
			bpool->search->fixcounts-=1;
			pthread_mutex_unlock(&mutex_unpinPage);
			return RC_OK;		
		}	
		pthread_mutex_unlock(&mutex_unpinPage);
				
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;	
	}	
			
}
//--------------------------------------------------------------
//Writing dirty page to the disk befor it is being replaced
//--------------------------------------------------------------
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;	
	if(bm->mgmtData != NULL)
	{
		// searching the page
		bpool->search=(PageFrame *)findPage(bm->mgmtData,page->pageNum); 
		
		//checking whether the search is equal to null or not
		if(bpool->search != NULL)
		{
			
			if(writeBlock(bpool->search->pageNum,bpool->fh,bpool->search->bph->data) == RC_OK)
			{	
			
				bpool->search->is_dirty=0;
				bpool->numWriteIO +=1;
				return RC_OK;	
			}	
		}
		else
		{
			return RC_OK;
		}			
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;	
	}	
}

//--------------------------------------------------------------
//checks and gets the empty frame of the Bufferpool
//--------------------------------------------------------------
int GetEmptyFrame(BM_BufferPool *bm)
{
	int i = 0;
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;	
	bpool->node = bpool->strtFrame;
	while(bpool->node != NULL)
	{
		if(bpool->node->pageFrameNum != i)
		{ 
			return i; 
		}	
		i++;
		bpool->node=bpool->node->nxtFrame;
	}
	
	if(i < bm->numPages)
	{
		return i;
	}	
	else
	{
		return -1;
	}
}

//--------------------------------------------------------------
//Bufferpool size is being returned
//--------------------------------------------------------------
int PoolSize(BM_BufferPoolInfo *bpool)
{
	int i = 0;
	bpool->node = bpool->strtFrame; 
	while(bpool->node != NULL)
	{
		i++;
		bpool->node=bpool->node->nxtFrame;
	}
	return i;
}

//--------------------------------------------------------------
//Changes the value of count
//--------------------------------------------------------------
void ChangeCount(BM_BufferPoolInfo *bpool)
{
	bpool->node = bpool->strtFrame; 
	while(bpool->node != NULL)
	{
		bpool->node->pageFrameCount = bpool->node->pageFrameCount + 1;
		bpool->node = bpool->node->nxtFrame;
	}
}

//---------------------------------------------------------------
//Calculates and returns the buffer count
//---------------------------------------------------------------
PageFrame *findCount(BM_BufferPoolInfo *bpool)
{
	int high = 0;
	bpool->node = bpool->strtFrame;
	bpool->search = bpool->strtFrame;
	while(bpool->node != NULL)
	{
		if(bpool->node->pageFrameCount > high)
		{
			if(bpool->node->fixcounts == 0)
			{
				high = bpool->node->pageFrameCount;
				bpool->search = bpool->node;
			}
		}
		bpool->node = bpool->node->nxtFrame;
	}
	return bpool->search;
}



//--------------------------------------------------------------
//Replaces the page as per the stratergy
//--------------------------------------------------------------
int replacementStrategy(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum1)
{
	int a;
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;	
	bpool->search = findCount(bm->mgmtData);
	if(bpool->search->is_dirty == 1)
	{
		if(writeBlock(bpool->search->pageNum,bpool->fh,bpool->search->bph->data) == RC_OK)
		{
			bpool->search->is_dirty = 0;
			bpool->numWriteIO += 1;
		}
	}
	ChangeCount(bm->mgmtData);
	int status=readBlock(pageNum1,bpool->fh,bpool->search->bph->data);
	if(status == RC_OK)
	{
		bpool->search->bph->pageNum = pageNum1;
		bpool->search->pageNum = pageNum1;
		bpool->search->is_dirty = 0;
		bpool->search->pageFrameCount = 1;
		bpool->search->freqCount = 1;
		bpool->search->fixcounts = 1;
		page->data = bpool->search->bph->data;
		page->pageNum = pageNum1;
		return RC_OK;
	}
	return status;
}

/*-----------------------------------------------------------
Get and return the highest count of the BufferPool
----------------------------------------------------------*/
int getHighestCount(BM_BufferPoolInfo *bpool)
{
	int high = 0;
	bpool->node = bpool->strtFrame;
	bpool->search = bpool->strtFrame;
	while(bpool->node != NULL)
	{
		if(bpool->node->pageFrameCount > high)
		{
			if(bpool->node->fixcounts == 0)
			{
				high = bpool->node->pageFrameCount;
				bpool->search = bpool->node;
			}
		}
		bpool->node = bpool->node->nxtFrame;
	}
	return bpool->search->pageFrameCount;
}

/*---------------------------------------------------------------
Return lowest frequency count for LFU strategy
-----------------------------------------------------------------*/
PageFrame *findLowFreqCnt(BM_BufferPoolInfo *bpool)
{
	int low = 99999;
	int highCount = getHighestCount(bpool);
	bpool->node = bpool->strtFrame;
	bpool->search = bpool->strtFrame;
	while(bpool->node != NULL)
	{
		if(bpool->node->freqCount <= low)
		{
			if(bpool->node->fixcounts == 0 && bpool->node->pageFrameCount <= highCount && bpool->node->pageFrameCount != 1)
			{
				low = bpool->node->freqCount;
				bpool->search = bpool->node;
			}
		}
		bpool->node = bpool->node->nxtFrame;
	}
	return bpool->search;
}
/*--------------------------------------------------------------
Implement LFU strategy
---------------------------------------------------------------*/
int LFU(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum)
{
	int status;
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;	
	bpool->search = findLowFreqCnt(bm->mgmtData);
	if(bpool->search->is_dirty == 1)
	{
		status = writeBlock(bpool->search->pageNum,bpool->fh,bpool->search->bph->data);
		if (status == RC_OK)
		{
			bpool->search->is_dirty = 0;
			bpool->numWriteIO += 1;
		}
	}
	ChangeCount(bm->mgmtData);
	status = readBlock(pageNum,bpool->fh,bpool->search->bph->data);
	if(status == RC_OK)
	{
		bpool->search->bph->pageNum = pageNum;
		bpool->search->pageNum = pageNum;
		bpool->search->is_dirty = 0;
		bpool->search->pageFrameCount = 1;
		bpool->search->freqCount = 1;
		bpool->search->fixcounts = 1;
		page->data = bpool->search->bph->data;
		page->pageNum = pageNum;
		return RC_OK;
	}
	return status;
}

/*-------------------------------------------------------------
Pin Page in BufferPool
--------------------------------------------------------------*/
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum1)
{
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;	
	if(bm->mgmtData != NULL)
	{	
		//Lock mutex while returning
		pthread_mutex_lock(&mutex_pinPage);
		if(pageNum1 >= bpool->fh->totalNumPages)
		{
			//printf("Creating missing page %i\n", pageNum1);
			int a = ensureCapacity(pageNum1 + 1, ((BM_BufferPoolInfo *)bm->mgmtData)->fh);
		}
		bpool->search = findPage(bm->mgmtData, pageNum1);
		bpool->node = bpool->strtFrame;
		while(bpool->search)
		{
			if(bpool->search->pageNum == pageNum1)
			{
				bpool->node = ((BM_BufferPoolInfo *)bm->mgmtData)->search;
				break;
			}
			bpool->search = bpool->search->nxtFrame;
		}
		if(bpool->search != bpool->node || bpool->search == 0)
		{
			int empty = GetEmptyFrame(bm);
			if (empty != -1)
			{
				if(PoolSize(bm->mgmtData) == 0)
				{
					bpool->strtFrame = (PageFrame *)malloc(sizeof(PageFrame));
					bpool->strtFrame->bph = MAKE_PAGE_HANDLE();
					bpool->strtFrame->bph->data = (char *) malloc(PAGE_SIZE);
					int status = readBlock(pageNum1,bpool->fh,bpool->strtFrame->bph->data);
					if(status == RC_OK)
					{
						page->data = bpool->strtFrame->bph->data;
						page->pageNum = pageNum1;
						bpool->strtFrame->bph->pageNum = pageNum1;
						bpool->strtFrame->pageFrameNum = empty;
						bpool->strtFrame->is_dirty = 0;
						bpool->strtFrame->pageFrameCount = 1;
						bpool->strtFrame->freqCount = 1;
						bpool->strtFrame->fixcounts = 1;
						bpool->strtFrame->pageNum = pageNum1;
						bpool->strtFrame->nxtFrame = NULL;
						bpool->currFrame = bpool->strtFrame;
					}
					else
					{
						printf("Pin Page failed because of: %d \n", status);
						pthread_mutex_unlock(&mutex_pinPage); 
						return RC_BUFFER_POOL_ERROR_IN_PINPAGE;
					}
				}
				else
				{
					bpool->currFrame->nxtFrame = (PageFrame *)malloc(sizeof(PageFrame));
					bpool->currFrame = bpool->currFrame->nxtFrame;
					bpool->currFrame->bph = MAKE_PAGE_HANDLE();
					bpool->currFrame->bph->data = (char *) malloc(PAGE_SIZE);
					bpool->currFrame->nxtFrame = (PageFrame *)malloc(sizeof(PageFrame));
					int a = readBlock(pageNum1,bpool->fh,bpool->currFrame->bph->data);
					if(a == RC_OK)
					{
						page->data =bpool->currFrame->bph->data;
						page->pageNum = pageNum1;
						bpool->currFrame->bph->pageNum = pageNum1;
						bpool->currFrame->pageFrameNum = empty;
						bpool->currFrame->is_dirty = 0;
						bpool->currFrame->pageFrameCount = 1;
						bpool->currFrame->freqCount = 1;
						bpool->currFrame->fixcounts = 1;
						bpool->currFrame->pageNum = pageNum1;
						bpool->currFrame->nxtFrame = NULL;
						ChangeCount(bm->mgmtData);
						bpool->currFrame->pageFrameCount = 1;
					}
					else
					{
						printf("Pin Page failed because of: %d \n", a);
						// Unlock mutex while returning	
						pthread_mutex_unlock(&mutex_pinPage);
						return RC_BUFFER_POOL_ERROR_IN_PINPAGE;
					}
				}
			}
			else
			{
				int status;
				if(bm->strategy == RS_LFU)
				{
					//implementing LFU strategy
					status = LFU(bm, page, pageNum1);
				}
				else
				{
					//Implementing the Specific Strategy 
					status = replacementStrategy(bm, page, pageNum1);
				}
				if(status != RC_OK)
				{
					printf("Pin Page failed because of: %d \n", status);
					// Unlock mutex while returning
					pthread_mutex_unlock(&mutex_pinPage);
					return RC_BUFFER_POOL_ERROR_IN_PINPAGE;
				}
			}
		}
		else
		{
			bpool->search->fixcounts += 1;
			page->data = bpool->search->bph->data;
			page->pageNum = pageNum1;
			if(bm->strategy == RS_LRU)
			{
				ChangeCount(bm->mgmtData);
				bpool->search->pageFrameCount = 1;
			}
			if(bm->strategy == RS_LFU)
			{			
				bpool->search->freqCount += 1;
			}
			pthread_mutex_unlock(&mutex_pinPage);
			return RC_OK;
			
		}
		bpool->numReadIO += 1;
		pthread_mutex_unlock(&mutex_pinPage);
		return RC_OK;
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;
	}
	
}

/*---------------------------------------------------------------
Return the contents of the page frame
-----------------------------------------------------------------*/
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	int p;
	PageNumber *page=(PageNumber *)malloc(sizeof(PageNumber)*bm->numPages);	
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;	
	if(bpool != NULL)
	{
		bpool->node=bpool->strtFrame;
		if(bpool->node !=NULL)
		{							
			for(p=0;p< bm->numPages;p++)
			{
				page[p]=-1;
			}
			p=0;
			while(bpool->node != NULL)
			{
				page[p]=bpool->node->pageNum;
				p++;
				bpool->node=bpool->node->nxtFrame;				
			}			
			
		}
		else
		{
			return NO_PAGE;
		}			
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;	
	}
	
	return page;
}
/*----------------------------------------------------------------
Get and return Fix Counts of BufferPool
------------------------------------------------------------------*/
int *getFixCounts (BM_BufferPool *const bm)
{
	int f;
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;
	int *fcounts = (int *)malloc(sizeof(int)*(bm->numPages));		
	//check if the BufferPool is initialized
	if(bpool != NULL)
	{
		bpool->node=bpool->strtFrame;
		if(bpool->node !=NULL)
		{
									
			for(f=0;f< bm->numPages;f++)
			{
				fcounts[f]=0;
			}
			f=0;
			while(bpool->node != NULL)
			{
				fcounts[f]=bpool->node->fixcounts;
				f++;
				bpool->node=bpool->node->nxtFrame;				
			}			
		}
		else
		{
			return NO_PAGE;
		}			
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;	
	}
	return fcounts;
}

/*-------------------------------------------------------------
Get and return number of Read IO of BufferPool
--------------------------------------------------------------*/
int getNumReadIO (BM_BufferPool *const bm)
{
	if(bm->mgmtData != NULL)
	{
		return ((BM_BufferPoolInfo *)bm->mgmtData)->numReadIO;
	}
	else
	{
		return 0;
	}
	
}

/*-------------------------------------------------------------
Get and return number of Write IO of BufferPool
--------------------------------------------------------------*/
int getNumWriteIO (BM_BufferPool *const bm)
{
	if(bm->mgmtData != NULL)
	{
		return ((BM_BufferPoolInfo *)bm->mgmtData)->numWriteIO;
	}
	else
	{
		return 0;
	}
}

/*------------------------------------------------------------
Check if the page is dirty or not and return accordingly
--------------------------------------------------------------*/
bool *getDirtyFlags(BM_BufferPool *const bm)
{		
	int d;
	BM_BufferPoolInfo *bpool=(BM_BufferPoolInfo *)bm->mgmtData;
	bool *dirty = (bool *)malloc(sizeof(bool)*bm->numPages);	
	
	//check if the BufferPool is initialized
	if(bpool != NULL)
	{
		bpool->node=bpool->strtFrame;
		if(bpool->node !=NULL)
		{
										
			for(d=0;d< bm->numPages;d++)
			{
				dirty[d]=FALSE;
			}
			d=0;
			while(bpool->node != NULL)
			{
				dirty[d]=bpool->node->is_dirty;
				d++;
				bpool->node=bpool->node->nxtFrame;				
			}			
			
		}
		else
		{
			return NO_PAGE;
		}			
	}
	else
	{
		return RC_BUFFER_POOL_NOT_INIT;	
	}
	return dirty;
}




