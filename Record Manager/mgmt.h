#ifndef MGMT_H
#define MGMT_H

#include <stdlib.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "expr.h"

//All related information of PageFrame is stored
typedef struct PageFrame
{
	int pageFrameNum;
	int pageNum;
	BM_PageHandle *bph;
	bool is_dirty;
	int pageFrameCount;
	int freqCount;
	int fixcounts;
	struct PageFrame *nxtFrame;
}PageFrame;

//All related information of BufferPool is stored
typedef struct BM_BufferPoolInfo
{	
	int numReadIO;
	int numWriteIO;
	SM_FileHandle *fh;
	PageFrame *node;
	PageFrame *search;
	PageFrame *currFrame;	
	PageFrame *strtFrame;
}BM_BufferPoolInfo;

//Hashmap implementation for checking the primary key
typedef struct Record_PKey
{
    char *data;
    struct Record_PKey *nextkey;
} Record_PKey;

//New records created struct

typedef struct RM_RecordPoolInfo
{
    BM_BufferPool *bm;
    int *emptyPages;
    Record_PKey *arr_keys;
    Record_PKey *node;
    Record_PKey *curr;
} RM_RecordPoolInfo;

//scaning of pool struct
typedef struct RM_ScanPool
{
    Expr *condition;
    int page_current;
    Record *currRecord;
} RM_ScanPool;

#endif
