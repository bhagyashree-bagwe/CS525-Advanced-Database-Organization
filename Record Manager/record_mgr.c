#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "record_mgr.h"
#include "mgmt.h"


/******************************************************/
/* This method converts to string data type from Object data type 
   and writes it to a file */
/******************************************************/
char *SerializeSchema(Schema *sch)
{    
		int memNum = AssignRecodMemory(sch),i=10;
		char *output,rand[10];
		output = (char *)malloc(3*memNum);
		//sending the data to status structure var
		sprintf(output, "%d", sch->numAttr);
		strcat(output, "\n");		
		for(i = 0; i < sch->numAttr; i++)
		{
			strcat(output, sch->attrNames[i]);
			strcat(output, " ");
			sprintf(rand, "%d", sch->dataTypes[i]);
			strcat(output, rand);
			strcat(output, " ");
			sprintf(rand, "%d", sch->typeLength[i]);
			strcat(output, rand);
			strcat(output, "\n");
		}
		sprintf(rand, "%d", sch->keySize);
		strcat(output, rand);
		strcat(output, "\n");
		for(i = 0; i < sch->keySize; i++)
		{
			sprintf(rand, "%d", sch->keyAttrs[i]);
			strcat(output, rand);
			strcat(output, " ");
		}
		strcat(output, "\n");
		return output;
}

Schema *deserializeSchema(char *fname)
{
			char rand[10],*result;
			result = (char *)malloc(10);
			SM_PageHandle ph;
			SM_FileHandle fh;
			ph = (SM_PageHandle) malloc(PAGE_SIZE);
			int i, j,status;
			status=openPageFile(fname, &fh);
			if(status == RC_OK)
			{
				status=readFirstBlock(&fh, ph); 
				if(status == RC_OK)
				{
					sprintf(result, "%c", ph[0]);
					for(i = 1; i < strlen(ph); i++)
					{
						sprintf(rand, "%c", ph[i]);

						if(strcmp(rand,"\n") == 0)
							break;
						else
							strcat(result, rand);
					}
					int numAttr = atoi(result);
					char **attrNames = (char **) malloc(sizeof(char*) * numAttr);
					DataType *dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
					int *typeLength = (int *) malloc(sizeof(int) * numAttr);;
					for(j = 0; j < numAttr; j++)
					{
						free(result);
						result = (char *)malloc(10);
						int k = 1;
						for(i = i + 1; i < strlen(ph) ; i++)
						{
							sprintf(rand, "%c", ph[i]);
							if(strcmp(rand," ") == 0)
							{
								if(k == 1)
								{
									attrNames[j] = (char *) malloc(2);
									strcpy(attrNames[j], result);
								}
								else if(k == 2)
								{
									dataTypes[j] = atoi(result);
								}
								k++;
								free(result);
								result = (char *)malloc(10);
							}
							else if(strcmp(rand, "\n") == 0)
							{
								typeLength[j] = atoi(result);
								break;
							}
							else
							{
								sprintf(rand, "%c", ph[i]);
								strcat(result, rand);
							}
						}
					}
					free(result);
					result = (char *)malloc(10);
					for(i = i + 1; i < strlen(ph); i++)
					{
						sprintf(rand, "%c", ph[i]);

						if(strcmp(rand,"\n") == 0)
							break;
						else
							strcat(result, rand);
					}
					int keySize = atoi(result);
					int *keyAttrs = (int *) malloc(sizeof(int) *keySize);
					for(j = 0; j < keySize; j++)
					{
						free(result);
						result = (char *)malloc(10);

						for(i = i + 1; i < strlen(ph); i++)
						{
							sprintf(rand, "%c", ph[i]);

							if(strcmp(rand," ") == 0)
							{
								keyAttrs[j] = atoi(result);
							}
							if(strcmp(rand,"\n") == 0)
								break;
							else
								strcat(result, rand);
						}
					}
					Schema *updateSchema = (Schema *) malloc(sizeof(Schema));
					updateSchema->numAttr = numAttr;
					updateSchema->attrNames = attrNames;
					updateSchema->dataTypes = dataTypes;
					updateSchema->typeLength = typeLength;
					updateSchema->keyAttrs = keyAttrs;
					updateSchema->keySize = keySize;
					return updateSchema;
				}
			}
}


/***************************************************************/
/* This method initializes the Record Manager */
/***************************************************************/
extern RC initRecordManager (void *mgmtData)
{
		return RC_OK;
}

/***************************************************************/
/* This method closes the Record Manager */
/***************************************************************/
extern RC shutdownRecordManager ()
{
		return RC_OK;
}

/***************************************************************/
/* This method creates table as per the given schema */
/***************************************************************/
extern RC createTable (char *name, Schema *schema)
{
		char *schemaInfo;
		int status=createPageFile(name);
		if(status == RC_OK)
		{
			SM_FileHandle fh;
			status=openPageFile(name, &fh);
			if(status == RC_OK)
			{
				schemaInfo = SerializeSchema(schema);
				status=writeBlock(0, &fh, schemaInfo) ;
				if(status == RC_OK)
				{
					schemaInfo = NULL;
					free(schemaInfo);
					return RC_OK;
				}
			}        
		}
		return status;
}

/***************************************************************/
/* This method opens a table of given name */
/***************************************************************/
extern RC openTable (RM_TableData *rel, char *name)
{
		int status;
		RM_RecordPoolInfo *record = (RM_RecordPoolInfo *)malloc(sizeof(RM_RecordPoolInfo));
		record->bm = MAKE_POOL();
		record->bm->mgmtData = NULL;
		status=initBufferPool(record->bm, name, 4, RS_FIFO, NULL);
		if(status == RC_OK)
		{
			record->emptyPages = (int *) malloc(sizeof(int));
			record->emptyPages[0] = ((BM_BufferPoolInfo *)(record->bm)->mgmtData)->fh->totalNumPages;

			rel->name = name;
			rel->mgmtData = record;
			rel->schema = deserializeSchema(name);
			return RC_OK;
		}
		return status;
}

/***************************************************************/
/* This method closes the table
/***************************************************************/
extern RC closeTable (RM_TableData *rel)
{
		  RM_RecordPoolInfo *record=(RM_RecordPoolInfo *)rel->mgmtData;
		//Close the bufferpool
		int status=shutdownBufferPool(record->bm);
		if(status == RC_OK)
		{   
		   	//release all memory
			record->bm->mgmtData = NULL;
			record->bm = NULL;
			record->emptyPages = NULL;
			free(record->emptyPages);
			record->node = record->arr_keys;
			while(record->node != NULL)
			{
				record->node->data = NULL;
				free(record->node->data);
				record->node=record->node->nextkey;
			}
			record->arr_keys = NULL;
			free(record->arr_keys);
			rel->mgmtData = NULL;
			free(rel->mgmtData);
			rel->schema = NULL;
			free(rel->schema);
			return RC_OK;
		}
		return status;
}

/***************************************************************/
/* This method deletes the table
/***************************************************************/
extern RC deleteTable (char *name)
{
		int status=destroyPageFile(name); 
		if(status == RC_OK)
			return RC_OK;
		return status;
}

/***************************************************************/
/* This method returns the number of tuples present in table
/***************************************************************/
extern int getNumTuples (RM_TableData *rel)
{
		RM_RecordPoolInfo *record=(RM_RecordPoolInfo *)rel->mgmtData;
		Record *rec = (Record *)malloc(sizeof(Record));
		RID recordid;
		recordid.page = 1;
		recordid.slot = 0;
		int status,c = 0;
		while(recordid.page < ((BM_BufferPoolInfo *)(record->bm)->mgmtData)->fh->totalNumPages && recordid.page > 0)// Check for the condition till the end of the table
		{
			if(getRecord (rel, recordid, rec) == RC_OK)
			{ 
				recordid.slot= 0;
				c++; //incerement record counter
				(recordid.page)++; //increment page id
			}
		}
		rec = NULL;
		free(rec);
		return c;// return the record counter
}


/***************************************************************/
/* This method returns the memory needed by schema */
/***************************************************************/
int AssignRecodMemory(Schema *sch)
{
			int i, mem = 0;
			//assign new memory as required
			for(i = 0; i < sch->numAttr; i++)
			{
				if(sch->dataTypes[i] == DT_INT)
					mem += sizeof(int);
				else if(sch->dataTypes[i] == DT_FLOAT)
					mem += sizeof(float);
				else if(sch->dataTypes[i] == DT_BOOL)
					mem += sizeof(bool);
				else
					mem += sch->typeLength[i];
			}
			return mem;//return memory
}

/***************************************************************/
/* This method checks if the primary Key exists */
/***************************************************************/
RC validatePKey(RM_TableData *rel, Record *record, int ops)
{
		RM_RecordPoolInfo *rec=(RM_RecordPoolInfo *)rel->mgmtData;
		int status;
		Value *value;
		char *serialize_valued;
		rec->node = rec->arr_keys;
		status = getAttr (record, rel->schema, rel->schema->keyAttrs[0], &value);
		if(status == RC_OK)
		{
			serialize_valued = serializeValue(value);
			while(rec->node != NULL)
			{
				if(strcmp(serialize_valued, rec->node->data) == 0)
					return 1;
				rec->node = rec->node->nextkey;
			}
		}
		if(ops == 1)
		{
			if(rec->emptyPages[0] == 1)
			{
				rec->arr_keys = (Record_PKey *)malloc(sizeof(Record_PKey));
				rec->arr_keys->data = (char *)malloc(strlen(serialize_valued));
				strcpy(rec->arr_keys->data, serialize_valued);
				rec->arr_keys->nextkey = NULL;
				rec->curr = rec->arr_keys;
			}
			else
			{
				rec->curr->nextkey = (Record_PKey *)malloc(sizeof(Record_PKey));
				rec->curr = rec->curr->nextkey;
				rec->curr->data = (char *)malloc(strlen(serialize_valued));
				strcpy(rec->curr->data, serialize_valued);
				rec->curr->nextkey = NULL;
			}
		}
		return 0;
}

/***************************************************************/
/* This method deletes the primary key */
/***************************************************************/
RC removePKey(RM_TableData *rel, Record *record)
{
		RM_RecordPoolInfo *record1=(RM_RecordPoolInfo *)rel->mgmtData;
		Value *value;
		char *serialize_valued;
		record1->node = record1->arr_keys;
		if(getAttr(record, rel->schema, rel->schema->keyAttrs[0], &value) == RC_OK)
		{
			serialize_valued = serializeValue(value);
			while(record1->node != NULL)
			{
				if(strcmp(serialize_valued, record1->node->data) == 0)
				{
					record1->node->data = "@@@@"; //Check data with '@'
					return 1;
				}
				record1->node = record1->node->nextkey;
			}
		}
		return 0;
}
/***************************************************************/
/* This method scans the table
/***************************************************************/
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
		if (rel == NULL)
			return RC_RM_NO_TABLE_INIT;
		//initialize record
		RM_ScanPool *spool;
		spool = (RM_ScanPool *)malloc(sizeof(RM_ScanPool));
		spool->currRecord=(Record *)malloc(sizeof(Record));
		spool->condition = cond;
		spool->page_current = 1;
		scan->rel = rel;
		scan->mgmtData = spool;
		return RC_OK;
}

/***************************************************************/
/* This method returns the next record that satisfies the provided scan condition */
/***************************************************************/
RC next (RM_ScanHandle *scan, Record *record)
{
		RM_ScanPool *spool=((RM_ScanPool *)scan->mgmtData);
		Value *output;
		RID recordid;
		int status;
		recordid.page = spool->page_current;
		recordid.slot = 0;
		if(spool->condition == NULL)
		{
			while(recordid.page > 0 && recordid.page < ((BM_BufferPoolInfo *)(((RM_RecordPoolInfo *)(scan->rel)->mgmtData)->bm)->mgmtData)->fh->totalNumPages)
			{
				status = getRecord (scan->rel, recordid, spool->currRecord);
				if(status == RC_OK) //copy record if true
				{
					record->data = spool->currRecord->data;
					record->id = spool->currRecord->id;
					spool->page_current =spool->page_current + 1;
				  
					recordid.page = spool->page_current;
					  recordid.slot = 0;
					return RC_OK;
				}
				return status;
			}
		}
		else
		{
			while(recordid.page > 0 && recordid.page < ((BM_BufferPoolInfo *)(((RM_RecordPoolInfo *)(scan->rel)->mgmtData)->bm)->mgmtData)->fh->totalNumPages)
			{        
				status = getRecord (scan->rel, recordid, (spool->currRecord));
				status = evalExpr (spool->currRecord, scan->rel->schema,spool->condition, &output);
				if(output->dt == DT_BOOL && output->v.boolV)
				{
					record->data = spool->currRecord->data;
					record->id = spool->currRecord->id;
					spool->page_current=spool->page_current+1 ;
					return RC_OK;
				}
				else
				{
					spool->page_current=spool->page_current+1 ;					
					recordid.page = spool->page_current;
					recordid.slot=0;
				}
			}
		}
		spool->page_current= 1;
		return RC_RM_NO_MORE_TUPLES;
}

/***************************************************************/
/* This method makes the scan data null */
/***************************************************************/
extern RC closeScan (RM_ScanHandle *scan)
{   
		RM_ScanPool *spool=((RM_ScanPool *)scan->mgmtData);
		//free the memory
		spool->currRecord = NULL;    
		free(spool->currRecord);
		scan->mgmtData = NULL;
		free(scan->mgmtData);   	   
		scan = NULL;
		free(scan);
		return RC_OK;
}

/***************************************************************/
/* This method returns the size of provided record */
/***************************************************************/
extern int getRecordSize (Schema *schema)
{   
		int mem = (AssignRecodMemory(schema))/2;
		return mem;
}

/***************************************************************/
/* This method creates a schema */
/***************************************************************/
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{   
   
		Schema *updateSchema = (Schema *) malloc(sizeof(Schema));
		updateSchema->numAttr = numAttr;
		updateSchema->attrNames = attrNames;
		updateSchema->dataTypes = dataTypes;
		updateSchema->typeLength = typeLength;
		updateSchema->keyAttrs = keys;
		updateSchema->keySize = keySize;
		return updateSchema;
}

/***************************************************************/
/* Free all schema attributes */
/***************************************************************/
extern RC freeSchema (Schema *schema)
{   
		schema->numAttr = NULL;
		schema->attrNames = NULL;
		schema->dataTypes = NULL;
		schema->typeLength = NULL;
		schema->keyAttrs = NULL;
		schema->keySize = NULL;	  
		schema = NULL;
		free(schema);
		return RC_OK;
}

/***************************************************************/
/* This method inserts a new record in the table */
/***************************************************************/
extern RC insertRecord (RM_TableData *rel, Record *record)
{
		RM_RecordPoolInfo *rec=(RM_RecordPoolInfo *)rel->mgmtData;
		//compare key with hashMap and insert if not found
		if(validatePKey(rel, record, 1)== 1)
			return RC_RM_PRIMARY_KEY_ALREADY_PRESENT_ERROR; 
		 Record *r = (Record *)malloc(sizeof(Record));
		 RID recordid;
		recordid.page = 1;
		recordid.slot = 0;
		while(recordid.page > 0 && recordid.page < ((BM_BufferPoolInfo *)(rec->bm)->mgmtData)->fh->totalNumPages)
		{
			if(getRecord (rel, recordid, r) == RC_OK)
			{   
				if(strncmp(r->data, "deleted:", 7) == 0)
					break;
				recordid.slot = 0;
				 recordid.page = recordid.page + 1;
			}
		}
		r = NULL;
		free(r); //free the memory
		rec->emptyPages[0] = recordid.page; // set the page number
		BM_PageHandle *page = MAKE_PAGE_HANDLE();//making page
		//pinning page 
		int status=pinPage(rec->bm, page, rec->emptyPages[0]);
		if(status == RC_OK)
		{        
			memset(page->data, '\0', strlen(page->data));
			sprintf(page->data, "%s", record->data);        
			status=markDirty(rec->bm, page);
			if(status == RC_OK)
			{   
			   //unpin the page
				 status=unpinPage(rec->bm, page);
				if(status == RC_OK)
				{  
				status=forcePage(rec->bm, page);
					if(status == RC_OK)
					{
						record->id.page = rec->emptyPages[0];
						record->id.slot = 0;
						printf("record data: %s\n", record->data);
					   	// page = NULL;
						free(page); //free page
						rec->emptyPages[0] += 1;
						return RC_OK;
					}
				}
			}
		}
		return status;
}

/***************************************************************/
/* This method deletes the given record from the table */
/***************************************************************/
extern RC deleteRecord (RM_TableData *rel, RID id)
{
		RM_RecordPoolInfo *rec=(RM_RecordPoolInfo *)rel->mgmtData;
		char tombstone[8] = "deleted:"; //soft delete for Tombstones implementation
		char *rand = MAKE_PAGE_HANDLE();  
		int i,status;
		if(id.page > 0 && id.page <=  ((BM_BufferPoolInfo *)(rec->bm)->mgmtData)->fh->totalNumPages)
		{
			BM_PageHandle *page = MAKE_PAGE_HANDLE();
			status = pinPage(rec->bm, page, id.page);//pin page
			if(status == RC_OK)
			{
				Record *r = (Record *)malloc(sizeof(Record));
				RID recordid;
				recordid.page = id.page;
				recordid.slot = id.slot;
				int status = getRecord (rel, recordid, r); //fetch record
				if(status == RC_OK)
				{   
					status = removePKey(rel, r); // delete key from hash map
					if(status == 0)
						return RC_RM_PRIMARY_KEY_DELETE_ERROR;
				}
				r = NULL;
				free(r);
				//soft delete of record
				strcpy(rand, tombstone);
				strcat(rand, page->data); //set tombstone for tombstone implementation
				memset(page->data, '\0', strlen(page->data));
				sprintf(page->data, "%s", rand); //copy the current data
				status = markDirty(rec->bm, page);//marking the page dirty
				if(status == RC_OK)
				{
					status = unpinPage(rec->bm, page);//unpin page
					if(status == RC_OK)
					{
						status = forcePage(rec->bm, page);
						if(status == RC_OK)
						{
						   // page = NULL;
							free(page);//free page
							return RC_OK;
						}
					}
				}
			}
			return status;
		}
		return RC_RM_NO_RECORD_FOUND; //Record doesnot exist 
}

/***************************************************************/
/* This method updates the given record's data */
/***************************************************************/
extern RC updateRecord (RM_TableData *rel, Record *record)
{
		RM_RecordPoolInfo *rec=(RM_RecordPoolInfo *)rel->mgmtData;
		int status;
		if(record->id.page > 0 && record->id.page <=  ((BM_BufferPoolInfo *)(rec->bm)->mgmtData)->fh->totalNumPages)
		{
			BM_PageHandle *page = MAKE_PAGE_HANDLE();
			status = pinPage(rec->bm, page, record->id.page);
			if(status == RC_OK)
			{
				//check Key with hash map
				Record *r = (Record *)malloc(sizeof(Record));
				RID recordid;
				recordid.page = record->id.page; //set page id
				recordid.slot = record->id.slot; //set slot id
				status = getRecord (rel, recordid, r); //fetch data from record
				Value *lVal, *rVal;
				char *lSer, *rSer;
				status = getAttr (r, rel->schema, rel->schema->keyAttrs[0], &lVal); // get the primary key attribute of the current location
				status = getAttr (record, rel->schema, rel->schema->keyAttrs[0], &rVal); // get the primary key attribute of the record to insert
				lSer = serializeValue(lVal); // serialize
				rSer = serializeValue(rVal); // serialize
				if(strcmp(lSer, rSer) != 0)
				{
					if(strncmp(r->data, "deleted:", 7) == 0)
						return RC_RM_UPDATE_ON_DELETE_RECORD_ERROR;
					status = validatePKey(rel, record, 2);
					if(status == 1)
						return RC_RM_PRIMARY_KEY_ALREADY_PRESENT_ERROR; //primary key exist
				}
				r = NULL;
				free(r);
				memset(page->data, '\0', strlen(page->data));
				sprintf(page->data, "%s", record->data);
				status = markDirty(rec->bm, page);//mark the page dirty
				if(status == RC_OK)
				{
					status = unpinPage(rec->bm, page);// unpin the page
					if(status == RC_OK)
					{
						status = forcePage(rec->bm, page);
						if(status == RC_OK)
						{
							free(page); //free page
							return RC_OK;
						}
					}
				}
			}
			return status;
		}
		return RC_RM_NO_RECORD_FOUND;
}

/***************************************************************/
/* This method fetches all the records from the table */
/***************************************************************/
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
		RM_RecordPoolInfo *rec=(RM_RecordPoolInfo *)rel->mgmtData;
		int status;
		if(id.page > 0 && id.page <=  ((BM_BufferPoolInfo *)(rec->bm)->mgmtData)->fh->totalNumPages)
		{
			BM_PageHandle *page = MAKE_PAGE_HANDLE();//initialize the page
			status = pinPage(rec->bm, page, id.page);//pin page
			if(status == RC_OK)
			{   //store the record data and id
				record->id = id;
				record->data = page->data;
				status = unpinPage(rec->bm, page);//unpin page
				if(status == RC_OK)
				{
					free(page);//free page
					return RC_OK;
				}
			}
			return status;
		}
		return RC_RM_NO_RECORD_FOUND;
}

/***************************************************************/
/* This method creates a new record */
/***************************************************************/
extern RC createRecord (Record **record, Schema *schema)
{
		int mem = AssignRecodMemory(schema);// get the required number of byte required for the record
		*record = (Record *)malloc(sizeof(Record));//allocate memory
		record[0]->data = (char *)malloc(mem + schema->numAttr + 1);
		int i;
		sprintf(record[0]->data, "%s", "(");
		for(i = 0; i < schema->numAttr - 1; i++)
		{
			strcat(record[0]->data,",");
		}
		strcat(record[0]->data,")");		
		return RC_OK;
}

/***************************************************************/
/* This method frees the memory used by records */
/***************************************************************/
extern RC freeRecord (Record *record)
{
		record->data = NULL;
		free(record->data);
		record = NULL;
		free(record);
		return RC_OK;
}

/***************************************************************/
/* This method fetches record's attributes */
/***************************************************************/
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
		int mem = AssignRecodMemory(schema);
		int i;
		char *previous, *output,rand[1000];
		if(attrNum < schema->numAttr)
		{
			previous = (char *)malloc(mem);
			output = (char *)malloc(schema->typeLength[attrNum]);
			if(attrNum == 0)
			{
				sprintf(previous, "%s", "(");
				sprintf(output, "%c", record->data[1]);
				for(i = 2; i < strlen(record->data); i++)
				{
					if(record->data[i] == ',')
						break;
					sprintf(rand, "%c", record->data[i]);
					strcat(output, rand);
				}
			}
			else if(attrNum > 0 && attrNum < schema->numAttr)
			{
				int reqNumCommas = attrNum, numCommas = 0;
				sprintf(previous, "%s", "(");
				for(i = 1; i < strlen(record->data); i++)
				{
					if(numCommas == reqNumCommas)
					{
						if(record->data[i] == ',' || record->data[i] == ')')
							break;
						sprintf(rand, "%c", record->data[i]);
						strcat(output, rand);
						continue;
					}
					if(record->data[i] == ',')
					{
						sprintf(output, "%c", record->data[++i]);
						numCommas++;
					}
					sprintf(rand, "%c", record->data[i]);
					strcat(previous, rand);
				}
			}
			Value *values = (Value*) malloc(sizeof(Value));
			if(schema->dataTypes[attrNum] == DT_INT)
				values->v.intV = atoi(output);
			else if(schema->dataTypes[attrNum] == DT_FLOAT)
				values->v.floatV = atof(output);
			else if(schema->dataTypes[attrNum] == DT_BOOL)
				values->v.boolV = (bool) *output;
			else
				values->v.stringV = output;
			values->dt = schema->dataTypes[attrNum];
			value[0] = values;
			previous = NULL;
			free(previous);
			output = NULL;
			free(output);
			return RC_OK;
		}
		return RC_RM_NO_MORE_TUPLES;
}

/***************************************************************/
/* This method sets attributes of record */
/***************************************************************/
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
		char rand[1000];
		char *previous, *post;
		int i;
		int mem = AssignRecodMemory(schema);
		previous = (char *)malloc(mem);
		post = (char *)malloc(mem);
		if(attrNum < schema->numAttr)
		{
			if(attrNum == 0)
			{
				sprintf(previous, "%s", "(");
				for(i = 1; i < strlen(record->data); i++)
				{
					if(record->data[i] == ',')
						break;
					sprintf(rand, "%c", record->data[i]);
					strcat(previous, rand);
				}
				sprintf(post, "%s", ",");
				for( i = i + 1; i < strlen(record->data); i++)
				{
					sprintf(rand, "%c", record->data[i]);
					strcat(post, rand);
				}
			}
			else if(attrNum > 0 && attrNum < schema->numAttr)
			{
				int reqNumCommas = attrNum, numCommas = 0;
				sprintf(previous, "%s", "(");
				for(i = 1; i < strlen(record->data); i++)            {
					if(numCommas > reqNumCommas)
						break;
					if(numCommas == reqNumCommas)
					{
						if(record->data[i] == ',')
							numCommas++;
						continue;
					}
					if(record->data[i] == ',')
						numCommas++;
					sprintf(rand, "%c", record->data[i]);
					strcat(previous, rand);
				}
				if(attrNum != (schema->numAttr - 1))
				{
					sprintf(post, "%s", ",");
					for( ; i < strlen(record->data); i++)
					{
						sprintf(rand, "%c", record->data[i]);
						strcat(post, rand);
					}
				}
				else
					sprintf(post, "%s", ")");
			}
			//Check type of attribute 
			if(schema->dataTypes[attrNum] == DT_INT)       
				sprintf(rand, "%d", value->v.intV);
			else if(schema->dataTypes[attrNum] == DT_FLOAT)		  
				sprintf(rand, "%f", value->v.floatV);
			else if(schema->dataTypes[attrNum] == DT_BOOL)		  
				sprintf(rand, "%d", value->v.boolV);
			else
				strcpy(rand, value->v.stringV);					  
			strcpy(record->data, previous);
			strcat(record->data, rand);
			strcat(record->data, post);
			previous = NULL;
			post = NULL;
			free(previous);
			free(post);
			return RC_OK;
		}
		return RC_RM_NO_MORE_TUPLES;
}
