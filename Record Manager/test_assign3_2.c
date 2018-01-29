#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"


#define ASSERT_EQUALS_RECORDS(_l,_r, schema, message)			\
  do {									\
    Record *_lR = _l;                                                   \
    Record *_rR = _r;                                                   \
    ASSERT_TRUE(memcmp(_lR->data,_rR->data,getRecordSize(schema)) == 0, message); \
    int i;								\
    for(i = 0; i < schema->numAttr; i++)				\
      {									\
        Value *lVal, *rVal;                                             \
		char *lSer, *rSer; \
        getAttr(_lR, schema, i, &lVal);                                  \
        getAttr(_rR, schema, i, &rVal);                                  \
		lSer = serializeValue(lVal); \
		rSer = serializeValue(rVal); \
        ASSERT_EQUALS_STRING(lSer, rSer, "attr same");	\
		free(lVal); \
		free(rVal); \
		free(lSer); \
		free(rSer); \
      }									\
  } while(0)

#define ASSERT_EQUALS_RECORD_IN(_l,_r, rSize, schema, message)		\
  do {									\
    int i;								\
    boolean found = false;						\
    for(i = 0; i < rSize; i++)						\
      if (memcmp(_l->data,_r[i]->data,getRecordSize(schema)) == 0)	\
	found = true;							\
    ASSERT_TRUE(0, message);						\
  } while(0)

#define OP_TRUE(left, right, op, message)		\
  do {							\
    Value *result = (Value *) malloc(sizeof(Value));	\
    op(left, right, result);				\
    bool b = result->v.boolV;				\
    free(result);					\
    ASSERT_TRUE(b,message);				\
   } while (0)

// test methods

static void testTombstoneImpl(void);
static void testPrimaryKeyImpl(void);
static void testCreateTableAndInsert (void);

// struct for test records
typedef struct TestRecord {
  int a;
  char *b;
  int c;
} TestRecord;

// helper methods
Record *testRecord(Schema *schema, int a, char *b, int c);
Schema *testSchema (void);
Record *fromTestRecord (Schema *schema, TestRecord in);

// test name
char *testName;

// main method
int 
main (void) 
{
  testName = "";  
  testCreateTableAndInsert();
  testTombstoneImpl();
 // testCreateTableAndInsert();
  testPrimaryKeyImpl();
  return 0;
}

void 
testTombstoneImpl(void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = { 
    {1, "aaaa", 3}, 
    {2, "bbbb", 2},
    {3, "cccc", 1},
    {4, "dddd", 3},
    {5, "eeee", 5},
    {6, "ffff", 1},
    {7, "gggg", 3},
    {8, "hhhh", 3},
    {9, "iiii", 2},
    {10, "jjjj", 5},
  };
  TestRecord updates[] = {
    {1, "iiii", 6},
    {2, "iiii", 6},
    {3, "iiii", 6},
    {6, "iiii", 6},
    {7, "iiii", 6},
    {9, "iiii", 6},
  };
  int deletes[] = {
    9,
    6,
    7,
    8,
    5,
    3
  };
  TestRecord finalR[] = {
    {1, "iiii", 6},
    {2, "iiii", 6},
    {3, "iiii", 6},
    {7, "iiii", 6},
    {5, "eeee", 5},
    {6, "iiii", 6},
    {9, "iiii", 6},
  };
  int numInserts = 10, numUpdates = 3, numDeletes = 6, numFinal = 7, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test creating a new table and insert,update,delete tuples";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);
  
  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(i = 0; i < numInserts; i++)
    {
      r = fromTestRecord(schema, inserts[i]);
      TEST_CHECK(insertRecord(table,r)); 
      rids[i] = r->id;
    }

  // delete rows from table
  for(i = 0; i < numDeletes; i++)
    {
      TEST_CHECK(deleteRecord(table,rids[deletes[i]]));
    }

  // update rows into table
  for(i = 0; i < numUpdates; i++)
    {
      r = fromTestRecord(schema, updates[i]);
      r->id = rids[i];
      TEST_CHECK(updateRecord(table,r)); 
    }
    
    r = fromTestRecord(schema, updates[3]);
    TEST_CHECK(insertRecord(table,r));
    rids[5] = r->id;

    r = fromTestRecord(schema, updates[4]);
    TEST_CHECK(insertRecord(table,r));
    rids[3] = r->id;

    r = fromTestRecord(schema, updates[5]);
    TEST_CHECK(insertRecord(table,r));
    rids[6] = r->id;

  TEST_CHECK(closeTable(table));
  TEST_CHECK(openTable(table, "test_table_r"));

  // retrieve records from the table and compare to expected final stage
  for(i = 0; i < numFinal; i++)
    {
      RID rid = rids[i];
      TEST_CHECK(getRecord(table, rid, r));
      ASSERT_EQUALS_RECORDS(fromTestRecord(schema, finalR[i]), r, schema, "compare records");
    }
  
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(table);
  TEST_DONE();
}

// ************************************************************ 
void
testCreateTableAndInsert (void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = { 
    {1, "aaaa", 3}, 
    {2, "bbbb", 2},
    {3, "cccc", 1},
    {4, "dddd", 3},
    {5, "eeee", 5},
    {6, "ffff", 1},
    {7, "gggg", 3},
    {8, "hhhh", 3},
    {9, "iiii", 2}
  };
  int numInserts = 9, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test creating a new table and inserting tuples";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);
  
  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(i = 0; i < numInserts; i++)
    {
      r = fromTestRecord(schema, inserts[i]);
      TEST_CHECK(insertRecord(table,r)); 
      rids[i] = r->id;
    }

  TEST_CHECK(closeTable(table));
  TEST_CHECK(openTable(table, "test_table_r"));

  // randomly retrieve records from the table and compare to inserted ones
  for(i = 0; i < 1000; i++)
    {
      int pos = rand() % numInserts;
      RID rid = rids[pos];
      TEST_CHECK(getRecord(table, rid, r));
      ASSERT_EQUALS_RECORDS(fromTestRecord(schema, inserts[pos]), r, schema, "compare records");
    }
  
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(rids);
  free(table);
  TEST_DONE();
}



void 
testPrimaryKeyImpl(void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = { 
    {1, "aaaa", 3}, 
    {2, "bbbb", 2},
    {3, "cccc", 1},
    {4, "dddd", 3},
    {5, "eeee", 5},
    {6, "ffff", 1},
    {7, "gggg", 3},
    {8, "hhhh", 3},
    {9, "iiii", 2},
    {10, "jjjj", 5},
  };

   TestRecord updates[] = {
    {11, "iiii", 6},
    {12, "iiii", 6},
    {5, "iiii", 6},
  };

  TestRecord finalR[] = {
    {11, "iiii", 6},
    {12, "iiii", 6},
    {3, "cccc", 1},
    {4, "dddd", 3},
    {5, "eeee", 5},
    {6, "ffff", 1},
    {7, "gggg", 3},
    {8, "hhhh", 3},
    {9, "iiii", 2},
    {10, "jjjj", 5},
  };
  int numInserts = 10, numUpdates = 2, numFinal = 7, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test creating a new table and insert,update,delete tuples";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);
  
  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(i = 0; i < numInserts; i++)
    {
      r = fromTestRecord(schema, inserts[i]);
      TEST_CHECK(insertRecord(table,r)); 
      rids[i] = r->id;
    }
  
  // update rows into table
  for(i = 0; i < numUpdates; i++)
    {
      r = fromTestRecord(schema, updates[i]);
      r->id = rids[i];
      TEST_CHECK(updateRecord(table,r)); 
    }

  r = fromTestRecord(schema, updates[2]);
  r->id = rids[2];
  int rc;
  rc = updateRecord(table,r);

  if( rc != RC_RM_PRIMARY_KEY_ALREADY_PRESENT_ERROR)
    TEST_CHECK(rc);
  
  TEST_CHECK(closeTable(table));
  TEST_CHECK(openTable(table, "test_table_r"));

  // retrieve records from the table and compare to expected final stage
  for(i = 0; i < numFinal; i++)
    {
      RID rid = rids[i];
      TEST_CHECK(getRecord(table, rid, r));
      ASSERT_EQUALS_RECORDS(fromTestRecord(schema, finalR[i]), r, schema, "compare records");
    }
  
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(table);
  TEST_DONE();
}

Schema *
testSchema (void)
{
  Schema *result;
  char *names[] = { "a", "b", "c" };
  DataType dt[] = { DT_INT, DT_STRING, DT_INT };
  int sizes[] = { 0, 4, 0 };
  int keys[] = {0};
  int i;
  char **cpNames = (char **) malloc(sizeof(char*) * 3);
  DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
  int *cpSizes = (int *) malloc(sizeof(int) * 3);
  int *cpKeys = (int *) malloc(sizeof(int));

  for(i = 0; i < 3; i++)
    {
      cpNames[i] = (char *) malloc(2);
      strcpy(cpNames[i], names[i]);
    }
  memcpy(cpDt, dt, sizeof(DataType) * 3);
  memcpy(cpSizes, sizes, sizeof(int) * 3);
  memcpy(cpKeys, keys, sizeof(int));

  result = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);

  return result;
}

Record *
fromTestRecord (Schema *schema, TestRecord in)
{
  return testRecord(schema, in.a, in.b, in.c);
}

Record *
testRecord(Schema *schema, int a, char *b, int c)
{
  Record *result;
  Value *value;

  TEST_CHECK(createRecord(&result, schema));

  MAKE_VALUE(value, DT_INT, a);
  TEST_CHECK(setAttr(result, schema, 0, value));
  freeVal(value);

  MAKE_STRING_VALUE(value, b);
  TEST_CHECK(setAttr(result, schema, 1, value));
  freeVal(value);

  MAKE_VALUE(value, DT_INT, c);
  TEST_CHECK(setAttr(result, schema, 2, value));
  freeVal(value);

  return result;
}
