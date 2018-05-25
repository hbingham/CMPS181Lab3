#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>



#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = NULL;


IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}


RC IndexManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return ERROR;
    return SUCCESS;


}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), ixfileHandle.fh);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    
    return _pf_manager->closeFile(ixfileHandle.fh);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    unsigned numPages = ixfileHandle.fh.getNumberOfPages();
    if(numPages == 0)
    {
	    void * rootPageData = calloc(PAGE_SIZE, 1);
	    void * leafPageData = calloc(PAGE_SIZE, 1);
	    if ((rootPageData == NULL) || (leafPageData == NULL))
		return ERROR;

	    newNonLeafPage(rootPageData, 0, 1);
   	    newLeafPage(leafPageData, 1);

	    if (ixfileHandle.fh.appendPage(rootPageData))
		return ERROR;
            if (ixfileHandle.fh.appendPage(leafPageData))
                return ERROR;
	    ixfileHandle.ixAppendPageCounter = ixfileHandle.ixAppendPageCounter + 2;
	    free(rootPageData);
	    free(leafPageData);

	    numPages = 2;
    }
    unsigned rootPageNum = 0;
    insertEntryRec(rootPageNum, ixfileHandle, attribute, key, rid, NULL);
    ixfileHandle.copyCounterValues();
    return SUCCESS;
}


RC IndexManager::insertEntryRec(unsigned pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void * copyKey)
{
    void * pageData = calloc(PAGE_SIZE, 1);
    ixfileHandle.fh.readPage(pageNum, pageData);
    indexDirectoryHeader indexHeader = getIndexDirectoryHeader(pageData);

    if(!indexHeader.isLeaf)
    {
	   unsigned nextPage = getChildPage(pageData, key, indexHeader, attribute);
	   RC retVal = insertEntryRec(nextPage, ixfileHandle, attribute, key, rid, NULL);
	   if (retVal < 0)
	   {
              unsigned freeSpace = getTotalFreeSpace(pageData, indexHeader);
              unsigned insertSize = getKeySize(key, attribute);
              if (insertSize + (INT_SIZE) <= freeSpace )
              {
                   prepPage(pageData, copyKey, indexHeader, attribute, rid, -retVal);
                   ixfileHandle.fh.writePage(pageNum, pageData);
              }
              else
              {
                   int newPage = splitPage(pageData, copyKey, attribute, ixfileHandle, copyKey, indexHeader, pageNum, rid);
                   return newPage * -1;
              }

	   }
    }
    else
    {
	   unsigned freeSpace = getTotalFreeSpace(pageData, indexHeader);
	   unsigned insertSize = getKeySize(key, attribute);
	   if (insertSize + (INT_SIZE * 2) <= freeSpace )
	   {
	   	prepPage(pageData, key, indexHeader, attribute, rid, 0);
           	ixfileHandle.fh.writePage(pageNum, pageData);
	   }
	   else
	   {
		int newPage = splitPage(pageData, key, attribute, ixfileHandle, copyKey, indexHeader, pageNum, rid);
		return newPage * -1;
	   }
    }
    return SUCCESS;
}



RC IndexManager::splitPage(void * page, const void *key, const Attribute &attribute, IXFileHandle &handle, void * copyKey, indexDirectoryHeader header, unsigned pageNum, const RID &rid)
{
	if (!header.isLeaf) return splitPageLeaf(page, key, attribute, handle, copyKey, header, pageNum, rid);
	else return splitPageNonLeaf(page, key, attribute, handle, copyKey, header, pageNum);
}


RC IndexManager::splitPageLeaf(void * page, const void *key, const Attribute &attribute, IXFileHandle &handle, void * copyKey, indexDirectoryHeader header, unsigned pageNum, const RID &rid)
{
   unsigned insertSlot = findInsertionSlot(page, key, header, attribute);
   void * smallPage= calloc(PAGE_SIZE, 1);
   void * bigPage = calloc(PAGE_SIZE, 1);
   unsigned insertPageOffset, readPageOffset, keySize;
   unsigned pageCount = handle.fh.getNumberOfPages();

   insertPageOffset = PAGE_SIZE - (INT_SIZE) * 2;
   newLeafPage(smallPage, pageNum);
   newLeafPage(bigPage, pageCount);

   memcpy((char*) smallPage + PAGE_SIZE - INT_SIZE, (char*) page + PAGE_SIZE - INT_SIZE, INT_SIZE);
   memcpy((char*) smallPage + PAGE_SIZE - (INT_SIZE * 2), &pageCount, INT_SIZE);
   memcpy((char*) bigPage + PAGE_SIZE - INT_SIZE, &pageNum, INT_SIZE);

   unsigned appendCount = 0;
   for (unsigned i = 0; i <= header.nodeCount; i++)
   {
           if(i == floor((header.nodeCount +1)/2))
           {
                insertPageOffset = PAGE_SIZE - (INT_SIZE * 2);
           }
           if(i < (floor((header.nodeCount + 1)/2)))
           {
              if (i == insertSlot)
              {
                  keySize = copyOverKey(key, smallPage, 0, insertPageOffset, attribute);
                  insertPageOffset = insertPageOffset - keySize;
                  memcpy((char*) smallPage + insertPageOffset - (INT_SIZE) * 2, &rid, INT_SIZE);
                  insertPageOffset = insertPageOffset - INT_SIZE * 2;
                  continue;
              }
              readPageOffset = getKeyOffset(page, appendCount);
              keySize = copyOverKey(page, smallPage,  readPageOffset, insertPageOffset,attribute);
              insertPageOffset = insertPageOffset - keySize;
              appendCount++;
              memcpy((char*) smallPage + insertPageOffset - (INT_SIZE*2), (char*) page + readPageOffset - (INT_SIZE * 2), INT_SIZE * 2); 
              insertPageOffset = insertPageOffset - (INT_SIZE * 2);
           }
           else
           {
              if (i == insertSlot)
              {
                  keySize = copyOverKey(key, smallPage, 0, insertPageOffset, attribute);
                  insertPageOffset = insertPageOffset - keySize;
                  memcpy((char*) smallPage + insertPageOffset - (INT_SIZE) * 2, &rid, INT_SIZE);
                  insertPageOffset = insertPageOffset - INT_SIZE * 2;
                  continue;
              }
              readPageOffset = getKeyOffset(page, appendCount);
              keySize = copyOverKey(page, bigPage,  readPageOffset, insertPageOffset,attribute);
              insertPageOffset = insertPageOffset - keySize;
              appendCount++;
              memcpy((char*) bigPage + insertPageOffset - (INT_SIZE*2), (char*) page + readPageOffset - (INT_SIZE * 2), INT_SIZE * 2); 
              insertPageOffset = insertPageOffset - (INT_SIZE * 2);
           }

   }
   handle.fh.writePage(pageNum, smallPage);
   handle.fh.appendPage(bigPage);
   return pageCount;
}


RC IndexManager::splitPageNonLeaf(void * page, const void *key, const Attribute &attribute, IXFileHandle &handle, void * copyKey, indexDirectoryHeader header, unsigned pageNum)
{
   unsigned insertSlot = findInsertionSlot(page, key, header, attribute);
   void * smallPage= calloc(PAGE_SIZE, 1);
   void * bigPage = calloc(PAGE_SIZE, 1);
   unsigned insertPageOffset, readPageOffset, keySize;
   unsigned pageCount = handle.fh.getNumberOfPages();
   insertPageOffset = PAGE_SIZE - INT_SIZE;
   newNonLeafPage(smallPage, pageNum, 0);
   newNonLeafPage(bigPage, pageCount, 0);

   unsigned zero = 0;

   unsigned appendCount = 0;
   for (unsigned i = 0; i <= header.nodeCount; i++)
   {
           if(i == 0)
           {
                // copy page 0 from original page to small page
                memcpy((char*) smallPage + PAGE_SIZE - INT_SIZE, (char*) page + PAGE_SIZE - INT_SIZE, INT_SIZE);
           }
           if(i == floor((header.nodeCount +1)/2))
           {
		insertPageOffset = PAGE_SIZE - INT_SIZE;
		if(i == insertSlot)
		{
		   memcpy((char*) bigPage + PAGE_SIZE - INT_SIZE, &pageCount, INT_SIZE);
		   copyOverKey(key, copyKey, zero, zero, attribute);
		   continue;
		}
		else
		{
		   readPageOffset = getKeyOffset(page, appendCount);
                   memcpy((char*) bigPage + PAGE_SIZE - INT_SIZE, (char*) page + readPageOffset - INT_SIZE, INT_SIZE);
		   readPageOffset = getKeyOffset(page, appendCount);
		   copyOverKey(page, copyKey, readPageOffset, zero, attribute);
		   appendCount++;
		   continue;
		}
           }
	   if(i < (floor((header.nodeCount + 1)/2)))
           {
	      if (i == insertSlot)
	      {
		  keySize = copyOverKey(key, smallPage, 0, insertPageOffset, attribute);
		  insertPageOffset = insertPageOffset - keySize;
		  memcpy((char*) smallPage + insertPageOffset - (INT_SIZE), &pageCount, INT_SIZE);
		  insertPageOffset = insertPageOffset - INT_SIZE;
		  continue;
	      }
              readPageOffset = getKeyOffset(page, appendCount);
              keySize = copyOverKey(page, smallPage,  readPageOffset, insertPageOffset,attribute);
              insertPageOffset = insertPageOffset - keySize;
              appendCount++;
              memcpy((char*) smallPage + insertPageOffset - (INT_SIZE), (char*) page + readPageOffset - (INT_SIZE), INT_SIZE);
              insertPageOffset = insertPageOffset - INT_SIZE;
	   }
	   else
	   {
              if (i == insertSlot)
              {
                  keySize = copyOverKey(key, bigPage, 0, insertPageOffset, attribute);
                  insertPageOffset = insertPageOffset - keySize;
                  memcpy((char*) bigPage + insertPageOffset - (INT_SIZE), &pageCount, INT_SIZE);
                  insertPageOffset = insertPageOffset - INT_SIZE;
                  continue;
              }
              readPageOffset = getKeyOffset(page, appendCount);
              keySize = copyOverKey(page, bigPage,  readPageOffset, insertPageOffset,attribute);
              insertPageOffset = insertPageOffset - keySize;
              appendCount++;
              memcpy((char*) bigPage + insertPageOffset - (INT_SIZE), (char*) page + readPageOffset - (INT_SIZE), INT_SIZE);
              insertPageOffset = insertPageOffset - INT_SIZE;
	   }
   }
   handle.fh.writePage(pageNum, smallPage);
   handle.fh.appendPage(bigPage);
   return pageCount;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool		lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return SUCCESS;
}


//THINGS I CHANGED
/*

in ix.h: 
  replace these
    void getKeyAtOffset(void * page, void *key, unsigned offset, const Attribute &attribute) const;
    void getKeyAtSlot(void * page, void *key, unsigned slot, const Attribute &attribute) const; //<=== THIS CONST
    (just added const, need to do that in the ix.cc as well)







*/
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const 
{
    /*
{
"keys":["P"],
"children":[
  {"keys":["C","G","M"],
  "children": [
    {"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]},
    {"keys": ["D:[(3,1),(3,2)]","E:[(4,1)]","F:[(5,1)]"]},
    {"keys": ["J:[(5,1),(5,2)]","K:[(6,1),(6,2)]","L:[(7,1)]"]},
    {"keys": ["N:[(8,1)]","O:[(9,1)]"]}
]},
{"keys":["T","X"],
  "children": [
    {"keys": ["Q:[(10,1)]","R:[(11,1)]","S:[(12,1)]"]},
    {"keys": ["U:[(13,1)]","V:[(14,1)]"]},
    {"keys": ["Y:[(15,1)]","Z:[(16,1)]"]}
]}
]
}
*/
  void *pageData = calloc(PAGE_SIZE, 1);  
  ixfileHandle.fh.readPage(0 , pageData);
  indexDirectoryHeader indexHeader;
  memcpy (&indexHeader, pageData, sizeof(indexDirectoryHeader));
  free(pageData);
  /*
  cout << "NODECOUNT: " << indexHeader.nodeCount << endl;
  cout << "isLeaf: " << indexHeader.isLeaf << endl;
  cout << "freeSpaceOffset: " << indexHeader.freeSpaceOffset << endl;
  */

  void *page = malloc(PAGE_SIZE);
  void *key;

  //void *page = malloc(PAGE_SIZE);
  cout << "{\n";
  //If it is a leaf, we start printing keys.
  
  if(indexHeader.isLeaf)
  {
        cout << "{\"keys\":[";
        //iterate through all the keys in the leaf.
        for(int i = 0; i < indexHeader.nodeCount; i++)
        {

          //Print each key.
          //ex:, cout << "\"P\"; 
          if(i != indexHeader.nodeCount - 1)
          {
            cout << ",";
          }
        }
        cout << "]}";



  }
   //If it's not a leaf, we want to walk down each of the children
  else if(!indexHeader.isLeaf)
  {
    //Testing
    cout << "{\"keys\":[";
    //cout << indexHeader.nodeCount;
    
    //iterate through all the keys in the leaf.
    for(int i = 0; i < indexHeader.nodeCount; i++)
    {
      cout << "\"";
      //Print keys.
      //unsigned keySize = getKeySize(key, attribute);
      //getKeyAtSlot(page, key, i, attribute);
      printByAttribute(key, attribute);

      cout << "\"]";
      if(i != indexHeader.nodeCount - 1)
      {
        cout << ",";
      }

    }
    cout << "],\n\"children\": [\n";


    //unsigned nextPage = 0;
    //memcpy (&nextPage, (char*)pageData + PAGE_SIZE - INT_SIZE, INT_SIZE);
    //cout << "\nTHIS IS THE nextPage: " << nextPage << endl;
    //recursePrint(ixfileHandle, attribute, nextPage);



    

    //print hte children, need to go down the tree recursively. 
    //start with the left-most page, and work your way down.
  }
}
void IndexManager::recursePrint(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned currPage) const 
{
  //cout << "IN RECURSE";
  void *thisPage = malloc(PAGE_SIZE);  
  ixfileHandle.fh.readPage(currPage, thisPage);
  indexDirectoryHeader indexHeader;
  memcpy (&indexHeader, thisPage, sizeof(indexDirectoryHeader));

  

  void *page = malloc(PAGE_SIZE);
  void *key;

  //If it is a leaf, we start printing keys.  
  if(indexHeader.isLeaf)
  {
        cout << "{\"keys\":[";
        //iterate through all the keys in the leaf.
        for(int i = 0; i < indexHeader.nodeCount; i++)
        {

          //Print each key.
          //getKeyAtSlot(page, key, i, attribute);
          //printByAttribute(key, attribute);

          if(i != indexHeader.nodeCount - 1)
          {
            cout << ",";
          }
        }
        cout << "]}";



  }
   //If it's not a leaf, we want to walk down each of the children
  else if(!indexHeader.isLeaf)
  {
    //Testing
    cout << "{\"keys\":[";
    //cout << indexHeader.nodeCount;
    
    //iterate through all the keys in the leaf.
    for(int i = 0; i < indexHeader.nodeCount; i++)
    {
      cout << "\"";
      //Print keys.
      //unsigned keySize = getKeySize(key, attribute);
      //getKeyAtSlot(page, key, i, attribute);
      printByAttribute(key, attribute);

      cout << "\"]";
      if(i != indexHeader.nodeCount - 1)
      {
        cout << ",";
      }

    }
    cout << "],\n\"children\": [\n";
    

    //print hte children, need to go down the tree recursively. 
    //start with the left-most page, and work your way down.
  }
}
void IndexManager::printByAttribute(const void *data, const Attribute &attribute) const
{
  
  if(attribute.type == TypeInt)
  {
    int intVal = *(int*)data;
    cout << intVal;
  }
  else if(attribute.type == TypeReal)
  {
    float floatVal = *(float*)data;
    cout << floatVal;
  }
  else if (attribute.type == TypeVarChar)
  {
    int size = *(int*)data;
    string varcharVal;
    varcharVal.assign((char*)data+sizeof(int), size);    
    cout << varcharVal;
  }
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return 1;
}

RC IX_ScanIterator::close()
{
    return SUCCESS;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

void IXFileHandle::copyCounterValues()
{
   ixReadPageCounter = fh.readPageCounter;
   ixAppendPageCounter = fh.appendPageCounter;
   ixWritePageCounter = fh.writePageCounter;
}


RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    memcpy(&readPageCount, &ixReadPageCounter, sizeof(ixReadPageCounter));
    memcpy(&writePageCount, &ixWritePageCounter, sizeof(ixReadPageCounter));
    memcpy(&appendPageCount, &ixAppendPageCounter, sizeof(ixReadPageCounter));
    return SUCCESS;
}


//Helper Functions
// Configures a new nonleaf  page, and puts it in "page".
void IndexManager::newNonLeafPage(void * page, unsigned pageNum, unsigned pageZeroNum)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    indexDirectoryHeader indexHeader;
    indexHeader.freeSpaceOffset = PAGE_SIZE -4;
    indexHeader.nodeCount = 0;
    indexHeader.isLeaf = false;
    setIndexDirectoryHeader(page, indexHeader);
    setPageNumAtOffset(page, PAGE_SIZE -4, pageZeroNum);
}

//configures a new leaf page, puts it in page
void IndexManager::newLeafPage(void * page, unsigned pageNum)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    indexDirectoryHeader indexHeader;
    indexHeader.freeSpaceOffset = PAGE_SIZE - 8;
    indexHeader.nodeCount = 0;
    indexHeader.isLeaf = true;
    setIndexDirectoryHeader(page, indexHeader);
    setPageNumAtOffset(page, PAGE_SIZE -4, pageNum);
    setPageNumAtOffset(page, PAGE_SIZE -8, pageNum);
}


void IndexManager::setIndexDirectoryHeader(void * page, indexDirectoryHeader indexHeader)
{   
// Setting the index directory header.
    memcpy (page, &indexHeader, sizeof(indexDirectoryHeader));
}

void IndexManager::setPageNumAtOffset(void * page, int32_t offset, uint32_t pageNum)
{
    memcpy((char*)page+offset, &pageNum, INT_SIZE);
}


bool IndexManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}

indexDirectoryHeader IndexManager::getIndexDirectoryHeader(void * page)
{
    // Getting the index header.
    indexDirectoryHeader indexHeader;
    memcpy (&indexHeader, page, sizeof(indexDirectoryHeader));
    return indexHeader;
}

//gets next child pag
// needs to be implemented beyond the base case
unsigned IndexManager::getChildPage(void * page, const void *key, indexDirectoryHeader header, const Attribute &attribute)
{
   unsigned nextPageNum;
   if(header.nodeCount == 0)
   {
	   memcpy(&nextPageNum, ((char*) page + PAGE_SIZE - 4), INT_SIZE);
	   return nextPageNum;
   }
   unsigned nextSlot = findInsertionSlot(page,key, header, attribute);
   if (nextSlot == 0)
   {
	   memcpy(&nextPageNum, ((char*) page + PAGE_SIZE - 4), INT_SIZE);
           return nextPageNum;
   }
   else
   {
	   unsigned nextPageOffset = getKeyOffset(page,nextSlot - 1) - INT_SIZE;
           memcpy(&nextPageNum, ((char*) page + nextPageOffset), INT_SIZE);
           return nextPageNum;
   }
}


//inserts the key and rid into the leaf page
//also saved the offset of the key after the header
RC IndexManager::prepPage(void * page, const void *key, indexDirectoryHeader header,  const Attribute &attribute, const RID &rid, unsigned pageNum)
{
   unsigned offset = header.freeSpaceOffset;
   unsigned keySize = getKeySize(key, attribute);

   offset = offset - getKeySize(key, attribute);
   memcpy(page, &key, keySize);

   insertOffset(page, key, header, offset, attribute);

   if (header.isLeaf)
{
   offset = offset - (INT_SIZE*2);
   memcpy(page, &rid, INT_SIZE*2);
}
   else
{
   offset = offset - INT_SIZE;
   memcpy(page, &pageNum, INT_SIZE); 
}
   header.nodeCount++;
   header.freeSpaceOffset = offset;
   setIndexDirectoryHeader(page, header);

   return SUCCESS;
}

//calculates the amount of free space on the page
unsigned IndexManager::getTotalFreeSpace(void * page, indexDirectoryHeader header)
{
   unsigned beginningOfPage = 2 * INT_SIZE + 1 + header.nodeCount * INT_SIZE;
   return header.freeSpaceOffset - beginningOfPage;
}

//returns the keySize
unsigned IndexManager::getKeySize(const void *key, const Attribute &attribute)
{
   unsigned size = 0;
   switch (attribute.type)
   {
	    case TypeInt:
		size += INT_SIZE;
	    break;
	    case TypeReal:
		size += REAL_SIZE;
	    break;
	    case TypeVarChar:
		uint32_t varcharSize;
		// We have to get the size of the VarChar field by reading the integer that precedes the string value itself
		memcpy(&varcharSize, (char*) key, VARCHAR_LENGTH_SIZE);
		size += varcharSize + INT_SIZE;
    		break;
   }
   return size;
}


void IndexManager::insertOffset(void * page, const void *key, indexDirectoryHeader header, unsigned offset, const Attribute &attribute)
{
   void * helperPageData = calloc(PAGE_SIZE, 1);
   memcpy(helperPageData, page, PAGE_SIZE);
   unsigned insertSlot = findInsertionSlot(page,key, header, attribute);
   unsigned insertOffset, helperPageOffset;
   unsigned appendCount = 0;
   for (unsigned i = 0; i <= header.nodeCount; i++)
   {
	   insertOffset = (INT_SIZE * 2) + 1 + (INT_SIZE * i);
	   if (i == insertSlot)
	   {
		memcpy((char*) page + insertOffset, &offset, INT_SIZE);
	   }
	   else
	   {
		helperPageOffset = (INT_SIZE * 2) + 1 + (INT_SIZE * appendCount);
		memcpy((char*) page + insertOffset, (char*) helperPageData + helperPageOffset, INT_SIZE);
		appendCount++;
	   }
   }
}

unsigned IndexManager::findInsertionSlot(void * page, const void *key, indexDirectoryHeader header, const Attribute &attribute)
{

   void * compKey;
   for(unsigned i = 0; i < header.nodeCount; i++)
   {
	   getKeyAtSlot(page, compKey, i, attribute);
	   if (!greaterEqual(key, compKey, attribute)) return i;
   }
   return header.nodeCount;
}


//page contains data
//key is a pointer to have data saved to it
//slot is requested slotnum
void IndexManager::getKeyAtSlot(void * page, void *key, unsigned slot, const Attribute &attribute)
{
   unsigned keyOffset= getKeyOffset(page, slot);
   getKeyAtOffset(page, key, keyOffset, attribute);
}



void IndexManager::getKeyAtOffset(void * page, void *key, unsigned offset, const Attribute &attribute)
{
   switch (attribute.type)
   {
            case TypeInt:
		memset(key, 0, INT_SIZE);
                memcpy(key,((char*) page + offset), INT_SIZE);
            break;
            case TypeReal:
		memset(key, 0, INT_SIZE);
		memcpy(key,((char*) page + offset), REAL_SIZE);
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) page, VARCHAR_LENGTH_SIZE);
		memset(key, 0, varcharSize);
                memcpy((char*) key, (char*) page, varcharSize);
                break;
   }
}

bool IndexManager::greaterEqual(const void * key1, void * key2, const Attribute &attribute)
{
   switch (attribute.type)
   {
            case TypeInt:
		uint32_t intKey1, intKey2;
                memcpy(&intKey1, key1, INT_SIZE);
		memcpy(&intKey2, key2, INT_SIZE);
		return intKey1 >= intKey2;
            break;
            case TypeReal:
                float floatKey1, floatKey2;
                memcpy(&floatKey1, key1, INT_SIZE);
                memcpy(&floatKey2, key2, INT_SIZE);
                return floatKey1 >= floatKey2;
            break;
            case TypeVarChar:
/*
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) page, VARCHAR_LENGTH_SIZE);
                memcpy((char*) key, (char*) page, varcharSize);
*/
		return true;
                break;
   }
   return true;
}

unsigned IndexManager::getKeyOffset(void * page, unsigned slotNum)
{
   unsigned offsetOffset = (1 +  (2 + slotNum ) * INT_SIZE);
   unsigned keyOffset;
   memcpy(&keyOffset, ((char*) page + offsetOffset), INT_SIZE);
   return keyOffset;
}


unsigned IndexManager::copyOverKey(const void * fromPage, void * toPage,  unsigned fromOffset, unsigned toFreeSpace,const Attribute &attribute)
{
   switch (attribute.type)
   {
            case TypeInt:
                memcpy((char*) toPage + toFreeSpace - INT_SIZE, (char*) fromPage + fromOffset, INT_SIZE);
		return INT_SIZE;
            break;
            case TypeReal:
		memcpy((char*) toPage + toFreeSpace - REAL_SIZE, (char*) fromPage + fromOffset, REAL_SIZE);
		return REAL_SIZE;
            break;
            case TypeVarChar:

                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) fromPage + fromOffset, VARCHAR_LENGTH_SIZE);
		memcpy((char*) toPage + toFreeSpace - varcharSize, (char*) fromPage + fromOffset, varcharSize);
                memcpy((char*) toPage + toFreeSpace -varcharSize -  VARCHAR_LENGTH_SIZE, (char*) fromPage + fromOffset + VARCHAR_LENGTH_SIZE, varcharSize);
		return varcharSize + VARCHAR_LENGTH_SIZE;
                break;
   }
   return 0;
}

/*
RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return ix_ScanIterator.scanInit(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

IX_ScanIterator::IX_ScanIterator()
{
	im = IndexManager::instance();
}

RC IX_ScanIterator::close()
{
	free(pageData);
	return SUCCESS;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	page = calloc(PAGE_SIZE, 1);
	ixFH.fh.readPage(currPage, page);
	indexDirectoryHeader indexHeader = getIndexDirectoryHeader(page);
	
	if(currSlot <= indexheader.nodeCount){
		if(compare()){
			unsigned offset = im->getKeyOffset(page, currSlot);
			const void * thing;
			im->getKeyAtSlot(page, thing, currSlot, attr);
			unsigned size = im->getKeySize(thing, attr);
			memcpy(&key, offset, size);
			memcpy(&rid, offset-INT_SIZE*2, INT_SIZE*2);
			return SUCCESS;
		}
		else
			return ERROR;
	}
	else{
		unsigned nextPage;
		memcpy(&nextPage, (char*)pageData + PAGE_SIZE - INT_SIZE, INT_SIZE);
		currPage = nextPage;
		currSlot = 0;
		if(compare()){
			unsigned offset = im->getKeyOffset(page, currSlot);
			const void * thing;
			im->getKeyAtSlot(page, thing, currSlot, attr);
			unsigned size = im->getKeySize(thing, attr);
			memcpy(&key, offset, size);
			memcpy(&rid, offset-INT_SIZE*2, INT_SIZE*2);
			return SUCCESS;
		}
		else
			return ERROR;
	}
}

RC RBFM_ScanIterator::scanInit(IXFileHandle &ix,
		const Attribute a,
		const void *lk,
		const void *hk,
		bool lki, 
		bool hki)
{
	currPage = 0;
	currSlot = 0;
	attr = a;
	lowVal = lk;
	highVal = hk;
	currVal = lowVal;
	ixFH = ix;
	if(lki)
		lowCompOp = GE_OP;
	else
		lowCompOp = GT_OP;
	if(hki)
		highCompOp = LE_OP;
	else
		highCompOp = LT_OP;
	currCompOp = lowCompOp;
	scanStartPage = search(currval, lki, ixFH, currPage);
	if(scanStartPage != 69)
		return SUCCESS;
	else
		return ERROR;
	scanStartSlot = currSlot;
	currVal = highVal;
	currCompOp = highCompOp;
}



unsigned IX_ScanIterator::search(const void *key, bool lowKeyInclusive, IXFileHandle &ixfileHandle, unsigned pageNum)
{
	pageData = calloc(PAGE_SIZE, 1);
	ixfileHandle.fh.readPage(pageNum, pageData);
	indexDirectoryHeader indexHeader = getIndexDirectoryHeader(pageData);
	
	if(indexHeader.isLeaf){
		//check if page key matches condition-- if not, it's EOF
		//return page number 
		
		//get key value, store in currVal
		im->getKeyAtSlot(pageData, currVal, currSlot, attr);
		//compare key against comparison value
		bool check = compare();
		
		if(check)
			return currPage;
		else 
			currSlot++;
		if(currSlot > indexheader.nodeCount)
			return 69;
	}
	
	if(indexHeader.nodeCount == 0)
		return 69; 
	currSlot = 0;
	while(currSlot<=indexheader.nodeCount){
		//get key value, store in currVal
		im->getKeyAtSlot(pageData, currVal, currSlot, attr);
		//compare key against comparison value
		bool check = compare();
		//if true, always go to left child (search only called to find low value) 
		if(check && currSlot == 0)
		{
			unsigned nextPage;
			memcpy(&nextPage, (char*)pageData + PAGE_SIZE - INT_SIZE, INT_SIZE);
			currPage = nextPage;
			currSlot = 0;
			pageRet = search(currVal, lowKeyInclusive, ixfileHandle, currPage);
		}
		else if (check && currSlot != 0)
		{
			unsigned offset = im->getKeyOffset(pageData, currSlot-1);
			unsigned nextPage;
			memcpy(&nextpage, offset - INT_SIZE, INT_SIZE);
			currPage = nextPage;
			currSlot = 0;
			pageRet = search(currVal, lowKeyInclusive, ixfileHandle, currPage);
		}
		else
		{
			currSlot++;
		}
		
	}
	if(currSlot > indexheader.nodeCount)
		return 69;
}

bool IX_ScanIterator::compare()
{
    // if no op, return true
	if (compOp == NO_OP) 
    	return true;
	// if Null lowkey and greater-than op, return true
    if (lowKey == NULL && (currCompOp == GE_OP || currCompOp == GT_OP)) 
    	return true;
    // if Null highkey and less-than op, return true
    if (highKey == NULL && (currCompOp == LE_OP || currCompOp == LT_OP)) 
        return true;
    //get enough space for our entry
    void *data = malloc(attr.length);
    //get entry at current slot, put it into data
    im->getKeyAtSlot(pageData, data, currSlot, attr);
    //set result variable
    bool result = false;
    // Checkscan condition on record data and scan value
    else if (attr.type == TypeInt)
    {
        int32_t recordInt;
        memcpy(&recordInt, (char*)data, INT_SIZE);
        result = compare(recordInt, currCompOp, currVal);
    }
    else if (attr.type == TypeReal)
    {
        float recordReal;
        memcpy(&recordReal, (char*)data, REAL_SIZE);
        result = compare(recordReal, currCompOp, currVal);
    }
    else if (attr.type == TypeVarChar)
    {
        uint32_t varcharSize;
        memcpy(&varcharSize, (char*)data, VARCHAR_LENGTH_SIZE);
        char recordString[varcharSize];
        memcpy(recordString, (char*)data + VARCHAR_LENGTH_SIZE, varcharSize);
        recordString[varcharSize] = '\0';

        result = compare(recordString, currCompOp, currVal);
    }
    free (data);
    return result;
}

bool IX_ScanIterator::compare(int recordInt, CompOp compOp, const void *value)
{
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordInt == intValue;
        case LT_OP: return recordInt < intValue;
        case GT_OP: return recordInt > intValue;
        case LE_OP: return recordInt <= intValue;
        case GE_OP: return recordInt >= intValue;
        case NE_OP: return recordInt != intValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool IX_ScanIterator::compare(float recordReal, CompOp compOp, const void *value)
{
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordReal == realValue;
        case LT_OP: return recordReal < realValue;
        case GT_OP: return recordReal > realValue;
        case LE_OP: return recordReal <= realValue;
        case GE_OP: return recordReal >= realValue;
        case NE_OP: return recordReal != realValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool IX_ScanIterator::compare(char *recordString, CompOp compOp, const void *value)
{
    if (compOp == NO_OP)
        return true;

    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    int cmp = strcmp(recordString, valueStr);
    switch (compOp)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        // Should never happen
        default: return false;
    }
}


*/
