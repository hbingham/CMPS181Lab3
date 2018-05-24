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
    insertEntryRec(rootPageNum, ixfileHandle, attribute, key, rid);
    ixfileHandle.copyCounterValues();
    return SUCCESS;
}


RC IndexManager::insertEntryRec(unsigned pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void * pageData = calloc(PAGE_SIZE, 1);
    ixfileHandle.fh.readPage(pageNum, pageData);
    indexDirectoryHeader indexHeader = getIndexDirectoryHeader(pageData);

    if(!indexHeader.isLeaf)
    {
	   unsigned nextPage = getChildPage(pageData, key, indexHeader, attribute);
	   insertEntryRec(nextPage, ixfileHandle, attribute, key, rid);
    }
    else
    {
//must add logic for stuff that doesnt fit!
	   unsigned freeSpace = getTotalFreeSpace(pageData, indexHeader);
	   prepLeafPage(pageData, key, indexHeader, attribute, rid);
	   ixfileHandle.fh.writePage(pageNum, pageData);
    }
    return SUCCESS;
}



RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
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
   
   return 69;
}


//inserts the key and rid into the leaf page
//also saved the offset of the key after the header
RC IndexManager::prepLeafPage(void * page, const void *key, indexDirectoryHeader header,  const Attribute &attribute, const RID &rid)
{
//Precondition: stuff fits, need to insert IN ORDER
//future note: OFFSETS COULD JUST BE IN ORDER AT THE BEGINNING instead of actually ordering the data
//will allow for easy deletions and dogags
   unsigned offset = header.freeSpaceOffset;
   unsigned keySize = getKeySize(key, attribute);

   offset = offset - getKeySize(key, attribute);
   memcpy(page, &key, keySize);

   insertOffset(page, key, header, offset, attribute);

   offset = offset - (INT_SIZE*2);
   memcpy(page, &rid, INT_SIZE*2);

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
   memcpy((char*) page + insertOffset, &offset, INT_SIZE);
}

unsigned IndexManager::findInsertionSlot(void * page, const void *key, indexDirectoryHeader header, const Attribute &attribute)
{

   void * compKey;
   for(unsigned i = 0; i < header.nodeCount; i++)
   {
	   getKeyAtSlot(page, compKey, i, attribute);
	   if (greaterEqual(key, compKey, attribute)) return i - 1;
   }
   return header.nodeCount;
}



void IndexManager::getKeyAtSlot(void * page, void *key, unsigned slot, const Attribute &attribute)
{
   unsigned offsetOffset = (1 +  (2 + slot ) * INT_SIZE);
   unsigned keyOffset;
   memcpy(&keyOffset, ((char*) page + offsetOffset), INT_SIZE);
   getKeyAtOffset(page, key, keyOffset, attribute);
}



void IndexManager::getKeyAtOffset(void * page, void *key, unsigned offset, const Attribute &attribute)
{
   switch (attribute.type)
   {
            case TypeInt:
                memcpy(key,((char*) page + offset), INT_SIZE);
            break;
            case TypeReal:
		memcpy(key,((char*) page + offset), REAL_SIZE);
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) page, VARCHAR_LENGTH_SIZE);
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
}


//TO MAKE:
//order offsets

//deal with siblings
//copying up on overflow

