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
    printf("PageNum: %x\n", numPages);
    if(numPages == 0)
    {
	    void * rootPageData = calloc(PAGE_SIZE, 1);
	    void * leafPageData = calloc(PAGE_SIZE, 1);
	    if ((rootPageData == NULL) || (leafPageData == NULL))
		return ERROR;

	    newNonLeafPage(rootPageData, 0);
   	    newLeafPage(leafPageData, 1);

	    if (ixfileHandle.fh.appendPage(rootPageData))
		return ERROR;
            if (ixfileHandle.fh.appendPage(leafPageData))
                return ERROR;

	    free(rootPageData);
	    free(leafPageData);
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

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return SUCCESS;
}


//Helper Functions
// Configures a new record based page, and puts it in "page".
void IndexManager::newNonLeafPage(void * page, unsigned pageNum)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    indexDirectoryHeader indexHeader;
    indexHeader.freeSpaceOffset = PAGE_SIZE -4;
    indexHeader.nodeCount = 0;
    setIndexDirectoryHeader(page, indexHeader);
    setPageNumAtOffset(page, PAGE_SIZE -4, pageNum);
}

void IndexManager::newLeafPage(void * page, unsigned pageNum)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    indexDirectoryHeader indexHeader;
    indexHeader.freeSpaceOffset = PAGE_SIZE - 8;
    indexHeader.nodeCount = 0;
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

/*unsigned IndexManager::getEntrySize( const Attribute &attribute, const void *key)
{
//TBD add stuff for char/varchar keys

//retSize = offsetSlot + pageSlot + key
   unsigned retSize = (2 * 4) +  4;

}
*/
//(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)

