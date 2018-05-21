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
    printf("hummm");
    // Creating a new paged file.
/*
    if (fileExists(fileName))
        return ERROR;
    // Attempt to open the file for writing
    FILE *pFile = fopen(fileName.c_str(), "wb");
    // Return an error if we fail

    if (pFile == NULL)
        return ERROR;

    fclose (pFile);
*/
    if (_pf_manager->createFile(fileName))
        return ERROR;
    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return ERROR;
    newIndexPage(firstPageData);
    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return ERROR;
    if (handle.appendPage(firstPageData))
        return ERROR;
    _pf_manager->closeFile(handle);

    free(firstPageData);
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
    
    return -1;
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
    return -1;
}


//Helper Functions
// Configures a new record based page, and puts it in "page".
void IndexManager::newIndexPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    indexDirectoryHeader indexHeader;
    indexHeader.freeSpaceOffset = PAGE_SIZE;
    indexHeader.nodeCount = 0;
    setIndexDirectoryHeader(page, indexHeader);
}


void IndexManager::setIndexDirectoryHeader(void * page, indexDirectoryHeader indexHeader)
{   
       // Setting the index directory header.
    memcpy (page, &indexHeader, sizeof(indexDirectoryHeader));
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

