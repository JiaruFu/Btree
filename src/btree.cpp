/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include <typeinfo>

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName,
                       BufMgr *bufMgrIn,
                       const int attrByteOffset,
                       const Datatype attrType)
{
    //set values of the private variables
    this->bufMgr = bufMgrIn;
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
    scanExecuting = false;

    std ::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std ::string indexName = idxStr.str(); // indexName is the name of the index file

    try
    {
        // try to create new file
        file = new BlobFile(indexName, false);
        headerPageNum = file->getFirstPageNo();
        Page *metaPage;
        bufMgr->readPage(file, headerPageNum, metaPage);
        IndexMetaInfo *inf = (IndexMetaInfo *)metaPage;

        // std::string str(inf->relationName);
        // std::cout << (inf->relationName) << std::endl;

        if (strcmp(inf->relationName, relationName.c_str()) != 0 ||
            (inf->attrByteOffset != attrByteOffset) ||
            (inf->attrType != attrType))
        {
            throw BadIndexInfoException(indexName);
        }

        rootPageNum = inf->rootPageNo;
        bufMgr->unPinPage(file, headerPageNum, false);
    }
    catch (FileExistsException fileExistsException)
    {
        file = new BlobFile(indexName, true);

        //create a new meta page
        Page *metaPage;
        bufMgr->allocPage(file, headerPageNum, metaPage);

        IndexMetaInfo *inf = (IndexMetaInfo *)metaPage;

        //set up other attributes
        strncpy(inf->relationName, relationName.c_str(), 20);
        inf->attrByteOffset = attrByteOffset;
        inf->attrType = attrType;

        //create a root page
        // PageId root_page_id;
        // blobfile->allocatePage(root_page_id);
        Page *rootPage;
        bufMgr->allocPage(file, rootPageNum, rootPage);

        //initialize the rootNode with two child leaves
        NonLeafNodeInt *rootNode = (NonLeafNodeInt *)rootPage;
        rootNode->level = 1;
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
        {
            rootNode->keyArray[i] = INT32_MAX;
        }
        for (int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++)
        {
            rootNode->pageNoArray[i] = NULL;
        }

        //create an empty left leaf page and right leaf page of the attribute type
        Page *leftLeafPage, *rightLeafPage;
        PageId leftLeafPageId, rightLeafPageId;

        bufMgr->allocPage(file, leftLeafPageId, leftLeafPage);
        bufMgr->allocPage(file, rightLeafPageId, rightLeafPage);

        LeafNodeInt *leftLeafNode = (LeafNodeInt *)leftLeafPage;
        LeafNodeInt *rightLeafNode = (LeafNodeInt *)rightLeafPage;

        //initialize the leaf page
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
        {
            leftLeafNode->keyArray[i] = INT32_MAX;
            rightLeafNode->keyArray[i] = INT32_MAX;
        }

        leftLeafNode->rightSibPageNo = rightLeafPageId;
        rightLeafNode->rightSibPageNo = NULL;

        rootNode->pageNoArray[0] = leftLeafPageId;
        rootNode->pageNoArray[1] = rightLeafPageId;

        //unpin the new leaf page. its dirty
        bufMgr->unPinPage(file, leftLeafPageId, true);
        bufMgr->unPinPage(file, rightLeafPageId, true);
        bufMgr->unPinPage(file, rootPageNum, true);

        // page number of root page
        // rootPageNum = root_page_id; // assign root page id
        inf->rootPageNo = rootPageNum;
        bufMgr->unPinPage(file, headerPageNum, true);

        // insert entries for every tuple in the base relation
        FileScan fscan(relationName, bufMgrIn);

        try
        {
            RecordId scanRid;
            while (1)
            {
                fscan.scanNext(scanRid);
                std::string recordStr = fscan.getRecord();
                const char *record = recordStr.c_str();
                int key = *((int *)(record + attrByteOffset));
                this->insertEntry(&key, scanRid);
            }
        }
        catch (EndOfFileException e)
        {
            bufMgr->flushFile(file);
        }
    }

    outIndexName = indexName;
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    scanExecuting = false;
    // bufMgr->unPinPage(file, rootPageNum, true);
    bufMgr->flushFile(file);
    delete file;
    file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
    Page *rootPage;
    bufMgr->readPage(file, rootPageNum, rootPage);
    // assume the key can only be integer
    NonLeafNodeInt *rootNode = (NonLeafNodeInt *)rootPage;
    bool splited; // record whether need to split the root node
    bool childLeaf;
    PageId newPageId;

    // Start from root to recursively find out the leaf to insert the entry in.
    this->recurseInsert(rootPage, rootNode->level, true, key, rid, splited, childLeaf, newPageId);

    //if the root splited, update the metapage
    if (splited)
    {
        if (rootNode->keyArray[INTARRAYNONLEAFSIZE - 1] == INT32_MAX)
        {
            //enough room
            this->insertNonLeaf(rootPage, (void *)&middleInt, newPageId);

            // print
            NonLeafNodeInt *node = (NonLeafNodeInt *)rootPage;
        }
        else
        {

            PageId addedPageId;
            this->split(rootPage, false, (void *)&middleInt, newPageId, addedPageId);

            //create a new NonLeafPage and put the middle int on it
            Page *newRootPage;
            PageId newRootPageId;
            bufMgr->allocPage(file, newRootPageId, newRootPage);
            NonLeafNodeInt *newRoot = (NonLeafNodeInt *)newRootPage;

            //we know this can never be just above the leaves so set level to 0
            newRoot->level = 0;
            rootPageNum = newRootPageId;

            //null eveything in this new page
            newRoot->pageNoArray[INTARRAYNONLEAFSIZE] = NULL;
            for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
            {
                newRoot->keyArray[i] = INT32_MAX;
                newRoot->pageNoArray[i] = NULL;
            }

            //the only value in the new root is the middle value passed up from the child
            newRoot->keyArray[0] = middleInt;

            //the left child is the old root page
            newRoot->pageNoArray[0] = rootPageNum;

            //the right child is the one that was added by the restructure method
            newRoot->pageNoArray[1] = addedPageId;

            //unpin the old root page and update the class references
            bufMgr->unPinPage(file, rootPageNum, true);
            rootPageNum = newRootPageId;
            rootPage = newRootPage;

            //update the meta info
            //read in the metainfo so it can be updated
            Page *metadataPage;

            bufMgr->readPage(file, headerPageNum, metadataPage);
            IndexMetaInfo *metadata = (IndexMetaInfo *)metadataPage;

            metadata->rootPageNo = newRootPageId;

            //unpin the metadataPage
            bufMgr->unPinPage(file, headerPageNum, true);
        }
    }
    bufMgr->unPinPage(file, rootPageNum, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm)
{
    lowValInt = *((int *)lowValParm);
    highValInt = *((int *)highValParm);
    lowOp = lowOpParm;
    highOp = highOpParm;

    if (lowValInt > highValInt)
    {
        // scanExecuting = false;
        throw BadScanrangeException();
    }

    if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE))
    {

        // scanExecuting = false;
        throw BadOpcodesException();
    }

    Page *nt_page;
    bufMgr->readPage(file, rootPageNum, nt_page);
    //set the currentPageData, variable defined in header file to that leaf node

    int index;

    if (scanExecuting)
    {
        // If another scan is already executing, that needs to be ended here.
        this->endScan();
    }

    startScanHeler(nt_page, lowValParm, index);
    //unpin root page
    bufMgr->unPinPage(file, rootPageNum, false);
    scanExecuting = true; // not error thrown within helper, start
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScanHeler
// Start from root to find out the leaf page
// -----------------------------------------------------------------------------

const void BTreeIndex::startScanHeler(Page *nl, const void *lowValParm, int &index)
{

    NonLeafNodeInt *np = (NonLeafNodeInt *)nl;
    int level = np->level;

    if (level == 0)
    {
        // recurse to find the level above leaf
        findPageNo(nl, lowValParm, index);

        Page *childPage;
        bufMgr->readPage(file, ((NonLeafNodeInt *)nl)->pageNoArray[index], childPage);
        startScanHeler(childPage, lowValParm, index);
        bufMgr->unPinPage(file, ((NonLeafNodeInt *)nl)->pageNoArray[index], false);
    }
    else
    {
        // above leaf node, get leaf page and search for record satisfing the requirements
        Page *leafPage;
        findPageNo(nl, lowValParm, index);
        PageId leafId = ((NonLeafNodeInt *)nl)->pageNoArray[index];
        bufMgr->readPage(file, leafId, leafPage);
        LeafNodeInt *leaf = (LeafNodeInt *)leafPage;
        // currentPageNum = leafPageId;

        // find the record satisfing the requirements
        bool found = false;
        while (!found)
        {

            // iterate through the key on the leaf
            for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
            {
                int key = leaf->keyArray[i];
                if (key == INT32_MAX)
                {
                    // no more key on this leaf
                    break;
                }
                // check whether the key satisfy the requirements
                if (lowOp == GTE && highOp == LTE)
                {
                    found = key <= highValInt && key >= lowValInt;
                }
                else if (lowOp == GT && highOp == LTE)
                {
                    found = key <= highValInt && key > lowValInt;
                }
                else if (lowOp == GTE && highOp == LT)
                {
                    found = key < highValInt && key >= lowValInt;
                }
                else
                {
                    found = key < highValInt && key > lowValInt;
                }

                if (found)
                {
                    currentPageData = leafPage;
                    currentPageNum = leafId;
                    nextEntry = i;

                    break;
                }
            }

            if (!found)
            {
                bufMgr->unPinPage(file, leafId, false);
                // unpin the page
                // search for next possible page
                if (leaf->rightSibPageNo != NULL)
                {
                    PageId nextPageId = leaf->rightSibPageNo;
                    bufMgr->readPage(file, nextPageId, leafPage);
                    leafId = nextPageId;
                    leaf = (LeafNodeInt *)leafPage;
                }
                else
                {
                    bufMgr->unPinPage(file, rootPageNum, false);
                    throw NoSuchKeyFoundException();
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid)
{
    if (!scanExecuting)
    {
        throw ScanNotInitializedException();
    }

    LeafNodeInt *leaf = (LeafNodeInt *)currentPageData;

    if (nextEntry == INTARRAYNONLEAFSIZE - 1 || leaf->keyArray[nextEntry] == INT32_MAX)
    {
        // end of the page
        // Unpin page and read papge
        bufMgr->unPinPage(file, currentPageNum, false);
        // No more next leaf
        if (leaf->rightSibPageNo == NULL)
        {
            // no more leaf page available
            throw IndexScanCompletedException();
        }
        else
        {

            currentPageNum = leaf->rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            leaf = (LeafNodeInt *)currentPageData;
            // Reset nextEntry
            nextEntry = 0;
        }
    }
    // find whether the rid satisfy op
    int key = leaf->keyArray[nextEntry];
    // check whether satisfy
    bool satisfy;

    if (lowOp == GTE && highOp == LTE)
    {
        satisfy = key <= highValInt && key >= lowValInt;
    }
    else if (lowOp == GT && highOp == LTE)
    {
        satisfy = key <= highValInt && key > lowValInt;
    }
    else if (lowOp == GTE && highOp == LT)
    {
        satisfy = key < highValInt && key >= lowValInt;
    }
    else
    {
        satisfy = key < highValInt && key > lowValInt;
    }

    if (satisfy)
    {
        // std::cout<<"next5"<< std::endl;
        outRid = leaf->ridArray[nextEntry];
        nextEntry++;
        // std::cout<<"next6"<< std::endl;
    }
    else
    {
        throw IndexScanCompletedException();
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
    if (!scanExecuting)
    {
        throw ScanNotInitializedException();
    }
    else
    {
        // FileIterator filePageIter((PageFile *)file);
        bufMgr->unPinPage(file, currentPageNum, false);
        // stop executing the scan
        scanExecuting = false;
        //reset variables
        currentPageData = nullptr;
        currentPageNum = static_cast<PageId>(-1);
        nextEntry = -1;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::recurseInsert
// -----------------------------------------------------------------------------
//
const void BTreeIndex::recurseInsert(Page *page, int level, bool isRoot, const void *keyPtr, const RecordId rid, bool &splited, bool &childLeaf, PageId &newPageId)
{
    NonLeafNodeInt *node = (NonLeafNodeInt *)page;

    int index; // index of node in pageNoArray to recurse on

    if (level == 0)
    {
        childLeaf = false;

        //find the key to recurse on
        this->findPageNo(page, keyPtr, index);

        // recurse
        //read in that page
        Page *child;
        bufMgr->readPage(file, node->pageNoArray[index], child);

        PageId pageIdFromChild;
        bool childsplited;
        bool fromLeaf;

        recurseInsert(child, ((NonLeafNodeInt *)child)->level, false, keyPtr, rid, childsplited, fromLeaf, pageIdFromChild);

        if (childsplited)
        {
            if (fromLeaf)
            {
                //read the pageIdFromChild and set its level to 1
                Page *pageFromChild;
                NonLeafNodeInt *nodeFromChild;
                bufMgr->readPage(file, pageIdFromChild, pageFromChild);
                nodeFromChild = (NonLeafNodeInt *)pageFromChild;
                nodeFromChild->level = 1;

                //unpin the page
                bufMgr->unPinPage(file, pageIdFromChild, true);
            }

            if (node->keyArray[INTARRAYNONLEAFSIZE - 1] == INT32_MAX)
            {
                //enough room, just insert
                splited = false;
                insertNonLeaf(page, (void *)&middleInt, pageIdFromChild);
            }
            else
            {
                splited = true;
                //save the value of middleInt that the previous restructure set
                int middleIntFromChild = middleInt;

                split(page, false, (void *)&middleIntFromChild, pageIdFromChild, newPageId);

                //only need to insert if not equal
                if (middleIntFromChild < middleInt)
                {
                    //insert it onto old node (node)
                    insertNonLeaf(page, (void *)&middleIntFromChild, pageIdFromChild);
                }
                else if (middleIntFromChild > middleInt)
                {
                    //insert
                    //read in that page
                    Page *newNodePage;
                    bufMgr->readPage(file, newPageId, newNodePage);

                    insertNonLeaf(newNodePage, (void *)&middleIntFromChild, pageIdFromChild);

                    //unpin that page
                    bufMgr->unPinPage(file, newPageId, true);
                }
            }
        }
        bufMgr->unPinPage(file, node->pageNoArray[index], true);
    }
    else
    {
        childLeaf = true;
        //we are the parent of the leaf
        //find what page the leaf is on
        findPageNo(page, keyPtr, index);

        //read in the leaf page and cast
        Page *leafPage;
        PageId leafPageId = node->pageNoArray[index];
        bufMgr->readPage(file, leafPageId, leafPage);
        LeafNodeInt *leaf = (LeafNodeInt *)leafPage;

        //find the index into the key array where the rid would go
        findKey(leafPage, keyPtr, index);

        //insert into leaf
        //if the last place in the leaf is NULL then we dont have to restructure
        if (leaf->keyArray[INTARRAYLEAFSIZE - 1] == INT32_MAX)
        {
            splited = false;

            //move entries over one place (start at the end)
            for (int i = INTARRAYLEAFSIZE - 1; i > index; i--)
            {
                leaf->keyArray[i] = leaf->keyArray[i - 1];
                leaf->ridArray[i] = leaf->ridArray[i - 1];
            }

            //actually insert the entry
            leaf->keyArray[index] = *(int *)keyPtr;
            leaf->ridArray[index].page_number = rid.page_number;
            leaf->ridArray[index].slot_number = rid.slot_number;
        }
        else
        {
            splited = true;

            split(leafPage, true, keyPtr, NULL, newPageId);

            //now actually put the entry passed in on one of these pages
            if (*((int *)keyPtr) >= middleInt)
            {
                Page *newLeafPage;
                bufMgr->readPage(file, newPageId, newLeafPage);

                LeafNodeInt *newLeaf = (LeafNodeInt *)newLeafPage;

                findKey(newLeafPage, keyPtr, index);

                //move the keys over on this page
                for (int i = INTARRAYLEAFSIZE - 1; i > index; i--)
                {
                    newLeaf->keyArray[i] = newLeaf->keyArray[i - 1];
                    newLeaf->ridArray[i].page_number = newLeaf->ridArray[i - 1].page_number;
                    newLeaf->ridArray[i].slot_number = newLeaf->ridArray[i - 1].slot_number;
                }

                //actually insert the record
                newLeaf->keyArray[index] = *(int *)keyPtr;
                newLeaf->ridArray[index].page_number = rid.page_number;
                newLeaf->ridArray[index].slot_number = rid.slot_number;

                // unpin pages
                bufMgr->unPinPage(file, newPageId, true);
            }
            else
            {
                findKey(leafPage, keyPtr, index);

                //move the keys
                for (int i = INTARRAYLEAFSIZE - 1; i > index; i--)
                {
                    leaf->keyArray[i] = leaf->keyArray[i - 1];
                    leaf->ridArray[i].page_number = leaf->ridArray[i - 1].page_number;
                    leaf->ridArray[i].slot_number = leaf->ridArray[i - 1].slot_number;
                }

                // insert
                leaf->keyArray[index] = *(int *)keyPtr;
                leaf->ridArray[index].page_number = rid.page_number;
                leaf->ridArray[index].slot_number = rid.slot_number;
            }
        }
        // unpin pages
        bufMgr->unPinPage(file, leafPageId, true);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::findPageNo
// -----------------------------------------------------------------------------
//
const void BTreeIndex::findPageNo(Page *page, const void *keyPtr, int &index)
{
    NonLeafNodeInt *node = (NonLeafNodeInt *)page;

    // iterate the key array to find the suitable index to recurse on
    for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
    {
        if (*(int *)keyPtr < node->keyArray[0])
        {
            index = 0;
            return;
        }
        else if (*(int *)keyPtr >= node->keyArray[i] && (i == INTARRAYNONLEAFSIZE - 1 || node->keyArray[i + 1] == INT32_MAX))
        {
            // reach the end of the array
            index = i + 1;
            return;
        }
        else if (*(int *)keyPtr >= node->keyArray[i] && *(int *)keyPtr < node->keyArray[i + 1])
        {
            // key is between keyarray[i] and keyarray[i+1]
            index = i + 1;
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::findRecurse
// -----------------------------------------------------------------------------
//
const void BTreeIndex::findKey(Page *leafPage, const void *keyPtr, int &index)
{
    // current key and node
    LeafNodeInt *leaf = (LeafNodeInt *)leafPage;

    // iterate the key array to find the suitable index to recurse on
    // assume no duplicate key
    for (int i = 0; i < INTARRAYLEAFSIZE; i++)
    {
        if (leaf->keyArray[0] == INT32_MAX || *(int *)keyPtr < leaf->keyArray[0])
        {
            index = 0;
            return;
        }
        else if (*((int *)keyPtr) > leaf->keyArray[i] && (i == INTARRAYLEAFSIZE - 1 || leaf->keyArray[i + 1] == INT32_MAX))
        {
            // reach the end of the array
            index = i + 1;
            return;
        }
        else if (*((int *)keyPtr) > leaf->keyArray[i] && *((int *)keyPtr) < leaf->keyArray[i + 1])
        {
            // key is between keyarray[i] and keyarray[i+1]
            index = i + 1;
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertNonLeaf
// -----------------------------------------------------------------------------
//
const void BTreeIndex::insertNonLeaf(Page *page, const void *keyPtr, PageId pageId)
{
    // current key and node
    NonLeafNodeInt *node = (NonLeafNodeInt *)page;

    // iterate the key array to find where to insert
    for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
    {
        if (node->keyArray[i] == INT32_MAX)
        {
            node->keyArray[i] = *((int *)keyPtr);
            node->pageNoArray[i + 1] = pageId;
            return;
        }
        else if (*((int *)keyPtr) < node->keyArray[i])
        {
            // shift the later part of the array
            for (int j = INTARRAYNONLEAFSIZE - 1; j > i; j--)
            {
                node->keyArray[j] = node->keyArray[j - 1];
                node->pageNoArray[j + 1] = node->pageNoArray[j];
            }
            // insert
            node->keyArray[i] = *((int *)keyPtr);
            node->pageNoArray[i + 1] = pageId;
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::split
// -----------------------------------------------------------------------------
//
const void BTreeIndex::split(Page *fullPage, bool isLeaf, const void *keyPtr, PageId newPageIdChild, PageId &newPageId)
{
    int middleIndex; // records the middle index to split

    if (isLeaf)
    {
        LeafNodeInt *fullLeaf = (LeafNodeInt *)fullPage;

        //create a new page
        Page *newLeafPage;
        bufMgr->allocPage(file, newPageId, newLeafPage);
        LeafNodeInt *newLeaf = (LeafNodeInt *)newLeafPage;

        //NULL everything in the new page
        for (int i = 0; i < INTARRAYLEAFSIZE; i++)
        {
            newLeaf->keyArray[i] = INT32_MAX;
            //not NULLing the rids here because we just assume if the key is NULL then so is the associated rid so don't access it
        }

        //get the middle value and index
        int middleIndex;
        this->findMiddle(fullPage, true, keyPtr, middleIndex);

        //copy all the keys and rids over from middleIndex
        for (int i = middleIndex; i < INTARRAYLEAFSIZE; i++)
        {
            newLeaf->keyArray[i - middleIndex] = fullLeaf->keyArray[i];
            fullLeaf->keyArray[i] = INT32_MAX;
            newLeaf->ridArray[i - middleIndex] = fullLeaf->ridArray[i];
            //not NULLing rids here again, just check if corresponding key is null to see if the data is valid
        }

        newLeaf->rightSibPageNo = fullLeaf->rightSibPageNo;
        fullLeaf->rightSibPageNo = newPageId;

        //unpin the page that was created
        bufMgr->unPinPage(file, newPageId, true);
    }
    else
    {
        //cast the fullPage to a nonleaf
        NonLeafNodeInt *fullNode = (NonLeafNodeInt *)fullPage;

        //create a new page
        Page *newNodePage;
        bufMgr->allocPage(file, newPageId, newNodePage);
        NonLeafNodeInt *newNode = (NonLeafNodeInt *)newNodePage;

        // set new node's level
        // std::cout<<"fullNodeLEVEL: "<<fullNode->level<<std::endl;
        newNode->level = fullNode->level;

        // set all in the new page to be NULL
        newNode->pageNoArray[INTARRAYNONLEAFSIZE];
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
        {
            newNode->keyArray[i] = INT32_MAX;
            newNode->pageNoArray[i] = NULL;
        }

        //get the middle value and index from the page
        this->findMiddle(fullPage, false, keyPtr, middleIndex);

        //the keyPtr we are trying to insert is the middle one
        if (*((int *)keyPtr) == middleInt)
        {
            newNode->pageNoArray[0] = newPageIdChild;

            for (int i = middleIndex + 1; i < INTARRAYNONLEAFSIZE; i++)
            {
                newNode->keyArray[i - middleIndex - 1] = fullNode->keyArray[i];
                fullNode->keyArray[i] = INT32_MAX;
                newNode->pageNoArray[i - middleIndex] = fullNode->pageNoArray[i + 1];
            }
        }
        else
        {
            for (int i = middleIndex + 1; i < INTARRAYNONLEAFSIZE; i++)
            {
                newNode->keyArray[i - middleIndex - 1] = fullNode->keyArray[i];
                fullNode->keyArray[i] = INT32_MAX;
                newNode->pageNoArray[i - middleIndex - 1] = fullNode->pageNoArray[i];
            }
            newNode->pageNoArray[INTARRAYNONLEAFSIZE - middleIndex - 1] = fullNode->pageNoArray[INTARRAYNONLEAFSIZE];
        }

        //unpin the page that was created
        bufMgr->unPinPage(file, newPageId, true);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::findMiddle()
// -----------------------------------------------------------------------------
//
const void BTreeIndex::findMiddle(Page *page, bool isLeaf, const void *keyPtr, int &middleIndex)
{
    if (isLeaf)
    {
        LeafNodeInt *leaf = (LeafNodeInt *)page;
        if (INTARRAYLEAFSIZE % 2 == 0)
        {
            if (*((int *)keyPtr) > leaf->keyArray[INTARRAYLEAFSIZE / 2 - 1] && *((int *)keyPtr) < leaf->keyArray[INTARRAYLEAFSIZE / 2])
            {
                middleInt = *((int *)keyPtr);
                middleIndex = INTARRAYLEAFSIZE / 2;
            }
            else if (*((int *)keyPtr) > leaf->keyArray[INTARRAYLEAFSIZE / 2])
            {
                middleInt = (leaf->keyArray[INTARRAYLEAFSIZE / 2]);
                middleIndex = INTARRAYLEAFSIZE / 2;
            }
            else
            {
                middleInt = (leaf->keyArray[INTARRAYLEAFSIZE / 2 - 1]);
                middleIndex = INTARRAYLEAFSIZE / 2 - 1;
            }
        }
        else
        {
            middleInt = (leaf->keyArray[INTARRAYLEAFSIZE / 2]);
            middleIndex = INTARRAYLEAFSIZE / 2;
        }
    }
    else //non leaf
    {
        // current node and key
        NonLeafNodeInt *node = (NonLeafNodeInt *)page;

        if (INTARRAYNONLEAFSIZE % 2 == 0)
        {
            if (*((int *)keyPtr) > node->keyArray[INTARRAYNONLEAFSIZE / 2 - 1] && *((int *)keyPtr) < node->keyArray[INTARRAYNONLEAFSIZE / 2])
            {
                middleInt = *((int *)keyPtr);
                middleIndex = INTARRAYNONLEAFSIZE / 2 - 1;
            }
            else if (*((int *)keyPtr) > node->keyArray[INTARRAYNONLEAFSIZE / 2])
            {
                middleInt = (node->keyArray[INTARRAYNONLEAFSIZE / 2]);
                middleIndex = INTARRAYNONLEAFSIZE / 2;
            }
            else if (*((int *)keyPtr) < node->keyArray[INTARRAYNONLEAFSIZE / 2 - 1])
            {
                middleInt = (node->keyArray[INTARRAYNONLEAFSIZE / 2 - 1]);
                middleIndex = INTARRAYNONLEAFSIZE / 2 - 1;
            }
            // assume no duplicate
        }
        else
        {
            if (*((int *)keyPtr) > node->keyArray[INTARRAYNONLEAFSIZE / 2 - 1] && *((int *)keyPtr) < node->keyArray[INTARRAYNONLEAFSIZE / 2])
            {
                middleInt = *((int *)keyPtr);
                middleIndex = INTARRAYNONLEAFSIZE / 2 - 1;
            }
            else if (*((int *)keyPtr) > node->keyArray[INTARRAYNONLEAFSIZE / 2] && *((int *)keyPtr) < node->keyArray[INTARRAYNONLEAFSIZE / 2 + 1])
            {
                middleInt = *((int *)keyPtr);
                middleIndex = INTARRAYNONLEAFSIZE / 2;
            }
            else if (*((int *)keyPtr) < node->keyArray[INTARRAYNONLEAFSIZE / 2 - 1])
            {
                middleInt = (node->keyArray[INTARRAYNONLEAFSIZE / 2 - 1]);
                middleIndex = INTARRAYNONLEAFSIZE / 2 - 1;
            }
            else if (*((int *)keyPtr) > node->keyArray[INTARRAYNONLEAFSIZE / 2])
            {
                middleInt = (node->keyArray[INTARRAYNONLEAFSIZE / 2]);
                middleIndex = INTARRAYNONLEAFSIZE / 2;
            }
            // assume no duplicate
        }
    }
}

} // namespace badgerdb
