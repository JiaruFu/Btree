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
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	// assume the key can only be integer
	NonLeafNodeInt *rootNode = (NonLeafNodeInt *)rootPage;
	bool splited; // record whether need to split the root node
	bool childLeaf;
	PageId newPageId;

	// Start from root to recursively find out the leaf to insert the entry in.
	recurseInsert(rootPage, rootNode->level, true, key, rid, splited, childLeaf, newPageId);

	//if the root splited, update the metapage
	if (splited)
	{
		if (rootNode->keyArray[nodeOccupancy - 1] == INT_MAX)
		{
			//we have room so just put it on this page
			insertNonLeaf(rootPage, (void *)&middleInt, newPageId);
		}
		else
		{
			PageId addedPageId;
			splite(rootPage, false, (void *)&middleInt, newPageId, addedPageId);

			//create a new NonLeafPage and put the middle int on it
			Page *newRootPage;
			PageId newRootPageId;
			bufMgr->allocPage(file, newRootPageId, newRootPage);
			NonLeafNodeInt *newRoot = (NonLeafNodeInt *)newRootPage;

			//we know this can never be just above the leaves so set level to 0
			newRoot->level = 0;

			//null eveything in this new page
			newRoot->pageNoArray[nodeOccupancy] = NULL;
			for (int i = 0; i < nodeOccupancy; i++)
			{
				newRoot->keyArray[i] = INT_MAX;
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
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
								 const Operator lowOpParm,
								 const void *highValParm,
								 const Operator highOpParm)
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid)
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::recurseInsert
// -----------------------------------------------------------------------------
//
const void BTreeIndex::recurseInsert(Page *page, int level, bol isRoot, const void *keyPtr, const RecordId rid, bool &splited, bol &childLeaf, PageId &newPageId)
{
	// key and node of current page
	int key = *((int *)keyPtr);
	NonLeafNodeInt *node = (NonLeafNodeInt *)page;

	if (isRoot && node->keyArray[0] == INT_MAX)
	{
		node->keyArray[0] = key;
	}

	int index; // index of node in pageNoArray to recurse on
	if (level == 0)
	{
		childLeaf = false;

		//find the key to recurse on
		findPageNo(page, keyPtr, &index);

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
			//if there is room we can just add the key
			if (node->keyArray[nodeOccupancy - 1] == INT_MAX)
			{
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
	}
	else
	{
		childLeaf = true;
		//we are the parent of the leaf
		//find what page the leaf is on
		findPageNo(page, keyPtr, &index);

		//read in the leaf page and cast
		Page *leafPage;
		PageId leafPageId = node->pageNoArray[index];
		bufMgr->readPage(file, leafPageId, leafPage);
		LeafNodeInt *leaf = (LeafNodeInt *)leafPage;

		//find the index into the key array where the rid would go
		findKey(leafPage, keyPtr, &index);

		//insert into leaf
		//if the last place in the leaf is NULL then we dont have to restructure
		if (leaf->keyArray[leafOccupancy - 1] == INT_MAX)
		{
			splited = false;

			//move entries over one place (start at the end)
			for (int i = leafOccupancy - 1; i > index; i--)
			{
				leaf->keyArray[i] = leaf->keyArray[i - 1];
				leaf->ridArray[i] = leaf->ridArray[i - 1];
			}

			//actually insert the entry
			leaf->keyArray[index] = key;
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

				findKey(newLeafPage, keyPtr, &index);

				//move the keys over on this page
				for (int i = leafOccupancy - 1; i > index; i--)
				{
					newLeaf->keyArray[i] = newLeaf->keyArray[i - 1];
					newLeaf->ridArray[i].page_number = newLeaf->ridArray[i - 1].page_number;
					newLeaf->ridArray[i].slot_number = newLeaf->ridArray[i - 1].slot_number;
				}

				//actually insert the record
				newLeaf->keyArray[index] = key;
				newLeaf->ridArray[index].page_number = rid.page_number;
				newLeaf->ridArray[index].slot_number = rid.slot_number;

				// unpin pages
				bufMgr->unPinPage(file, newPageId, true);
			}
			else
			{
				findKey(leafPage, keyPtr, &index);

				//move the keys
				for (int i = leafOccupancy - 1; i > index; i--)
				{
					leaf->keyArray[i] = leaf->keyArray[i - 1];
					leaf->ridArray[i].page_number = leaf->ridArray[i - 1].page_number;
					leaf->ridArray[i].slot_number = leaf->ridArray[i - 1].slot_number;
				}

				// insert
				leaf->keyArray[index] = key;
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
	// current key and node
	int key = *((int *)keyPtr);
	NonLeafNodeInt *node = (NonLeafNodeInt *)page;

	// iterate the key array to find the suitable index to recurse on
	for (int i = 0; i < nodeOccupancy; i++)
	{
		if (key < node->keyArray[0])
		{
			index = 0;
			return;
		}
		else if (key >= node->keyArray[i] && (i == nodeOccupancy - 1 || node->keyArray[i + 1] == INT_MAX))
		{
			// reach the end of the array
			return i + 1;
			return;
		}
		else if (key >= node->keyArray[i] && key < node->keyArray[i + 1])
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
const void BTreeIndex::findKey(Page *leafPage, const void *keyPtr, int &index);
{
	// current key and node
	int key = *((int *)keyPtr);
	LeafNodeInt *leaf = (LeafNodeInt *)page;

	// iterate the key array to find the suitable index to recurse on
	// assume no duplicate key
	for (int i = 0; i < leafOccupancy; i++)
	{
		if (leaf->keyArray[0] == INT_MAX || key < leaf->keyArray[0])
		{
			index = 0;
			return;
		}
		else if (key > leaf->keyArray[i] && (i == leafOccupancy - 1 || leaf->keyArray[i + 1] == INT_MAX))
		{
			// reach the end of the array
			index = i + 1;
			return;
		}
		else if (key > leaf->keyArray[i] && key < leaf->keyArray[i + 1])
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
	int key = *((int *)keyPtr);
	NonLeafNodeInt *node = (NonLeafNodeInt *)page;

	// iterate the key array to find where to insert
	for (int i = 0; i < nodeOccupancy; i++)
	{
		if (node->keyArray[i] == INT_MAX)
		{
			node->keyArray[i] = key;
			node->pageNoArray[i + 1] = pageId;
			return;
		}
		else if (key < node->keyArray[i])
		{
			// shift the later part of the array
			for (int j = nodeOccupancy - 1; j > i; j--)
			{
				node->keyArray[j] = node->keyArray[j - 1];
				node->pageNoArray[j + 1] = node->pageNoArray[j];
			}
			// insert
			node->keyArray[i] = key;
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
	int key = *((int *)keyPtr); // current key
	int middleIndex;			// records the middle index to split

	if (isLeaf)
	{
		LeafNodeInt *fullLeaf = (LeafNodeInt *)fullPage;

		//create a new page
		Page *newLeafPage;
		bufMgr->allocPage(file, newPageId, newLeafPage);
		LeafNodeInt *newLeaf = (LeafNodeInt *)newLeafPage;

		//NULL everything in the new page
		for (int i = 0; i < leafOccupancy; i++)
		{
			newLeaf->keyArray[i] = INT_MAX;
			//not NULLing the rids here because we just assume if the key is NULL then so is the associated rid so don't access it
		}

		//get the middle value and index
		int middleIndex;
		findMiddleValue(fullPage, true, keyPtr, middleIndex);

		//copy all the keys and rids over from middleIndex
		for (int i = middleIndex; i < leafOccupancy; i++)
		{
			newLeaf->keyArray[i - middleIndex] = fullLeaf->keyArray[i];
			fullLeaf->keyArray[i] = INT_MAX;
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

		// set all in the new page to be NULL
		newNode->pageNoArray[nodeOccupancy] = NULL;
		for (int i = 0; i < nodeOccupancy; i++)
		{
			newNode->keyArray[i] = INT_MAX;
			newNode->pageNoArray[i] = NULL;
		}

		//get the middle value and index from the page
		findMiddleValue(fullPage, false, keyPtr, middleIndex);

		//the keyPtr we are trying to insert is the middle one
		if (*((int *)keyPtr) == middleInt)
		{
			newNode->pageNoArray[0] = newPageIdFromChild;

			for (int i = middleIndex + 1; i < nodeOccupancy; i++)
			{
				newNode->keyArray[i - middleIndex - 1] = fullNode->keyArray[i];
				fullNode->keyArray[i] = INT_MAX;
				newNode->pageNoArray[i - middleIndex] = fullNode->pageNoArray[i + 1];
			}
		}
		else
		{
			for (int i = middleIndex + 1; i < nodeOccupancy; i++)
			{
				newNode->keyArray[i - middleIndex - 1] = fullNode->keyArray[i];
				fullNode->keyArray[i] = INT_MAX;
				newNode->pageNoArray[i - middleIndex - 1] = fullNode->pageNoArray[i];
			}
			newNode->pageNoArray[nodeOccupancy - middleIndex - 1] = fullNode->pageNoArray[nodeOccupancy];
		}

		//unpin the page that was created
		bufMgr->unPinPage(file, newPageId, true);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::findMiddle()
// -----------------------------------------------------------------------------
//
const void BTreeIndex::findMiddle()
{
	if (isLeaf)
	{
		LeafNodeInt *leaf = (LeafNodeInt *)page;
		int key = *((int *)keyPtr);
		if (leafOccupancy % 2 == 0)
		{
			if (key > leaf->keyArray[leafOccupancy / 2 - 1] && key < leaf->keyArray[leafOccupancy / 2])
			{
				middleInt = key;
				middleIndex = leafOccupancy / 2;
			}
			else if (key > leaf->keyArray[leafOccupancy / 2])
			{
				middleInt = (leaf->keyArray[leafOccupancy / 2]);
				middleIndex = leafOccupancy / 2;
			}
			else
			{
				middleInt = (leaf->keyArray[leafOccupancy / 2 - 1]);
				middleIndex = leafOccupancy / 2 - 1;
			}
		}
		else
		{
			middleInt = (leaf->keyArray[leafOccupancy / 2]);
			middleIndex = leafOccupancy / 2;
		}
	}
	else //non leaf
	{
		// current node and key
		NonLeafNodeInt *node = (NonLeafNodeInt *)page;
		int key = *((int *)keyPtr);

		if (nodeOccupancy % 2 == 0)
		{
			if (key > node->keyArray[nodeOccupancy / 2 - 1] && key < node->keyArray[nodeOccupancy / 2])
			{
				middleInt = key;
				middleIndex = nodeOccupancy / 2 - 1;
			}
			else if (key > node->keyArray[nodeOccupancy / 2])
			{
				middleInt = (node->keyArray[nodeOccupancy / 2]);
				middleIndex = nodeOccupancy / 2;
			}
			else if (key < node->keyArray[nodeOccupancy / 2 - 1])
			{
				middleInt = (node->keyArray[nodeOccupancy / 2 - 1]);
				middleIndex = nodeOccupancy / 2 - 1;
			}
			// assume no duplicate
		}
		else
		{
			if (key > node->keyArray[nodeOccupancy / 2 - 1] && key < node->keyArray[nodeOccupancy / 2])
			{
				middleInt = key;
				middleIndex = nodeOccupancy / 2 - 1;
			}
			else if (key > node->keyArray[nodeOccupancy / 2] && key < node->keyArray[nodeOccupancy / 2 + 1])
			{
				middleInt = key;
				middleIndex = nodeOccupancy / 2;
			}
			else if (key < node->keyArray[nodeOccupancy / 2 - 1])
			{
				middleInt = (node->keyArray[nodeOccupancy / 2 - 1]);
				middleIndex = nodeOccupancy / 2 - 1;
			}
			else if (key > node->keyArray[nodeOccupancy / 2])
			{
				middleInt = (node->keyArray[nodeOccupancy / 2]);
				middleIndex = nodeOccupancy / 2;
			}
			// assume no duplicate
		}
	}
}

} // namespace badgerdb
