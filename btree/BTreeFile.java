/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package btree;

import diskmgr.Page;
import exceptions.AddFileEntryException;
import exceptions.ConstructPageException;
import exceptions.ConvertException;
import exceptions.DeleteFashionException;
import exceptions.DeleteFileEntryException;
import exceptions.DeleteRecException;
import exceptions.FreePageException;
import exceptions.GetFileEntryException;
import exceptions.HashEntryNotFoundException;
import exceptions.IndexFullDeleteException;
import exceptions.IndexInsertRecException;
import exceptions.IndexSearchException;
import exceptions.InsertException;
import exceptions.InsertRecException;
import exceptions.InvalidFrameNumberException;
import exceptions.IteratorException;
import exceptions.KeyNotMatchException;
import exceptions.KeyTooLongException;
import exceptions.LeafDeleteException;
import exceptions.LeafInsertRecException;
import exceptions.LeafRedistributeException;
import exceptions.NodeNotMatchException;
import exceptions.PageUnpinnedException;
import exceptions.PinPageException;
import exceptions.RecordNotFoundException;
import exceptions.RedistributeException;
import exceptions.ReplacerException;
import exceptions.UnpinPageException;
import global.AttrType;
import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import heap.HFPage;
import index.IndexFile;
import index.Key;
import index.KeyEntry;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import btree.page.BTIndexPage;
import btree.page.BTLeafPage;
import btree.page.BTSortedPage;
import btree.page.BTHeaderPage;
 
/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst
{
	public static final int NAIVE_DELETE = 0;

	public static final int FULL_DELETE = 1;

	private final static int MAGIC0 = 1989;

	private PageId headerPageId;
	
	private BTHeaderPage headerPage;
	
	private String dbName;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this
	 *         btree file.
	 */
	public BTHeaderPage getHeaderPage()
	{
		return headerPage;
	}
	
	//	Helper method to get the page id of the header page
	private PageId getHeaderPageId(String filename) throws GetFileEntryException {
		try {
			return Minibase.JavabaseDB.get_file_entry(filename);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e,"");
		}
	}
	
	//	Add a file entry to the header page
	private void addFileEntry(String filename, PageId pageId) throws AddFileEntryException {
		try {
			Minibase.JavabaseDB.add_file_entry(filename, pageId);
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e,"");
		}      
	}
	
	//	Delete file entry from the header page
	private void deleteFileEntry(String filename) throws DeleteFileEntryException {
		try {
			Minibase.JavabaseDB.delete_file_entry(filename);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e,"");
		} 
	}
	
	private Page pinPage(PageId pageno) throws PinPageException {
		try {
		    Page page = new Page();
		    Minibase.JavabaseBM.pinPage(pageno, page, false);
		    return page;
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e,"");
		}
	}
	
	private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException
	{ 
	    try{
		    Minibase.JavabaseBM.unpinPage(pageno, dirty);    
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		} 
	}
	
	private void freePage(PageId pageno) throws FreePageException
	{
	    try{
			Minibase.JavabaseBM.freePage(pageno);    
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e,"");
		}       
	}
	
	private void updateHeader(PageId newRoot) throws IOException, PinPageException, UnpinPageException {
		BTHeaderPage header;
		header = new BTHeaderPage(pinPage(headerPageId));
		header.set_rootId(newRoot);
		unpinPage(headerPageId, true);
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 * 
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not get the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException
	{
		headerPageId = getHeaderPageId(filename);
		headerPage = new BTHeaderPage(headerPageId);
		dbName = filename;
	}

	/**
	 * if index file exists, open it; else create it.
	 * 
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException
	{
		headerPageId = getHeaderPageId(filename);
		//index file does not exist
		if (headerPageId == null) {
			headerPage = new BTHeaderPage();
			headerPageId = headerPage.getPageId();
			addFileEntry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);    
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
		}
		else {
			headerPage = new BTHeaderPage(headerPageId);  
		}      
		dbName = filename;
	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 * 
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException, UnpinPageException
	{
		if (headerPage != null) {
			unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 * 
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException
	{
		if(headerPage != null) {
			PageId rootId = headerPage.get_rootId();
			
			if (rootId.pid != INVALID_PAGE) 
				_destroyFile(rootId);
			
			unpinPage(headerPageId, false);
			freePage(headerPageId);      
			deleteFileEntry(dbName);
			headerPage = null;
		}
	}
	
	//recursively traverse the pages to destroy the tree
	private void  _destroyFile(PageId pageId) 
		    throws IOException, 
			   IteratorException, 
			   PinPageException,
		       ConstructPageException, 
			   UnpinPageException, 
			   FreePageException
	{ 
	    BTSortedPage sortedPage;
		Page page = pinPage(pageId);
		sortedPage= new BTSortedPage(page, headerPage.get_keyType());
		      
		if (sortedPage.getType() == BTSortedPage.INDEX) {
			BTIndexPage indexPage= new BTIndexPage(page, headerPage.get_keyType());
		    RID rid = new RID();
		    PageId childId;
		    KeyEntry entry;
		    for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) { 
			  childId = (PageId) (entry.getData());
			  _destroyFile(childId);
			}
		} 
		else { 	
			unpinPage(pageId, false);
			freePage(pageId);
		}
	}


	/**
	 * insert record with the given key and rid
	 * 
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(Key key, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
	IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
	NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
	LeafDeleteException, InsertException, IOException
	{
		KeyEntry newRootEntry;

		//if no root exists, we create a new root page, which will be a leaf page
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			PageId newRootPageId;
			BTLeafPage newRootPage;

			newRootPage = new BTLeafPage(headerPage.get_keyType());
			newRootPageId = newRootPage.getCurPage();

			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));

			newRootPage.insertRecord(key, rid);

			unpinPage(newRootPageId, true);
			updateHeader(newRootPageId);
			
			return;
		}

		//if the root already exists, we insert the record
		newRootEntry = _insert(key, rid, headerPage.get_rootId());

		//if root split has occurred due to record insertion, we update root information
		if (newRootEntry != null) {
			BTIndexPage newRootPage;
			PageId newRootPageId;

			newRootPage = new BTIndexPage(headerPage.get_keyType());
			newRootPageId = newRootPage.getCurPage();

			newRootPage.insertKey(newRootEntry.key, ((PageId) newRootEntry.getData()));

			// the old root becomes the left child of the new root
			newRootPage.setPrevPage(headerPage.get_rootId());

			unpinPage(newRootPageId, true);

			updateHeader(newRootPageId);
		}

		return;
	}

	private KeyEntry _insert(Key key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException

	{

		BTSortedPage currentPage;
		Page page;
		KeyEntry upEntry;

		page = pinPage(currentPageId);
		currentPage = new BTSortedPage(page, headerPage.get_keyType());

		// for Index pages, we recurse and split (if required)
		// for Leaf pages, we insert (key, rid) pair and split if necessary

		if (currentPage.getType() ==  BTSortedPage.INDEX) {
			BTIndexPage currentIndexPage = new BTIndexPage(page, headerPage.get_keyType());
			PageId currentIndexPageId = currentPageId;
			PageId nextPageId;

			nextPageId = currentIndexPage.getPageNoByKey(key);

			// now unpin the page, recurse and then pin it again
			unpinPage(currentIndexPageId, false);

			upEntry = _insert(key, rid, nextPageId);

			//no split has occurred on the lower level
			if (upEntry == null)
				return null;
			
			// a child on the lower level has split; upEntry has to be inserted on this Index page
			currentIndexPage = new BTIndexPage(pinPage(currentPageId), headerPage.get_keyType());

			// check whether there can still be entries inserted on that page
			if (currentIndexPage.available_space() >= upEntry.getSizeInBytes()) {

				// no split has occurred
				currentIndexPage.insertKey(upEntry.key, ((PageId) upEntry.getData()));

				unpinPage(currentIndexPageId, true);

				return null;
			}

			// not enough space on current page 
			BTIndexPage newIndexPage;
			PageId newIndexPageId;

			// therefore we have to allocate a new index page and redistribute index entries
			newIndexPage = new BTIndexPage(headerPage.get_keyType());
			newIndexPageId = newIndexPage.getCurPage();

			KeyEntry tmpEntry;
			PageId tmpPageId;
			RID insertRid;
			RID delRid = new RID();

			for (tmpEntry = currentIndexPage.getFirst(delRid); tmpEntry != null; tmpEntry = currentIndexPage
					.getFirst(delRid)) {
				newIndexPage.insertKey(tmpEntry.key, ((PageId) tmpEntry.getData()));
				currentIndexPage.deleteSortedRecord(delRid);
			}
			
			//trying to split equally
			RID firstRid = new RID();
			KeyEntry undoEntry = null;
			for (tmpEntry = newIndexPage.getFirst(firstRid); (currentIndexPage.available_space() > newIndexPage
					.available_space()); tmpEntry = newIndexPage.getFirst(firstRid)) {
				// inserting (key,pageId) into the new index page
				undoEntry = tmpEntry;
				currentIndexPage.insertKey(tmpEntry.key, ((PageId) tmpEntry.getData()));
				newIndexPage.deleteSortedRecord(firstRid);
			}

			// undoing the final record
			if (currentIndexPage.available_space() < newIndexPage.available_space()) {

				newIndexPage.insertKey(undoEntry.key, ((PageId) undoEntry.getData()));

				currentIndexPage.deleteSortedRecord(
						new RID(currentIndexPage.getCurPage(), (int) currentIndexPage.getSlotCnt() - 1));
			}

			// checking whether (newKey, newIndexPageId) pair should be inserted on new or old index page

			tmpEntry = newIndexPage.getFirst(firstRid);

			if (upEntry.key.compareTo(tmpEntry.key) >= 0) {
				// the new data entry needs to be inserted on the new index page
				newIndexPage.insertKey(upEntry.key, ((PageId) upEntry.getData()));
			} 
			else {
				currentIndexPage.insertKey(upEntry.key,((PageId) upEntry.getData()));

				int i = (int) currentIndexPage.getSlotCnt() - 1;
				tmpEntry =  new KeyEntry(currentIndexPage.getpage(), currentIndexPage.getSlotOffset(i),
						currentIndexPage.getSlotLength(i), headerPage.get_keyType(),  BTSortedPage.INDEX);
				newIndexPage.insertKey(tmpEntry.key, ((PageId) tmpEntry.getData()));
				currentIndexPage.deleteSortedRecord(new RID(currentIndexPage.getCurPage(), i));

			}

			unpinPage(currentIndexPageId, true);

			upEntry = newIndexPage.getFirst(delRid);

			// setting prevPageId of the newIndexPage to the pageId of the deleted entry
			newIndexPage.setPrevPage(((PageId) upEntry.getData()));

			// deleting first record on new index page
			newIndexPage.deleteSortedRecord(delRid);

			unpinPage(newIndexPageId, true);

			upEntry.setData(newIndexPageId);

			return upEntry;

		}

		else if (currentPage.getType() ==  BTSortedPage.LEAF) {
			BTLeafPage currentLeafPage = new BTLeafPage(page, headerPage.get_keyType());

			PageId currentLeafPageId = currentPageId;


			// check whether there can still be entries inserted on that page
			if (currentLeafPage.available_space() >= (new KeyEntry(key, rid).getSizeInBytes())) {
				// no split has occurred

				currentLeafPage.insertRecord(key, rid);

				unpinPage(currentLeafPageId, true /* DIRTY */);

				return null;
			}

			//not enough space on the current leaf page
			BTLeafPage newLeafPage;
			PageId newLeafPageId;
			
			// we allocate a new leaf page and redistribute the data entries
			newLeafPage = new BTLeafPage(headerPage.get_keyType());
			newLeafPageId = newLeafPage.getCurPage();

			newLeafPage.setNextPage(currentLeafPage.getNextPage());
			newLeafPage.setPrevPage(currentLeafPageId); // for dbl-linked list
			currentLeafPage.setNextPage(newLeafPageId);

			// changing the prevPage pointer on the next page:
			PageId rightPageId;
			rightPageId = newLeafPage.getNextPage();
			if (rightPageId.pid != INVALID_PAGE) {
				BTLeafPage rightPage;
				rightPage = new BTLeafPage(rightPageId, headerPage.get_keyType());

				rightPage.setPrevPage(newLeafPageId);
				unpinPage(rightPageId, true /* = DIRTY */);
			}

			KeyEntry tmpEntry;
			RID firstRid = new RID();

			for (tmpEntry = currentLeafPage.getFirst(firstRid); tmpEntry != null; tmpEntry = currentLeafPage
					.getFirst(firstRid)) {

				newLeafPage.insertRecord(tmpEntry.key, (RID) tmpEntry.getData());
				currentLeafPage.deleteSortedRecord(firstRid);

			}

			KeyEntry undoEntry = null;
			for (tmpEntry = newLeafPage.getFirst(firstRid); newLeafPage.available_space() < currentLeafPage
					.available_space(); tmpEntry = newLeafPage.getFirst(firstRid)) {
				undoEntry = tmpEntry;
				currentLeafPage.insertRecord(tmpEntry.key, (RID) tmpEntry.getData());
				newLeafPage.deleteSortedRecord(firstRid);
			}

			if (key.compareTo(undoEntry.key) < 0) {
				// undoing the final record
				if (currentLeafPage.available_space() < newLeafPage.available_space()) {
					newLeafPage.insertRecord(undoEntry.key, (RID) undoEntry.getData());

					currentLeafPage.deleteSortedRecord(
							new RID(currentLeafPage.getCurPage(), (int) currentLeafPage.getSlotCnt() - 1));
				}
			}

			// checking whether (key, rid) pair will be inserted on the new or old leaf page

			if (key.compareTo(undoEntry.key) >= 0) {
				// the new data entry belongs on the new Leaf page
				newLeafPage.insertRecord(key, rid);

			} else {
				currentLeafPage.insertRecord(key, rid);
			}

			unpinPage(currentLeafPageId, true);

			tmpEntry = newLeafPage.getFirst(firstRid);
			upEntry = new KeyEntry(tmpEntry.key, newLeafPageId);

			unpinPage(newLeafPageId, true);

			return upEntry;
		} else {
			throw new InsertException(null, "");
		}
	}

	
	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 * 
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 * 
	 */
	public boolean delete(Key key, RID rid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		BTLeafPage leafPage;
		RID curRid = new RID(); // iterator
		Key curkey;
		RID dummyRid;
		PageId nextpage;
		boolean deleted;
		KeyEntry entry;

		leafPage = findRunStart(key, curRid); // find first page,rid of key
		if (leafPage == null)
			return false;

		entry = leafPage.getCurrent(curRid);

		while (true) {

			while (entry == null) { // have to go right
				nextpage = leafPage.getNextPage();
				unpinPage(leafPage.getCurPage(), false);
				if (nextpage.pid == INVALID_PAGE) {
					return false;
				}

				leafPage = new BTLeafPage(pinPage(nextpage), headerPage.get_keyType());
				entry = leafPage.getFirst(new RID());
			}

			if (key.compareTo(entry.key) > 0)
				break;

			if (leafPage.delEntry(new KeyEntry(key, rid)) == true) {

				// successfully found <key, rid> on this page and deleted it.
				// unpin dirty page and return OK.
				unpinPage(leafPage.getCurPage(), true);

				return true;
			}

			nextpage = leafPage.getNextPage();
			unpinPage(leafPage.getCurPage(), false);

			leafPage = new BTLeafPage(pinPage(nextpage), headerPage.get_keyType());

			entry = leafPage.getFirst(curRid);
		}

		/*
		 * We reached a page with first key > `key', so return an error. We should have
		 * got true back from delUserRid above.
		 */

		unpinPage(leafPage.getCurPage(), false);
		return false;
	}

	BTLeafPage findRunStart(Key lo_key, RID startrid) throws IOException, IteratorException, KeyNotMatchException,
			ConstructPageException, PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		while (sortPage.getType() == BTSortedPage.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null && curEntry.key.compareTo(lo_key) < 0) {

				prevpageno = ((PageId) curEntry.getData());
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno, false);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno, false);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}


		if (lo_key == null) {
			return pageLeaf;
		}

		while (curEntry.key.compareTo(lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno, false);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}
		  
	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index (2) lo_key = null, hi_key!= null range scan from min
	 * to the hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key
	 * to max (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (
	 * might not unique) (5) lo_key!= null, hi_key!= null, lo_key < hi_key range
	 * scan from lo_key to hi_key
	 * 
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(Key lo_key, Key hi_key)
			throws IOException, KeyNotMatchException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
	      if ( headerPage.get_rootId().pid==INVALID_PAGE) {
		scan.leafPage=null;
		return scan;
	      }
	      
	      scan.treeFilename=dbName;
	      scan.endkey=hi_key;
	      scan.didfirst=false;
	      scan.deletedcurrent=false;
	      scan.curRid=new RID();     
	      scan.keyType=headerPage.get_keyType();
	      scan.maxKeysize=headerPage.get_maxKeySize();
	      scan.bfile=this;
	      
	      //this sets up scan at the starting position, ready for iteration
	      scan.leafPage=findRunStart( lo_key, scan.curRid);
	      return scan;
	}


	/**
	 * For debug. Print the B+ tree structure out
	 * 
	 * @param header
	 *            the head page of the B+ tree file
	 * @exception IOException
	 *                error from the lower layer
	 * @exception ConstructPageException
	 *                error from BT page constructor
	 * @exception IteratorException
	 *                error from iterator
	 * @exception HashEntryNotFoundException
	 *                error from lower layer
	 * @exception InvalidFrameNumberException
	 *                error from lower layer
	 * @exception PageUnpinnedException
	 *                error from lower layer
	 * @exception ReplacerException
	 *                error from lower layer
	 */
	public void printBTree() throws IOException,
			ConstructPageException, IteratorException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			PageUnpinnedException, ReplacerException
	{
		BTHeaderPage header = getHeaderPage();
		if (header.get_rootId().pid == INVALID_PAGE)
		{
			System.out.println("The Tree is Empty!!!");
			return;
		}

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The B+ Tree Structure---------------");

		System.out.println("header page: " + header.get_rootId());

		_printTree(header.get_rootId(), "", header.get_keyType());

		System.out.println("--------------- End ---------------");
		System.out.println("");
		System.out.println("");
	}

	private void _printTree(PageId currentPageId, String prefix, int keyType) throws IOException, ConstructPageException,
			IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException,
			ReplacerException
	{

		BTSortedPage sortedPage = new BTSortedPage(currentPageId, keyType);
		prefix = prefix + "    ";
		
		// for index pages, go through their child pages
		if (sortedPage.getType() == BTSortedPage.INDEX)
		{
			BTIndexPage indexPage = new BTIndexPage((Page) sortedPage, keyType);

			System.out.println(prefix + "index page: " + currentPageId);
			System.out.println(prefix + "  first child: " + 
					indexPage.getPrevPage());
			
			_printTree(indexPage.getPrevPage(), prefix, keyType);

			RID rid = new RID();
			for (KeyEntry entry = indexPage.getFirst(rid); 
				 entry != null;  
				 entry = indexPage.getNext(rid))
			{
				System.out.println(prefix + "  key: " + entry.key + ", page: ");
				_printTree((PageId) entry.getData(), prefix, keyType);
			}
		}
		
		// for leaf pages, iterate through the keys and print them out
		else if (sortedPage.getType() == BTSortedPage.LEAF)
		{
			BTLeafPage leafPage = new BTLeafPage((Page) sortedPage, keyType);
			RID rid = new RID();
			System.out.println(prefix + "leaf page: " + sortedPage);
			for (KeyEntry entry = leafPage.getFirst(rid); 
				 entry != null;  
				 entry = leafPage.getNext(rid))
			{
				if (keyType == AttrType.attrInteger)
					System.out.println(prefix + "  ("
							+ entry.key + ",  "
							+ entry.getData() + " )");
				if (keyType == AttrType.attrString)
					System.out.println(prefix + "  ("
							+ entry.key + ",  "
							+ entry.getData() + " )");
			}
		}		
		
		Minibase.JavabaseBM.unpinPage(currentPageId, false);
	}


}
