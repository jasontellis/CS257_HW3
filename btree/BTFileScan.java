/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package btree;

import exceptions.ScanDeleteException;
import exceptions.ScanIteratorException;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.Minibase;

import index.IndexFileScan;
import index.Key;
import index.KeyEntry;

import java.io.IOException;

import btree.page.BTLeafPage;

/**
 * BTFileScan implements a search/iterate interface to B+ tree index files
 * (class BTreeFile). It derives from abstract base class IndexFileScan.
 */
public class BTFileScan extends IndexFileScan implements GlobalConst
{

	BTreeFile bfile;
	String treeFilename; // B+ tree we're scanning
	BTLeafPage leafPage; // leaf page containing current record
	RID curRid; // position in current leaf; note: this is
				// the RID of the key/RID pair within the
				// leaf page.
	boolean didfirst; // false only before getNext is called
	boolean deletedcurrent; // true after deleteCurrent is called (read
							// by get_next, written by deleteCurrent).

	Key endkey; // if NULL, then go all the way right
						// else, stop when current record > this value.
						// (that is, implement an inclusive range
						// scan -- the only way to do a search for
						// a single value).
	int keyType;
	int maxKeysize;

	/**
	 * Iterate once (during a scan).
	 * 
	 * @return null if done; otherwise next KeyDataEntry
	 * @exception ScanIteratorException
	 *                iterator error
	 */
	public KeyEntry get_next() throws ScanIteratorException
	{
	    KeyEntry entry;
	    PageId nextpage;
	    try {
	      if (leafPage == null)
	        return null;
	      
	      if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
	         didfirst = true;
	         deletedcurrent = false;
	         entry=leafPage.getCurrent(curRid);
	      }
	      else {
	         entry = leafPage.getNext(curRid);
	      }

	      while ( entry == null ) {
	         nextpage = leafPage.getNextPage();
	         Minibase.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
		 if (nextpage.pid == INVALID_PAGE) {
		    leafPage = null;
		    return null;
		 }

	         leafPage=new BTLeafPage(nextpage, keyType);
		 	
		 entry=leafPage.getFirst(curRid);
	      }

	      if (endkey != null)  
	        if ( entry.key.compareTo(endkey)  > 0) {
	        // went past right end of scan 
		    Minibase.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
	            leafPage=null;
		    return null;
	        }

	      return entry;
	    }
	    catch ( Exception e) {
	         e.printStackTrace();
	         throw new ScanIteratorException();
	    }
	}

	/**
	 * Delete currently-being-scanned(i.e., just scanned) data entry.
	 * 
	 * @exception ScanDeleteException
	 *                delete error when scan
	 */
	public void delete_current() throws ScanDeleteException
	{
		KeyEntry entry;
		try {
			if (leafPage == null) {
				System.out.println("No Record to delete!");
				throw new ScanDeleteException();
			}

			if ((deletedcurrent == true) || (didfirst == false))
				return;

			entry = leafPage.getCurrent(curRid);
			Minibase.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
			bfile.delete(entry.key, (RID) entry.getData());
			leafPage = bfile.findRunStart(entry.key, curRid);

			deletedcurrent = true;
			return;
		} catch (Exception e) {
			e.printStackTrace();
			throw new ScanDeleteException();
		}
	}

	/**
	 * max size of the key
	 * 
	 * @return the maxumum size of the key in BTFile
	 */
	public int keysize()
	{
		return maxKeysize;
	}

	/**
	 * destructor. unpin some pages if they are not unpinned already. and do
	 * some clearing work.
	 * 
	 * @exception IOException
	 *                error from the lower layer
	 * @exception exceptions.InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception exceptions.ReplacerException
	 *                error from the lower layer
	 * @exception exceptions.PageUnpinnedException
	 *                error from the lower layer
	 * @exception exceptions.HashEntryNotFoundException
	 *                error from the lower layer
	 */
	public void destroyBTreeFileScan() throws IOException,
			exceptions.InvalidFrameNumberException, exceptions.ReplacerException,
			exceptions.PageUnpinnedException, exceptions.HashEntryNotFoundException
	{
		if (leafPage != null) {
	         Minibase.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
	     } 
	     leafPage=null;
	}

}
