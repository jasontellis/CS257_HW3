package btree;

import exceptions.AddFileEntryException;
import exceptions.ConstructPageException;
import exceptions.GetFileEntryException;
import exceptions.HashEntryNotFoundException;
import exceptions.InvalidFrameNumberException;
import exceptions.PageUnpinnedException;
import exceptions.ReplacerException;
import global.AttrType;
import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.TestDriver;

import index.Key;
import index.KeyEntry;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Random;

import bufmgr.BufMgr;

/**
 * Note that in JAVA, methods can't be overridden to be more private. Therefore,
 * the declaration of all private functions are now declared protected as
 * opposed to the private type in C++.
 */

// watching point: RID rid, some of them may not have to be newed.
public class BTTest extends TestDriver implements GlobalConst
{

	public BTreeFile file;

	public int postfix = 0;

	public int keyType;

	public BTFileScan scan;

	public int deleteFashion;

	 
	/**
	 * BTDriver Constructor, inherited from TestDriver
	 */
	public BTTest()
	{
		super("BTree ");
	}
	
	public void initBeforeTests()
	{
		Random random = new Random();
		dbpath = "BTREE" + random.nextInt() + ".minibase-db";
		logpath = "BTREE" + random.nextInt() + ".minibase-log";

		try {
			Minibase.initBufMgr(new BufMgr(50,"bufmgr.Clock"));
		} catch(Exception ire)
		{
			ire.printStackTrace();
			System.exit(1);
		}
		
		Minibase.initDiskMgr(dbpath, 5000);

		System.out.println("\n" + "Running BTree tests...." + "\n");

		keyType = AttrType.attrInteger;
	}
	
	/**
	 * test1 - read in the file test.txt, and do whatever it says.  Format for 
	 * the file is one command per line, where a command is either:
	 * "i<integer>" - insert an integer value into the index
	 * "d<integer>" - delete an integer key from the index
	 * "p" - print the current state of the tree
	 */
	public boolean test1()
	{
		BTreeFile newIndex = null;
		
		try
		{
			newIndex = new BTreeFile("test",keyType,4,0);
			BufferedReader br = new BufferedReader(new FileReader("test.txt"));
			String line = null;
			RID dummy = new RID();
			while ((line = br.readLine()) != null)
			{
				//System.out.println("Processing line: " + line);
				switch (line.charAt(0))
				{
				case 'i':
					newIndex.insert(new Key(Integer.parseInt(line.substring(1))), dummy);
					break;
					
				case 'd':
					newIndex.delete(new Key(Integer.parseInt(line.substring(1))), dummy);
					break;
					
				case 'p':
					newIndex.printBTree();
					break;
					
				default:  
					System.out.println("Unable to parse line: " + line);
				}
			}
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			return(false);
		}
		
		//
		// all done, remove the index
		//
		
		try
		{
			newIndex.close();
			newIndex.destroyFile();
		} catch (Exception e)
		{
			e.printStackTrace();
			return(false);
		}
		
		return(true);
	}
	
	// 
	// convenience function for creating files to run through test1()
	//
	// it creates the specified number of keys in random order, then 
	// optionally deletes them.  It also optionally allows duplicate keys.
	//

	public static void createFile(String name, int numKeys, boolean doDelete, 
			boolean allowDups)
	{
		PrintStream ps = null;
		Random rand = new Random(System.currentTimeMillis());
		
		try
		{
			int written[] = new int[numKeys];
			int numWritten = 0;
			ps = new PrintStream(new FileOutputStream(name));
			
			// if we're in non-mixed mode, insert all nums, then delete all.			
			// insert a random key
			while (numWritten < numKeys)
			{
				int keyVal = (int)Math.round(rand.nextDouble() * (numKeys - 1));
				if ((written[keyVal]==0) || allowDups)
				{
					ps.println("i"+keyVal);
					written[keyVal]++;
					numWritten++;
				}
			}

			// print the whole tree
			ps.println("p");

			// now delete all keys
			if (doDelete) while (numWritten > 0)
			{
				int keyVal = (int)Math.round(rand.nextDouble() * (numKeys - 1));
				if (written[keyVal]>0) 
				{
					ps.println("d"+keyVal);
					written[keyVal]--;
					numWritten--;   
				}
			}

			// print the whole tree
			ps.println("p");
		} 
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ps.close();
	}
	
	
	public static void main(String[] argvs)
	{
//		createFile("10keysInsertOnlyNoDups.txt",10,false,false);
//		createFile("100keysInsertOnlyNoDups.txt",100,false,false);
//		createFile("1000keysInsertOnlyNoDups.txt",1000,false,false);
//		createFile("10000keysInsertOnlyNoDups.txt",10000,false,false);
//		
//		createFile("10keysInsertDeleteNoDups.txt",10,true,false);
//		createFile("100keysInsertDeleteNoDups.txt",100,true,false);
//		createFile("1000keysInsertDeleteNoDups.txt",1000,true,false);
//		createFile("10000keysInsertDeleteNoDups.txt",10000,true,false);
//		
//		createFile("10keysInsertDeleteDups.txt",10,true,true);
//		createFile("100keysInsertDeleteDups.txt",100,true,true);
//		createFile("1000keysInsertDeleteDups.txt",1000,true,true);
//		createFile("10000keysInsertDeleteDups.txt",10000,true,true);
//		
//		System.exit(0);
		
		try
		{
			BTTest bttest = new BTTest();
			bttest.runTests();
		} catch (Exception e)
		{
			e.printStackTrace();
			System.out.println("Error encountered during BTree tests:\n");
			Runtime.getRuntime().exit(1);
		}
	}	
}