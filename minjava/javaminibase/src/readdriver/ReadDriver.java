package readdriver;


import java.io.*;
import java.util.*;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import diskmgr.PCounter;
import heap.*;
import global.*;
import btree.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;
import iterator.SortFirstSky;
import iterator.BlockNestedLoopsSky;
import iterator.NestedLoopsSky;
import tests.TestDriver;
import btree.BTreeSortedSky;

/** Note that in JAVA, methods can'tuple1 be overridden to be more private.
 Therefore, the declaration of all private functions are now declared
 protected as opposed to the private type in C++.
 */

//watching point: RID rid, some of them may not have to be newed.

public class ReadDriver  extends TestDriver implements  GlobalConst{
    protected String dbpath;
    protected String logpath;

    private static RID   rid;
    private static Heapfile  f = null;
    private static boolean status = OK;
    private static int[] pref_list;
    private static int _n_pages;
    private static int nColumns;
    public static String heapFile = "hFile.in";
    private static AttrType[] attrType;
    private static IndexFile indexFile;
    private static short[] attrSizes;
	  private static short tSize = 34;
    // create an iterator by open a file scan
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);

    private static boolean indexesCreated;
    private String inputFile;

    public ReadDriver(){
        super("main");
    }

    public boolean runTests () {
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        dbpath = "/tmp/main"+System.getProperty("user.name")+".minibase-db";
        logpath = "/tmp/main"+System.getProperty("user.name")+".minibase-log";

        SystemDefs sysdef = new SystemDefs(dbpath,50000, 40000,"Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");

        return _pass;
    }
	private void dataMenu() {
		System.out.println("Default has been set to data2.txt. Enter file name without {.txt}.\n");
		System.out.println("[1]   Read input data2");
    System.out.println("[2]   Read input data3");
    System.out.println("[3]   Read input data_large_skyline");
		System.out.println("[4]	  Read your own input");
		System.out.println("\n[0]  Quit!");
        System.out.print("Hi, Enter your choice :");
	}
	
	private void prefMenu() {
		System.out.println("[1]   Set pref = [1]");
    System.out.println("[2]   Set pref = [1,2]");
    System.out.println("[3]   Set pref = [1,3]");
    System.out.println("[4]   Set pref = [1,3,5]");
    System.out.println("[5]   Set pref = [1,2,3,4,5]");
		System.out.println("[6]	  Set your own preference list of attributes");
		System.out.println("\n[0]  Quit!");
        System.out.print("Hi, Enter your choice :");
	}
    private void pageMenu() {
		System.out.println("[1]   Set n_page = 5");
        System.out.println("[2]   Set n_page = 10");
        System.out.println("[3]   Set n_page = <your_wish>");
		System.out.println("\n[0]  Quit!");
        System.out.print("Hi, Enter your choice :");
	}
	
	private void algoMenu() {
		System.out.println("[1]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[2]  Run Block Nested Loop on data with parameters ");
        System.out.println("[3]  Run Sort First Sky on data with parameters ");
        System.out.println("[4]  Run individual Btree Sky on data with parameters ");
        System.out.println("[5]  Run combined Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Hi, Enter your choice :");
	}
    private void readData(String fileName) throws IOException, InvalidTupleSizeException, InvalidTypeException {

        // Create the heap file object
        try {
            heapFile = Heapfile.getRandomHFName();
            f = new Heapfile(heapFile);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers() ) {
            System.err.println ("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if ( status == OK ) {

            // Read data and construct tuples
            File file = new File(fileName + ".txt");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim());

            attrType = new AttrType[nColumns];
            attrSizes = new short[nColumns];

            for(int i=0; i<attrType.length; i++){
                attrType[i] = new AttrType(AttrType.attrReal);
            }
            for(int i = 0; i< attrSizes.length; i++){
                attrSizes[i] = 32;
            }

            projlist = new FldSpec[nColumns];

            for(int i = 0; i< nColumns; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns,attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: "+size);
			      tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }
            sc.nextLine();
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size

                double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                        .split("\\s+"))
                        .filter(s -> !s.isEmpty())
                        .toArray(String[]::new))
                        .mapToDouble(Double::parseDouble)
                        .toArray();

                for(int i=0; i<doubleArray.length; i++) {
                    try {
                        tuple1.setFloFld(i+1, (float) doubleArray[i]);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    rid = f.insertRecord(tuple1.returnTupleByteArray());
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

            }
            try {
                System.out.println("record count "+f.getRecCnt());
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            } catch (HFDiskMgrException e) {
                e.printStackTrace();
            } catch (HFBufMgrException e) {
                e.printStackTrace();
            }
        }
    }
    protected float floatKey(Tuple tuple1, int[] pref_attr_lst, int pref_attr_lst_len) {
		float sum = 0.0f;
        int j = 0;
		for(int i = 0; i < tuple1.noOfFlds(); i++) {
			if(pref_attr_lst[j] == i && j < pref_attr_lst_len) {
				try {
					sum += tuple1.getFloFld(i-1);
				}
				catch (Exception e){
					status = FAIL;
					e.printStackTrace();
				}
                j++;
			}
		}
		return sum;
	}

    protected String testName () {
        return "Main Driver";
    }

	protected boolean runAllTests (){
		int choice = 1;
		
		while(choice != 0) {
			dataMenu();
			
			try{
				choice = GetStuff.getChoice();
				switch(choice) {
					case 1:
                        System.out.println(System.getProperty("user.dir"));
                        inputFile = "../../data/data2";
                        readData(inputFile);
                        break;

          case 2:
            System.out.println(System.getProperty("user.dir"));
            inputFile = "../../data/data3";
            readData(inputFile);
            break;
          case 3:
            System.out.println(System.getProperty("user.dir"));
            inputFile = "../../data/data_large_skyline";
            readData(inputFile);
            break;

					case 4:
						String dataFile = GetStuff.getReturn();
						System.out.println(System.getProperty("user.dir"));
                        inputFile = "../../data/" + dataFile;
                        readData(inputFile);
						break;
					case 0:
						break;
				}
			if (choice == 0)
				break;
			
			prefMenu();
        indexesCreated = false;
				choice = GetStuff.getChoice();
				switch(choice) {
					case 1:
              pref_list = new int[]{1};
              break;
					case 2:
              pref_list = new int[]{1,2};
              break;
          case 3:
              pref_list = new int[]{1,3};
              break;
          case 4:
              pref_list = new int[]{1,3,5};
              break;
          case 5:
              pref_list = new int[]{1,2,3,4,5};
              break;

					case 6:
						System.out.println("Enter number of preferred attributes: ");
						int prefLen = GetStuff.getChoice();
						pref_list = new int[prefLen];
						for (int i = 0; i< prefLen; i++)
						{	
							System.out.println("Enter preferred attribute index:");
						pref_list[i] = GetStuff.getChoice();
						}
						System.out.println(Arrays.toString(pref_list));
						break;	
					case 0:
						break;
				}
				if (choice == 0)
				break;
			
			pageMenu();
			
				choice = GetStuff.getChoice();
				switch(choice) {
					case 1:
                        _n_pages = 5;
                        break;

                    case 2:
                        _n_pages = 10;
                        break;

                    case 3:
                        System.out.println("Enter n_pages of your choice: ");
                        _n_pages = GetStuff.getChoice();
                        if(_n_pages<=0)
                            break;
                        break;
					case 0:
						break;
				}
				if (choice == 0)
				break;
			
			//choice = GetStuff.getChoice();
			while( choice != 0){
				algoMenu();
				
				
					choice = GetStuff.getChoice();
					switch(choice){
					case 1:
                        // call nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runNestedLoopSky(heapFile);
                        break;

                    case 2:
                        // call block nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBNLSky(heapFile);
                        break;

                    case 3:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runSortFirstSky(heapFile);
                        break;

                    case 4:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBtreeSky();
                        break;

                    case 5:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBTreeSortedSky();
						            break;

                    case 0:
                        break;
					}
				}
				break;
			}
				catch(Exception e) {
                e.printStackTrace();
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("       !!         Something is wrong                    !!");
                System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

				}
			}
			
		
		return true;
	}

    public static void runNestedLoopSky(String hf) throws PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanNested = initialiseFileScan(hf);

        NestedLoopsSky nested = null;
        try {
            nested = new NestedLoopsSky(attrType, attrType.length, attrSizes, fscanNested, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(nested);

        try {
            nested.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runBNLSky(String hf) throws PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanBlock = initialiseFileScan(hf);

        Iterator block = null;
        try {
            block = new BlockNestedLoopsSky(attrType, attrType.length, attrSizes, fscanBlock, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(block);

        try {
            block.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runSortFirstSky(String hf) throws PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscan = initialiseFileScan(hf);

        Iterator sort = null;
        try {
            sort = new SortFirstSky(attrType, attrType.length, attrSizes, fscan, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(sort);

        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    private void runBtreeSky() throws Exception {
        System.out.println("Running BTreeSky");
        System.out.println("DataFile: " + inputFile);
        System.out.println("Preference list: " + Arrays.toString(pref_list));
        System.out.println("Number of pages: " + _n_pages);
        System.out.println("Pref list length: " + pref_list.length);
        if (!indexesCreated) {
            BTreeUtil.createBtreesForPrefList(heapFile, f, attrType, attrSizes, pref_list);
            indexesCreated = true;
        }

        // autobox to IndexFile type
        IndexFile[] index_file_list = BTreeUtil.getBTrees(pref_list);
        SystemDefs.JavabaseBM.flushPages();

        BTreeSky btreesky = new BTreeSky(attrType, nColumns, attrSizes, null, heapFile, pref_list,
                pref_list.length, index_file_list, _n_pages);

        PCounter.initialize();
        btreesky.findBTreeSky();

        System.out.println("BTreeSky Complete\n");
    }

    public void runBTreeSortedSky() {
        try {
            BTreeCombinedIndex obj = new BTreeCombinedIndex();
            IndexFile indexFile = obj.combinedIndex(heapFile, attrType, attrSizes, pref_list, pref_list.length);
            System.out.println("Index created!");
            SystemDefs.JavabaseBM.flushPages();

            System.out.println("CombinedBTreeIndex scanning");
            String fileName = BTreeCombinedIndex.random_string1;

            BTreeSortedSky btree = new BTreeSortedSky(attrType, attrType.length, attrSizes, null, fileName, pref_list, pref_list.length, indexFile, _n_pages);
            PCounter.initialize();
            btree.computeSkyline();

            System.out.println("BTreeSortSky Complete");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getNextAndPrintAllSkyLine(Iterator iter) {
// this needs to be before the fn call since call to any algo 1,2,3 from 4,5 reinitializes counter
//        PCounter.initialize();

        int count = -1;
        Tuple tuple1 = null;

        System.out.println("\n -- Skyline Objects -- ");
        do {
            try {
                if (tuple1 != null) {
                    tuple1.print(attrType);
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count++;

            try {
                tuple1 = iter.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        } while (tuple1 != null);

        System.out.println("\nRead statistics "+PCounter.rcounter);
        System.out.println("Write statistics "+PCounter.wcounter);

        System.out.println("\nNumber of Skyline objects: " + count+"\n");
    }

    public static FileScan initialiseFileScan(String hf) throws PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscan = null;

        try {
            fscan = new FileScan(hf, attrType, attrSizes, (short) attrType.length, attrType.length, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return fscan;
    }

    public static void main(String [] args) {

        try{
            ReadDriver rd = new ReadDriver();
            rd.runTests();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }finally {

        }
    }

}


/**
 * To get the integer off the command line
 */
class GetStuff {
    GetStuff() {}

    public static int getChoice () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;

        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            return -1;
        }
        catch (IOException e) {
            return -1;
        }

        return choice;
    }

    public static String getReturn () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
		String ret = "data2";
        try {
            ret = in.readLine();
        }
        catch (IOException e) {}
		
		return ret;
    }
}
