package queryinterface;

import btree.ConstructPageException;
import btree.FloatKey;
import btree.IntegerKey;
import btree.StringKey;
import btree.*;
import bufmgr.*;
import diskmgr.PCounter;
import global.*;
import hash.*;
import heap.*;
import index.IndexException;
import iterator.*;
import iterator.Iterator;
import tests.TestDriver;

import java.io.*;
import java.util.*;

public class QueryInterface extends TestDriver implements GlobalConst {

    protected String dbpath;
    protected String logpath;
    protected String dbName;

    private static RID rid;
    private static int _n_pages;
    private static Heapfile f = null;
    private static String METAFILE_POSTFIX = "-meta";
    private static BTreeFile bTreeUnclusteredFile = null;
    private static BTreeClusteredFile bTreeClusteredFile = null;
    private static ClusteredHashFile hashFile = null;
    private static UnclusteredHashFile unclusteredHashFile = null;
    private static Heapfile indexCatalogFile = null;
    private static boolean status = OK;
    private static int nColumns;
    private static AttrType[] attrType;
    private static short[] attrSizes;
    private static AttrType[] attrType2;
    private static short[] attrSizes2;
    private static int nColumns2;
    private static String[] attrNames;
    private static String[] attrNames2;
    private static AttrType[] indexAttrTypes;
    private static short[] indexAttrSizes;
    private static AttrType[] metaAttrTypes;
    private static short[] metaAttrSizes;
    private static Tuple indexTuple;
    private static Tuple metaTuple;
    private static short tSize = 34;
    private static short attrStringSize = 32;
    private static int[] pref_list;
    private static FldSpec[] projlist;
    private static FldSpec[] projlist2;
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private SystemDefs sysDef;
    private static boolean indexesCreated;
    private boolean dbClosed = true;
    public static final short CLUSTERED_HASH = 0;
    public static final short CLUSTERED_BTREE = 1;
    public static final short UNCLUSTERED_HASH = 2;
    public static final short UNCLUSTERED_BTREE = 3;
    public static final short NO_INDEX = 4;
    public static final String INDEX_FILE_NAME = "indexCatalog";
    public static final short EQUAL=0;
    public static final short LESS=1;
    public static final short GREATER=2;
    public static final short LESSOREQUAL=3;
    public static final short GREATEROREQUAL=4;
    public static String outputTableName = null;
    public static Boolean outputResultToTable = false;
    public static Heapfile outputTable = null;
    public static ArrayList<BTreeClusteredFile.RidChange> RidChanges = null;
    public static ArrayList<RidTuplePair> ridTuplePairs = null;
    public static String[] oAttrName;
    public static AttrType[] oAttrTypes;
    public static short[] oAttrSize;

    private void menuInterface() {
        System.out.println("-------------------------- MENU --------------------------");
        System.out.println("[1]   Open database");
        System.out.println("[2]   Close current database");
        System.out.println("[3]   Create a new table");
        System.out.println("[4]   Create index");
        System.out.println("[5]   Insert data from file");
        System.out.println("[6]   Delete data from file");
        System.out.println("[7]   Output all tuples in a table");
        System.out.println("[8]   Output all keys stored in Index for a given attribute");
        System.out.println("[9]   Run SKYLINE operators");
        System.out.println("[10]  Run GROUPBY operators");
        System.out.println("[11]  Run JOIN operators");
        System.out.println("[12]  Run TOPKJOIN operators");
        System.out.println("[13]  Destroy Database");
        System.out.println("[14]  Set n_page = 5");
        System.out.println("[15]  Set n_page = 10");
        System.out.println("[16]  Set n_page = <your_wish>");
        System.out.println("\n[0]  Quit");
        System.out.print("Enter your choice :");
    }

    /**
     * QueryInterface Constructor
     */
    public QueryInterface() {
        super("main");
    }

    public boolean runTests() {
        initMetaAndIndexAttrs();
        boolean _pass = runAllTests();

        //Clean up again
        //cleanDB();
        if (!dbClosed) {
            close_database();
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completed successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    protected String testName() {
        return "Query Interface Driver";
    }

    private void initMetaAndIndexAttrs() {
        // For Index
        short size;
        indexTuple = new Tuple();
        indexAttrTypes = new AttrType[4];

        indexAttrTypes[0] = new AttrType(AttrType.attrString); // For relName
        indexAttrTypes[1] = new AttrType(AttrType.attrString); // For attrName
        indexAttrTypes[2] = new AttrType(AttrType.attrInteger);// For indexType
        indexAttrTypes[3] = new AttrType(AttrType.attrInteger);// For keyIndex

        indexAttrSizes = new short[2];
        indexAttrSizes[0] = (short) attrStringSize;
        indexAttrSizes[1] = (short) attrStringSize;

        try {
            indexTuple.setHdr((short) 4, indexAttrTypes, indexAttrSizes);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to set tuple header!");
        }

        size = indexTuple.size();

        indexTuple = new Tuple(size);
        try {
            indexTuple.setHdr((short) 4, indexAttrTypes, indexAttrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        //For Attrs
        metaTuple = new Tuple();
        metaAttrTypes = new AttrType[3];

        metaAttrTypes[0] = new AttrType(AttrType.attrString); // For attrName
        metaAttrTypes[1] = new AttrType(AttrType.attrInteger);// For indexType
        metaAttrTypes[2] = new AttrType(AttrType.attrInteger);// For attrLen

        metaAttrSizes = new short[1];
        metaAttrSizes[0] = (short) attrStringSize;

        try {
            metaTuple.setHdr((short) 3, metaAttrTypes, metaAttrSizes);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to set tuple header!");

        }

        size = metaTuple.size();

        metaTuple = new Tuple(size);
        try {
            metaTuple.setHdr((short) 3, metaAttrTypes, metaAttrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }
    }

    protected boolean runAllTests() {
        int choice = 1;
        String tname, fname, dbname;
        String[] tokens;
        System.out.println();
        while (choice != 0) {
            menuInterface();
            status = OK;
            try {
                choice = GetStuff.getChoice();
                if (choice != 1 && dbClosed && choice != 0) {
                    System.out.println("Please open a database first!");

                } else {
                    switch (choice) {

                        case 1:
                            dbClosed = false;
                            System.out.print("Enter Database Name: ");
                            dbname = GetStuff.getStringChoice();
                            System.out.println();
                            open_database(dbname);
                            break;

                        case 2:
                            SystemDefs.JavabaseBM.flushPages();
                            close_database();
                            break;

                        case 3:
                            createTableMenu();
                            break;

                        case 4:
                            createIndexMenu();
                            break;

                        case 5:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Tablename Filename: ");
                            tokens = GetStuff.getStringChoice().split(" ");
                            System.out.println();
                            insert_data(tokens[0], tokens[1]);
                            break;

                        case 6:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Tablename Filename: ");
                            tokens = GetStuff.getStringChoice().split(" ");
                            System.out.println();
                            delete_data(tokens[0], tokens[1]);
                            break;

                        case 7:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Tablename: ");
                            tname = GetStuff.getStringChoice();
                            System.out.println();
                            printTable(tname);
                            break;

                        case 8:
                            SystemDefs.JavabaseBM.flushPages();
                            System.out.print("Enter Tablename ATT_NO: ");
                            tokens = GetStuff.getStringChoice().split(" ");
                            System.out.println();
                            int attrIndex = Integer.parseInt(tokens[1]);
                            printIndexKeys(tokens[0], attrIndex);
                            break;

                        case 9:
                            SystemDefs.JavabaseBM.flushPages();
                            skylineMenu();
                            break;
                        case 10:

                            // GROUPBY SORT MAX 2 3 lol 50
                            try {
                                SystemDefs.JavabaseBM.flushPages();
                            } catch (PageNotFoundException | BufMgrException | HashOperationException | PagePinnedException e) {
                                e.printStackTrace();
                            }

                            System.out.println("Enter Query:");
                            tokens = GetStuff.getStringChoice().split(" ");
                            String method = String.valueOf(tokens[1]);
                            String jAttr1 = String.valueOf(tokens[2]);
                            int jAttr2 = Integer.parseInt(tokens[3]);
                            String mAttr1 = String.valueOf(tokens[4]);
                            String tableName = String.valueOf(tokens[5]);
                            _n_pages = Integer.parseInt(tokens[6]);

                            AggType aggType;
                            if (jAttr1.equals("MAX")) {
                                aggType = new AggType(AggType.MAX);
                            } else if (jAttr1.equals("MIN")) {
                                aggType = new AggType(AggType.MIN);
                            } else if (jAttr1.equals("AVG")) {
                                aggType = new AggType(AggType.AVG);
                            } else {
                                aggType = new AggType(AggType.SKYLINE);
                            }

                            String[] atts = mAttr1.split(",");
                            FldSpec[] aggList = new FldSpec[atts.length];
                            int count = 0;
                            for (String att: atts) {
                                aggList[count] = new FldSpec(new RelSpec(RelSpec.outer), Integer.parseInt(att));
                                count++;
                            }

                            getTableAttrsAndType(tableName);
                            CustomScan scan = new CustomScan(tableName);

                            if (method.equals("SORT")) {
                                GroupByWithSort groupBy = new GroupByWithSort(attrType,nColumns, attrSizes, scan, new FldSpec(new RelSpec(RelSpec.outer), jAttr2),
                                        aggList, aggType, projlist, 0, _n_pages);
                                PCounter.initialize();
                                Tuple tup  = groupBy.get_next();
                                while (tup != null) {
                                    tup.print();
                                    tup = groupBy.get_next();
                                }
                                groupBy.close();

                                PCounter.printStats();
                            } else {
                                GroupByWithHash groupBy = new GroupByWithHash(attrType,nColumns, attrSizes, scan, new FldSpec(new RelSpec(RelSpec.outer), jAttr2),
                                        aggList, aggType, projlist, 0, _n_pages);
                                PCounter.initialize();
                                Tuple tup  = groupBy.get_next();
                                while (tup != null) {
                                    tup.print();
                                    tup = groupBy.get_next();
                                }
                                groupBy.close();

                                PCounter.printStats();
                            }


                            break;
                        case 11:
                            hashJoinTest();
//                            System.out.println("Enter your choice:\n[1] Hash Join\n [2] Index Join \n[3] NLJ [4] SMJ");
//                            int c = GetStuff.getChoice();
//                            if (c==1) {
//                                hashJoinTest();
//                            }else if(c==2){
//                                indexJoin();
//                            }else if(c==3){
//                                nlj();
//                            }else if(c==4){
//                                smj();
//                            }
                            break;

                        case 12:
                            System.out.println("Enter your choice:\n[1] Hash-based Top-K Join\n[2] NRA-based Top-K Join");
                            int ch = GetStuff.getChoice();

                            // Hash based top k join
                            if(ch == 1){
                                // Sample Queries
//                                create_table CLUSTERED HASH 2 r_sii2000_10_10_10_dup
//                                create_table CLUSTERED BTREE 2 r_sii2000_10_75_200
//                                TOPKJOIN HASH 11 r_sii2000_10_10_10_dup 2 3 r_sii2000_10_75_200 2 3 5 mater j1
//                                TOPKJOIN HASH 8 r_sii2000_10_10_10_dup 2 3 r_sii2000_10_75_200 2 3 5

                                performTopKHashJoin();

                            }else if(ch == 2){
                                System.out.println("Enter Query:");
                                tokens = GetStuff.getStringChoice().split(" ");
                                int jAttr10 = Integer.valueOf(tokens[4]), jAttr11 = Integer.valueOf(tokens[7]),
                                        mAttr10 = Integer.valueOf(tokens[5]), mAttr2 = Integer.valueOf(tokens[8]);
                                String fileName1 = tokens[3], fileName2 = tokens[6];
                                int k = Integer.valueOf(tokens[2]);
                                int n_pages = Integer.valueOf(tokens[9]);

                                // createTable(fileName1, true, (short) 5, mAttr10);
                                // createTable(fileName2, true, (short) 5, mAttr2);

                                getTableAttrsAndType(fileName1);
                                getSecondTableAttrsAndType(fileName2);

                                createAttrDef(mAttr10);

                                String oTable = "null";
                                if (tokens.length > 10) {
                                    oTable = tokens[11];
                                    createOutputTable(oTable, fileName1, fileName2, jAttr10, mAttr10, mAttr2);
                                    System.out.println("Output Table Created");
                                }

                                // printTable(fileName1);
                                // printTable(fileName2);

                                FldSpec[] joinList = new FldSpec[2];
                                FldSpec[] mergeList = new FldSpec[2];

                                joinList[0] = new FldSpec(rel, jAttr10);
                                joinList[1] = new FldSpec(rel, jAttr11);
                                mergeList[0] = new FldSpec(rel, mAttr10);
                                mergeList[1] = new FldSpec(rel, mAttr2);

                                TopK_NRAJoin topK_NRAJoin = new TopK_NRAJoin(attrType, attrType.length, attrSizes, joinList[0], mergeList[0],
                                        attrType2, attrType2.length, attrSizes2, joinList[1], mergeList[1], fileName1, fileName2, k, n_pages, oTable);

                                PCounter.initialize();
                                try {
                                    SystemDefs.JavabaseBM.flushPages();
                                } catch (PageNotFoundException | BufMgrException | HashOperationException | PagePinnedException e) {
                                    e.printStackTrace();
                                }

                                topK_NRAJoin.computeTopK_NRA(oAttrTypes, oAttrSize, oAttrName);

                                System.out.println("\nRead statistics " + PCounter.rcounter);
                                System.out.println("Write statistics " + PCounter.wcounter);
                                System.out.println();
                            }
                            break;

                        case 13:
                            System.out.print("Enter Database Name: ");
                            dbName = GetStuff.getStringChoice();
                            System.out.println();
                            dbpath = "/tmp/" + dbName + ".minibase-db";
                            logpath = "/tmp/" + dbName + ".minibase-log";
                            cleanDB();
                            break;

                        case 14:
                            _n_pages = 5;
                            break;

                        case 15:
                            _n_pages = 10;
                            break;

                        case 16:
                            System.out.println("Enter n_pages of your choice: ");
                            _n_pages = GetStuff.getChoice();
                            if (_n_pages <= 0)
                                break;
                            break;

                        case 0:
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("       !!               Something is wrong              !!");
                System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            }
        }
        return true;
    }

    private void performTopKHashJoin() throws IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException,
            UnknowAttrType, AddFileEntryException, InvalidSlotNumberException, WrongPermat, JoinsException,
            UnpinPageException, PinPageException, TupleUtilsException, InvalidRelation, GetFileEntryException,
            PageNotReadException, FileScanException, KeyNotMatchException, InvalidTypeException,
            ConstructPageException, hash.ConstructPageException, PredEvalException, IteratorException {

        System.out.println("Enter Query:");
        String[] tokens = GetStuff.getStringChoice().split(" ");
        int jAttr1 = Integer.parseInt(tokens[4]);
        int jAttr2 = Integer.parseInt(tokens[7]);
        int mAttr1 = Integer.parseInt(tokens[5]);
        int mAttr2 = Integer.parseInt(tokens[8]);
        String fileName1 = tokens[3];
        String fileName2 = tokens[6];
        int k = Integer.parseInt(tokens[2]);
        int n_pages = Integer.parseInt(tokens[9]);

//        createTable(fileName1, false, NO_INDEX, 0);
//        createTable(fileName2, false, NO_INDEX, 0);
//        CustomScan outerCS = new CustomScan(fileName1);
//        CustomScan innerCS = new CustomScan(fileName2);

//        String innerHF = createTempHeapFileForSkyline(fileName1);
//        String outerHF = createTempHeapFileForSkyline(fileName2);
//        setAttrDesc(innerHF);
//        setAttrDesc(outerHF);
        getTableAttrsAndType(fileName1);
        getSecondTableAttrsAndType(fileName2);

        String oTable = null;
        Heapfile outFile = null;
        if(tokens.length > 10){
            oTable = tokens[11];

//                outFile = new Heapfile(oTable);
//                outFile.deleteFile();
//                outFile = new Heapfile(oTable);
//                setTableMeta(outFile, oAttrTypes, oAttrSize, oAttrName);
            createTypeDefForHashTopK();
            createOutputTable(oTable, "","",0,0,0);

        }

        FldSpec[] joinList = new FldSpec[2];
        FldSpec[] mergeList = new FldSpec[2];

        joinList[0] = new FldSpec(rel, jAttr1);
        joinList[1] = new FldSpec(rel, jAttr2);
        mergeList[0] = new FldSpec(rel, mAttr1);
        mergeList[1] = new FldSpec(rel, mAttr2);

        TopK_HashJoin topK_hashJoin = new TopK_HashJoin(attrType, attrType.length, attrSizes, joinList[0], mergeList[0],
                attrType2, attrType2.length, attrSizes2, joinList[1], mergeList[1], fileName1, fileName2, k, n_pages, oTable);

        try {
//            outerCS.close();
//            innerCS.close();
            SystemDefs.JavabaseBM.flushPages();
        } catch (PageNotFoundException e) {
            e.printStackTrace();
        } catch (PagePinnedException e) {
            e.printStackTrace();
        } catch (HashOperationException e) {
            e.printStackTrace();
        } catch (BufMgrException e) {
            e.printStackTrace();
        }
        // initialize after flushing pages to disk
        PCounter.initialize();

        try {
//                Tuple t = new Tuple();
            java.util.Iterator it = topK_hashJoin.get_next();

            while((it.hasNext())){
                Tuple tuple = (Tuple) it.next();
//                RID rid = new RID();
//                rid = outFile.insertRecord(tuple.returnTupleByteArray());
                tuple.print(topK_hashJoin.getOutputAttrType());
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.out.println("\nRead statistics "+PCounter.rcounter);
        System.out.println("Write statistics "+PCounter.wcounter);

        try {
            SystemDefs.JavabaseBM.flushPages();
        } catch (PageNotFoundException | BufMgrException | HashOperationException | PagePinnedException e) {
            e.printStackTrace();
        }
//        Heapfile hf;
//        try {
//            hf = new Heapfile(innerHF);
//            hf.deleteFile();
//            hf = new Heapfile(outerHF);
//            hf.deleteFile();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        PCounter.initialize();

        System.out.println("\n\n------------------- Hash based Top K Join completed ---------------------\n\n");
    }

    private void createTypeDefForHashTopK() {
        int len1 = attrType.length, len2 = attrType2.length;
        oAttrTypes = new AttrType[len1 + len2 + 1];
        System.arraycopy(attrType, 0, oAttrTypes, 0, len1);
        System.arraycopy(attrType2, 0, oAttrTypes, len1, len2);

        oAttrTypes[len1+len2] = new AttrType(AttrType.attrReal);

        oAttrSize = new short[attrSizes.length + attrSizes2.length];
        int j = 0;
        for(int i = 0; i < oAttrTypes.length; i++){
            if(oAttrTypes[i].attrType == AttrType.attrString)
                oAttrSize[j++] = (short) 32;
        }

        oAttrName = new String[len1 + len2 + 1];
        System.arraycopy(attrNames, 0, oAttrName, 0, len1);
        System.arraycopy(attrNames2, 0, oAttrName, len1, len2);

        oAttrName[len1+len2] = "mergeAttr";
    }

    private void skylineMenu() {
        try {
            //skyline NLS 2,3 r_sii2000_10_10_10_dup 10 MATER output
            System.out.print("Enter tablename or query: ");
            String[] tokens = GetStuff.getStringChoice().split(" ");
            String tname;
            if (tokens.length > 1) {
                tname = tokens[3];
                getTableAttrsAndType(tname);
                String pref[] = tokens[2].split(",");
                pref_list = new int[pref.length];
                for (int i = 0; i < pref.length; i++) {
                    pref_list[i] = Integer.parseInt(pref[i].trim());
                }
                String skylineType = tokens[1];
                _n_pages = Integer.parseInt(tokens[4]);

                if (tokens[5].equals("MATER")) {
                    outputTableName = tokens[6];
                    outputResultToTable = true;
                    outputTable = new Heapfile(outputTableName);
                    outputTable.deleteFile();
                    setTableMeta(outputTableName, attrType, attrSizes, attrNames);
                } else {
                    outputTableName = null;
                    outputResultToTable = false;
                }
                String tempRelName = createTempHeapFileForSkyline(tname);
                f = new Heapfile(tempRelName);
                if (skylineType.equals("NLS")) {
                    // call nested loop sky
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    runNestedLoopSky(tempRelName, outputResultToTable, outputTableName);
                } else if (skylineType.equals("BNLS")) {
                    // call block nested loop sky
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    runBNLSky(tempRelName, outputResultToTable, outputTableName);
                } else if (skylineType.equals("SFS")) {
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    runSortFirstSky(tempRelName, outputResultToTable, outputTableName);
                } else if (skylineType.equals("BTS")) {
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    runBtreeSky(tempRelName, outputResultToTable, outputTableName);
                } else if (skylineType.equals("BTSS")) {
                    // call block nested loop sky
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    runBTreeSortedSky(tempRelName, outputResultToTable, outputTableName);
                }

                f.deleteFile();
                return;
            }
            tname = tokens[0];
            getTableAttrsAndType(tname);
            String tempRelName = createTempHeapFileForSkyline(tname);
            f = new Heapfile(tempRelName);
            System.out.println();
            prefMenu();
            indexesCreated = false;
            int choice = GetStuff.getChoice();
            switch (choice) {
                case 1:
                    pref_list = new int[]{1};
                    break;
                case 2:
                    pref_list = new int[]{1, 2};
                    break;
                case 3:
                    pref_list = new int[]{1, 3};
                    break;
                case 4:
                    pref_list = new int[]{1, 3, 5};
                    break;
                case 5:
                    pref_list = new int[]{1, 2, 3, 4, 5};
                    break;

                case 6:
                    System.out.println("Enter number of preferred attributes: ");
                    int prefLen = GetStuff.getChoice();
                    pref_list = new int[prefLen];
                    for (int i = 0; i < prefLen; i++) {
                        System.out.println("Enter preferred attribute index:");
                        pref_list[i] = GetStuff.getChoice();
                    }
                    System.out.println(Arrays.toString(pref_list));
                    break;
                case 0:
                    break;
            }

            if (choice == 0)
                return;

            pageMenu();

            choice = GetStuff.getChoice();
            switch (choice) {
                case 1:
                    _n_pages = 5;
                    break;

                case 2:
                    _n_pages = 10;
                    break;

                case 3:
                    System.out.println("Enter n_pages of your choice: ");
                    _n_pages = GetStuff.getChoice();
                    if (_n_pages <= 0)
                        break;
                    break;
                case 0:
                    break;
            }
            if (choice == 0)
                return;

            //choice = GetStuff.getChoice();
            while (choice != 0) {
                algoMenu();
                choice = GetStuff.getChoice();

                if (choice != 0) {
                    outputTableMenu();
                }

                switch (choice) {
                    case 1:
                        // call nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runNestedLoopSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 2:
                        // call block nested loop sky
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBNLSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 3:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runSortFirstSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 4:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBtreeSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 5:
                        SystemDefs.JavabaseBM.flushPages();
                        PCounter.initialize();
                        runBTreeSortedSky(tempRelName, outputResultToTable, outputTableName);
                        break;

                    case 0:
                        f.deleteFile();
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println("       !!         Something is wrong                    !!");
            System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
            System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
    }

    private void outputTableMenu() {
        System.out.println("Do you want to store result in an output table");
        System.out.println("[1] YES");
        System.out.println("[2] NO");
        System.out.print("Enter your choice:");
        int yourChoice = GetStuff.getChoice();
        outputResultToTable = false;
        if (yourChoice == 1) {
            outputResultToTable = true;
            System.out.print("Enter Tablename: ");
            outputTableName = GetStuff.getStringChoice();
            try {
                outputTable = new Heapfile(outputTableName);
                outputTable.deleteFile();
                setTableMeta(outputTableName, attrType, attrSizes, attrNames);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            outputResultToTable = false;
            outputTableName = null;
        }
    }

    private void prefMenu() {
        System.out.println("[1]   Set pref = [1]");
        System.out.println("[2]   Set pref = [1,2]");
        System.out.println("[3]   Set pref = [1,3]");
        System.out.println("[4]   Set pref = [1,3,5]");
        System.out.println("[5]   Set pref = [1,2,3,4,5]");
        System.out.println("[6]   Set your own preference list of attributes");
        System.out.println("\n[0]  Quit!");
        System.out.print("Enter your choice :");
    }

    private void createTableMenu() throws IOException, InvalidTupleSizeException, ConstructPageException, GetFileEntryException, AddFileEntryException, FieldNumberOutOfBoundException {
        System.out.print("Enter create table query: ");
//        create_table CLUSTERED BTREE 2 r_sii2000_1_75_200
        int attrInd = -1;
        String[] tokens = GetStuff.getStringChoice().split(" ");
        String fname;
        if (tokens.length > 1) {
            if (tokens[1].equals("CLUSTERED")) {
            fname = tokens[4];
            attrInd = Integer.parseInt(tokens[3]);
            if (tokens[2].equals("BTREE")) {
                createTable(fname, true, CLUSTERED_BTREE, attrInd);
            } else if (tokens[2].equals("HASH")) {
                createTable(fname, true, CLUSTERED_HASH, attrInd);
            } else if (tokens[2].equals("BTREE_NRA"))
                createTable(fname, true, (short) 5, attrInd);
            } else {
                fname = tokens[1];
                createTable(fname, false, NO_INDEX, attrInd);
            }
            return;
        }
        fname = tokens[0];
        System.out.println("[1]   Create Clustered BT Index");
        System.out.println("[2]   Create Clustered Hash Index");
        System.out.println("[3]   Do not create index");
        System.out.println("[4]   Create Clustered BT Index in reverse order");
        System.out.print("Please enter your choice: ");
        int choice = GetStuff.getChoice();
        if (choice == 1 || choice == 2 || choice == 4) {
            System.out.print("Please enter attribute index :");
            attrInd = GetStuff.getChoice();
        }
        try {
            switch (choice) {
                case 1:
                    createTable(fname, true, CLUSTERED_BTREE, attrInd);
                    break;
                case 2:
                    createTable(fname, true, CLUSTERED_HASH, attrInd);
                    break;
                case 3:
                    createTable(fname, false, NO_INDEX, attrInd);
                    break;
                case 4:
                    createTable(fname, true, (short) 5, attrInd);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createIndexMenu() throws IOException, InvalidTupleSizeException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        System.out.print("Enter index query or table name: ");
        int attrInd = -1;
        String[] tokens = GetStuff.getStringChoice().split(" ");
        String tname;
        if (tokens.length > 1) {
            tname = tokens[3];
            attrInd = Integer.parseInt(tokens[2]);

            try {
                if (tokens[1].equals("BTREE")) {
                    createIndex(tname, UNCLUSTERED_BTREE, attrInd);
                } else {
                    createIndex(tname, UNCLUSTERED_HASH, attrInd);
                }
            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace();
            }
            return;
        }
        tname = tokens[0];
        System.out.println("[1]   Create Unclustered BT Index");
        System.out.println("[2]   Create Unclustered Hash Index");
        System.out.print("Please enter your choice:");

        int choice = GetStuff.getChoice();
        if (choice == 1 || choice == 2) {
            System.out.print("Please enter attribute index :");
            attrInd = GetStuff.getChoice();
        }

        try {
            switch (choice) {
                case 1:
                    createIndex(tname, UNCLUSTERED_BTREE, attrInd);
                    break;
                case 2:
                    createIndex(tname, UNCLUSTERED_HASH, attrInd);
                    break;
            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }
    }

    private void pageMenu() {
        System.out.println("[1]   Set n_page = 5");
        System.out.println("[2]   Set n_page = 10");
        System.out.println("[3]   Set n_page = <your_wish>");
        System.out.println("\n[0]  Quit!");
        System.out.print("Enter your choice :");
    }

    private void algoMenu() {
        System.out.println("[1]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[2]  Run Block Nested Loop on data with parameters ");
        System.out.println("[3]  Run Sort First Sky on data with parameters ");
        System.out.println("[4]  Run individual Btree Sky on data with parameters ");
        System.out.println("[5]  Run combined Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Enter your choice :");
    }

    public void cleanDB() {
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
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        System.out.println("Successfully deleted database " + dbName.toUpperCase());
    }

    private void close_database() {
        try {
            SystemDefs.JavabaseBM.flushPages();
            sysDef.JavabaseDB.closeDB();
            dbClosed = true;
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not close the database\n");
            e.printStackTrace();
        }

        System.out.println("Successfully closed database " + dbName.toUpperCase());
    }

    public void open_database(String nameRoot) {
        dbName = nameRoot;
//        dbpath = "/tmp/" + nameRoot + ".minibase-db";
//        logpath = "/tmp/" + nameRoot + ".minibase-log";
        // dbpath = "/Users/musabafzal/Desktop/cse510dbmsi/minjava/javaminibase/" + nameRoot + ".minibase-db";
        // logpath = "/Users/musabafzal/Desktop/cse510dbmsi/minjava/javaminibase/" + nameRoot + ".minibase-log";
        dbpath = "..\\" + nameRoot + ".minibase-db";
        logpath = "..\\" + nameRoot + ".minibase-log";
        File f = new File(dbpath);

        if (f.exists() && !f.isDirectory()) {
            sysDef = new SystemDefs(dbpath, 0, 40000, "Clock");
            System.out.println("Successfully opened database " + dbName.toUpperCase());
        } else {
            sysDef = new SystemDefs(
                    dbpath, 50000, 40000, "Clock");
            System.out.println("Successfully created database " + dbName.toUpperCase());
        }

    }

    private void createOutputTable(String fileName, String fileName1, String fileName2, int joinAttr1, int mergeAttr1, int mergeAttr2) throws IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException {

        if (status == OK) {

            // Read data and construct tuples
            // getTableAttrsAndType(fileName1);
            // getSecondTableAttrsAndType(fileName2);

            try {
                f = new Heapfile(fileName);
                f.deleteFile();
                f = new Heapfile(fileName);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            setTableMeta(fileName, oAttrTypes, oAttrSize, oAttrName);
        }
    }

    private void createAttrDef(int mergeAttr1){
        int len1 = attrType.length, len2 = attrType2.length;
        oAttrTypes = new AttrType[len1 + len2 + 2];
        System.arraycopy(attrType, 0, oAttrTypes, 0, len1);
        System.arraycopy(attrType2, 0, oAttrTypes, len1, len2);
        if(attrType[mergeAttr1 - 1].attrType == AttrType.attrInteger){
            oAttrTypes[len1+len2] = new AttrType(AttrType.attrInteger);
            oAttrTypes[len1+len2+1] = new AttrType(AttrType.attrInteger);
        }else{
            oAttrTypes[len1+len2] = new AttrType(AttrType.attrReal);
            oAttrTypes[len1+len2+1] = new AttrType(AttrType.attrReal);
        }

        oAttrSize = new short[attrSizes.length + attrSizes2.length];
        int j = 0;
        for(int i = 0; i < oAttrTypes.length; i++){
            if(oAttrTypes[i].attrType == AttrType.attrString)
                oAttrSize[j++] = (short) 32;
        }

        oAttrName = new String[len1 + len2 + 2];
        System.arraycopy(attrNames, 0, oAttrName, 0, len1);
        System.arraycopy(attrNames2, 0, oAttrName, len1, len2);
        
        oAttrName[len1+len2] = "LB";
        oAttrName[len1+len2+1] = "UB";
    }

    private void createTable(String fileName, Boolean createIndex, short clusteredIndexType, int attrIndex) throws IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException {

//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;fileName
//        }

        if (status == OK) {

            // Read data and construct tuples
            setAttrDesc(fileName);
            File file = new File("../../data/" + fileName + ".csv");
//            File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + fileName + ".csv");

            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < nColumns; i++) {
                sc.nextLine();
            }

            try {
                if (createIndex) {
                    short keySize = 4;
                    if (attrType[attrIndex - 1].attrType == AttrType.attrString) {
                        keySize = attrStringSize;
                    }
                    if (clusteredIndexType == CLUSTERED_BTREE || clusteredIndexType == 5) {
                        bTreeClusteredFile = new BTreeClusteredFile(fileName, attrType[attrIndex - 1].toInt(), keySize, attrIndex, 0, (short) nColumns, attrType, attrSizes);
                    } else {
                        hashFile = new ClusteredHashFile(fileName, 75, attrType[attrIndex - 1].toInt(), keySize, (short) nColumns, attrType, attrSizes);
                    }
                } else {
                    f = new Heapfile(fileName);
                }
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not create file\n");
                e.printStackTrace();
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            int count = 0;
            ClusteredHashRecord rec;
            PCounter.initialize();
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");

                for (int i = 0; i < row.length; i++) {
                    try {
                        // System.out.println(attrType[i].toInt());
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            int value;
                            if (clusteredIndexType == 5 && i == attrIndex - 1)
                                value = -Integer.parseInt(row[i]);
                            else
                                value = Integer.parseInt(row[i]);
                            tuple1.setIntFld(i + 1, value);
                        } else if (attrType[i].toInt().equals(AttrType.attrReal)) {
                            float value;
                            if (clusteredIndexType == 5 && i == attrIndex - 1)
                                value = -Float.parseFloat(row[i]);
                            else
                                value = Float.parseFloat(row[i]);
                            tuple1.setFloFld(i + 1, value);
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    if (createIndex) {
                        if (clusteredIndexType == CLUSTERED_BTREE || clusteredIndexType == 5) {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
                                bTreeClusteredFile.insert(key, tuple1);
                            } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                                FloatKey key = new FloatKey(tuple1.getFloFld(attrIndex));
                                bTreeClusteredFile.insert(key, tuple1);
                            } else {
                                StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
                                bTreeClusteredFile.insert(key, tuple1);
                            }

                        } else {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                                rec = new ClusteredHashRecord(key, tuple1);
                                hashFile.insertRecord(key, rec);
                            } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                                hash.FloatKey key = new hash.FloatKey(tuple1.getFloFld(attrIndex));
                                rec = new ClusteredHashRecord(key, tuple1);
                                hashFile.insertRecord(key, rec);
                            } else {
                                hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                                rec = new ClusteredHashRecord(key, tuple1);
                                hashFile.insertRecord(key, rec);
                            }

                        }

                    } else {
                        rid = f.insertRecord(tuple1.returnTupleByteArray());
                    }
                    count++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                if (createIndex) {
                    addIndexToCatalog(fileName, attrNames[attrIndex - 1], clusteredIndexType, attrIndex);
                }
                PCounter.printStats();
                System.out.println("New table created " + fileName.toUpperCase());
                System.out.println("Record count: " + count);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void createIndex(String tableName, short unclusteredIndexType, int attrIndex) throws IOException, FieldNumberOutOfBoundException {

        Tuple t;
        int count;
        //Check if clustered index exists
        int indexTypeIfExists = findIfIndexExists(tableName, -1);
        getTableAttrsAndType(tableName);
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned ***\n");
//            status = FAIL;
//        }

        int indexOnAttrExists = findIfIndexExists(tableName, attrIndex);

        if (status != OK) {
            return;
        }

        if (indexOnAttrExists != NO_INDEX) {
            String indexType = "";
            switch (indexOnAttrExists) {
                case 0:
                    indexType = "CLUSTERED_HASH";
                    break;
                case 1:
                    indexType = "CLUSTERED_BTREE";
                    break;
                case 2:
                    indexType = "UNCLUSTERED_HASH";
                    break;
                case 3:
                    indexType = "UNCLUSTERED_BTREE";
                    break;
            }
            System.out.println("Index already exists on this attribute of type: " + indexType);
            return;
        }

        String indexFileName = tableName + '-' + unclusteredIndexType + '-' + attrIndex;

        try {
            short keySize = 4;
            if (attrType[attrIndex - 1].attrType == AttrType.attrString) {
                keySize = attrStringSize;
            }
            if (unclusteredIndexType == UNCLUSTERED_HASH) {
                unclusteredHashFile = new UnclusteredHashFile(indexFileName, attrType[attrIndex - 1].toInt(), keySize, 75);
            } else {
                bTreeUnclusteredFile = new BTreeFile(indexFileName, attrType[attrIndex - 1].toInt(), keySize, 0);
            }
        } catch (Exception e) {
            System.out.println("Failed to initialize unclustered index file!");
            e.printStackTrace();
        }
        PCounter.initialize();
        try {
            if (indexTypeIfExists == NO_INDEX) {
                try {
                    f = new Heapfile(tableName);
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Could not create heap file ***\n");
                    e.printStackTrace();
                }

                Scan scan = null;
                RID rid = new RID();

                try {
                    scan = f.openScan();
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Error opening scan\n");
                    e.printStackTrace();
                }

                t = new Tuple();

                try {
                    t = scan.getNext(rid);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                while (t != null) {
                    try {
                        t.setHdr((short) nColumns, attrType, attrSizes);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }


                    try {
                        if (unclusteredIndexType == UNCLUSTERED_HASH) {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                                unclusteredHashFile.insertRecord(key, rid);
                            } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                                hash.FloatKey key = new hash.FloatKey(t.getFloFld(attrIndex));
                                unclusteredHashFile.insertRecord(key, rid);
                            } else {
                                hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                                unclusteredHashFile.insertRecord(key, rid);
                            }
                        } else {
                            if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                                IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                                bTreeUnclusteredFile.insert(key, rid);
                            } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                                FloatKey key = new FloatKey(t.getFloFld(attrIndex));
                                bTreeUnclusteredFile.insert(key, rid);
                            } else {
                                StringKey key = new StringKey(t.getStrFld(attrIndex));
                                bTreeUnclusteredFile.insert(key, rid);
                            }
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                        return;
                    }

                    try {
                        t = scan.getNext(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                // clean up
                try {
                    scan.closescan();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            } else if (indexTypeIfExists == CLUSTERED_BTREE) {
                bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                BTClusteredFileScan scan = null;
                try {
                    scan = bTreeClusteredFile.new_scan(null, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeyNotMatchException e) {
                    e.printStackTrace();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                }

                KeyDataEntry data = null;
                rid = new RID();
                data = scan.get_next(rid);
                t = null;

                while (data != null) {
                    try {
                        t = ((Tuple) ((ClusteredLeafData) data.data).getData());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (unclusteredIndexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(t.getFloFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        }
                    } else {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(t.getFloFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        }
                    }

                    try {
                        data = scan.get_next(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeClusteredFile.close();

            } else if (indexTypeIfExists == CLUSTERED_HASH) {
                RID rid = new RID();
                hashFile = new ClusteredHashFile(tableName, (short) attrType.length, attrType, attrSizes);
                ClusteredHashFileScan fscan = null;
                fscan = hashFile.newScan(null, null);
                t = null;
                t = fscan.getNextTuple(rid);

                while (t != null) {
//                    try {
//                        t.print(attrType);
//                    } catch (Exception e) {
//                        status = FAIL;
//                        e.printStackTrace();
//                    }

                    if (unclusteredIndexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(t.getFloFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, rid);
                        }
                    } else {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(t.getFloFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, rid);
                        }
                    }

                    try {
                        t = fscan.getNextTuple(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                hashFile.close();
            }
            System.out.println("Successfully created unclustered index!");
            PCounter.printStats();
            addIndexToCatalog(tableName, attrNames[attrIndex - 1], unclusteredIndexType, attrIndex);
            if (unclusteredIndexType == UNCLUSTERED_HASH) {
                unclusteredHashFile.close();
            } else {
                bTreeUnclusteredFile.close();
            }
        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to print results");
            e.printStackTrace();
        }
    }

    private void printIndexKeys(String tableName, int attrIndex) throws IOException, FieldNumberOutOfBoundException {

        Tuple t;
        int count = 0;
        //Check if clustered index exists
        getTableAttrsAndType(tableName);
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned ***\n");
//            status = FAIL;
//        }

        int indexOnAttrExists = findIfIndexExists(tableName, attrIndex);

        if (status != OK) {
            return;
        }

        if (indexOnAttrExists == NO_INDEX) {
            System.out.println("No Index exists on this attribute: ");
            return;
        }

        String unclusteredIndexFileName = tableName + '-' + indexOnAttrExists + '-' + attrIndex;

        short keySize = 4;
        if (attrType[attrIndex - 1].attrType == AttrType.attrString) {
            keySize = attrStringSize;
        }

        System.out.println("***KEYS***");
        System.out.println("----------");
        rid = new RID();
        PCounter.initialize();
        try {
            if (indexOnAttrExists == CLUSTERED_HASH) {
                hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                ClusteredHashFileScan fscan = null;
                fscan = hashFile.newScan(null, null);
                t = null;
                t = fscan.getNextTuple(rid);


                while (t != null) {
                    try {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            System.out.println(t.getIntFld(attrIndex));
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            System.out.println(t.getFloFld(attrIndex));
                        } else {
                            System.out.println(t.getStrFld(attrIndex));
                        }
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    count++;

                    try {
                        t = fscan.getNextTuple(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                hashFile.close();
            } else if (indexOnAttrExists == CLUSTERED_BTREE) {
                bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                BTClusteredFileScan scan = null;
                try {
                    scan = bTreeClusteredFile.new_scan(null, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeyNotMatchException e) {
                    e.printStackTrace();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                }

                KeyDataEntry data = null;
                data = scan.get_next(rid);
//                t = null;


                while (data != null) {
                    try {
//                        t = ((Tuple) ((ClusteredLeafData) data.data).getData());
                        System.out.println(data.key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    count++;

//                    if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
//                        System.out.println(t.getIntFld(attrIndex));
//                    } else {
//                        System.out.println(t.getStrFld(attrIndex));
//                    }

                    try {
                        data = scan.get_next(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeClusteredFile.close();

            } else if (indexOnAttrExists == UNCLUSTERED_HASH) {
                unclusteredHashFile = new UnclusteredHashFile(unclusteredIndexFileName);
                UnclusteredHashRecord record = null;
                UnclusteredHashFileScan scan = unclusteredHashFile.newScan(null, null);
                try {
                    record = scan.getNextRecord();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (PageUnpinnedException e) {
                    e.printStackTrace();
                } catch (InvalidFrameNumberException e) {
                    e.printStackTrace();
                } catch (HashEntryNotFoundException e) {
                    e.printStackTrace();
                } catch (ReplacerException e) {
                    e.printStackTrace();
                }

                while (record != null) {
                    try {
                        System.out.println(record.toString());
                        count++;
                        record = scan.getNextRecord();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (PageUnpinnedException e) {
                        e.printStackTrace();
                    } catch (InvalidFrameNumberException e) {
                        e.printStackTrace();
                    } catch (HashEntryNotFoundException e) {
                        e.printStackTrace();
                    } catch (ReplacerException e) {
                        e.printStackTrace();
                    }
                }
                unclusteredHashFile.close();
            } else if (indexOnAttrExists == UNCLUSTERED_BTREE) {
                bTreeUnclusteredFile = new BTreeFile(unclusteredIndexFileName);
                BTFileScan scan = bTreeUnclusteredFile.new_scan(null, null);

                KeyDataEntry data = null;

                data = scan.get_next();

                while (data != null) {
                    try {
                        System.out.println(data.key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    count++;

                    try {
                        data = scan.get_next();
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeUnclusteredFile.close();
            }
            PCounter.printStats();
            System.out.println("AT THE END OF SCAN!");
            System.out.println("*** TOTAL KEYS " + count + " ***");

        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to print index keys");
            e.printStackTrace();
        }
    }

    private void addIndexToCatalog(String relName, String attrName, short indexType, int attrIndex) throws IOException, FieldNumberOutOfBoundException, HFDiskMgrException, InvalidTupleSizeException, HFException, InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {
        try {
            indexCatalogFile = new Heapfile(INDEX_FILE_NAME);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }

        indexTuple.setStrFld(1, relName);
        indexTuple.setStrFld(2, attrName);
        indexTuple.setIntFld(3, (int) indexType);
        indexTuple.setIntFld(4, attrIndex);

        indexCatalogFile.insertRecord(indexTuple.returnTupleByteArray());
    }

    // Return index type
    private int findIfIndexExists(String relName, int attrIndex) {
        // Read data and construct tuples
        FileScan fscan = null;
        int indexType = NO_INDEX;

        FldSpec[] projections = new FldSpec[indexAttrTypes.length];

        for (int i = 0; i < indexAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(INDEX_FILE_NAME, indexAttrTypes, indexAttrSizes, (short) indexAttrTypes.length, indexAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        while (t != null) {
            try {
                if (attrIndex == -1 && t.getStrFld(1).equals(relName) && (t.getIntFld(3) == (short) 5 || t.getIntFld(3) == CLUSTERED_BTREE || t.getIntFld(3) == CLUSTERED_HASH)) {
                    indexType = t.getIntFld(3);
                    break;
                }
                if (attrIndex != -1 && t.getStrFld(1).equals(relName) && t.getIntFld(4) == attrIndex) {
                    indexType = t.getIntFld(3);
                    break;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        return indexType;
    }

    private List<IndexDesc> getAllIndexesForRel(String relName) {
        // Read data and construct tuples
        FileScan fscan = null;

        FldSpec[] projections = new FldSpec[indexAttrTypes.length];

        for (int i = 0; i < indexAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(INDEX_FILE_NAME, indexAttrTypes, indexAttrSizes, (short) indexAttrTypes.length, indexAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        List<IndexDesc> allIndexes = new ArrayList<IndexDesc>();
        while (t != null) {
            try {
                if (t.getStrFld(1).equals(relName)) {
                    IndexDesc id = new IndexDesc(t.getIntFld(3), t.getIntFld(4));
                    allIndexes.add(id);
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

//            nColumns++;

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        return allIndexes;
    }

    private void printTable(String tableName) throws IOException {

        int count = 0;
        getTableAttrsAndType(tableName);
        CustomScan scan = null;
        try {
            scan = new CustomScan(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Tuple tup = null;
        try {
            tup = scan.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (tup != null) {
            tup.print(attrType);
            count++;
            try {
                tup = scan.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            scan.close();
        } catch (Exception e) {
//            e.printStackTrace();
        }
        System.out.println("Record count: " + count);

    }

    private void insert_data(String tableName, String filename) throws IOException, FieldNumberOutOfBoundException {
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;
//        }


        if (status == OK) {
            getTableAttrsAndType(tableName);
          File file = new File("../../data/" + filename + ".csv");
//            File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + filename + ".csv");
            Scanner sc = new Scanner(file);
            int attrIndex = -1;
            int indexTypeIfExists = findIfIndexExists(tableName, -1);
            List<IndexDesc> allIndexes = getAllIndexesForRel(tableName);

            if (indexTypeIfExists != NO_INDEX) {
                for (int i = 0; i < allIndexes.size(); i++) {
                    if (allIndexes.get(i).indexType == indexTypeIfExists) {
                        attrIndex = allIndexes.get(i).attrIndex;
                        break;
                    }
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                    RidChanges = new ArrayList<BTreeClusteredFile.RidChange>();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                } else {
                    f = new Heapfile(tableName);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                }
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not open file ***\n");
                e.printStackTrace();
            }

            String columnMetaData;
            int attributeType;

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim().split(",")[1];
                if (columnMetaData.equals("INT")) {
                    attributeType = AttrType.attrInteger;
                } else if (columnMetaData.equals("STR")) {
                    attributeType = AttrType.attrString;
                } else {
                    attributeType = AttrType.attrReal;
                }
                if (attributeType != attrType[i].attrType) {
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            int count = 0;
            RidTuplePair ridtuple;
            PCounter.initialize();
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");
                tuple1 = new Tuple(size);
                try {
                    tuple1.setHdr((short) nColumns, attrType, attrSizes);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    status = FAIL;
                    e.printStackTrace();
                }
                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            int value = Integer.parseInt(row[i]);
                            tuple1.setIntFld(i + 1, value);
                        } else if (attrType[i].toInt().equals(AttrType.attrReal)) {
                            float value = Float.parseFloat(row[i]);
                            tuple1.setFloFld(i + 1, value);
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    if (indexTypeIfExists == CLUSTERED_BTREE) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
                            RidChanges = bTreeClusteredFile.insert(key, tuple1);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(tuple1.getFloFld(attrIndex));
                            RidChanges = bTreeClusteredFile.insert(key, tuple1);
                        } else {
                            StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
                            RidChanges = bTreeClusteredFile.insert(key, tuple1);
                        }
                        IndexDesc index;
                        for (int i = 0; i < allIndexes.size(); i++) {
                            index = allIndexes.get(i);
                            if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                                bulkRestructureUnclustered(RidChanges, tableName, index.indexType, index.attrIndex, tuple1);
                            }
                        }
                    } else if (indexTypeIfExists == CLUSTERED_HASH) {
                        ridtuple = new RidTuplePair();
                        ridtuple.tuple = tuple1;
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                            ridtuple.rid = hashFile.insertRecord(key, tuple1);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(tuple1.getFloFld(attrIndex));
                            ridtuple.rid = hashFile.insertRecord(key, tuple1);
                        } else {
                            hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                            ridtuple.rid = hashFile.insertRecord(key, tuple1);
                        }
                        ridTuplePairs.add(ridtuple);
                    } else {
                        ridtuple = new RidTuplePair();
                        ridtuple.rid = f.insertRecord(tuple1.returnTupleByteArray());
                        ridtuple.tuple = tuple1;
                        ridTuplePairs.add(ridtuple);
                    }
                    count++;
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile.close();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile.close();
                }
                System.out.println("New records inserted: " + count);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (indexTypeIfExists != CLUSTERED_BTREE) {
                IndexDesc index;
                for (int i = 0; i < allIndexes.size(); i++) {
                    index = allIndexes.get(i);
                    if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                        bulkUpdateUnclustered(ridTuplePairs, tableName, index.indexType, index.attrIndex, true);
                    }
                }
            }
            PCounter.printStats();

        }
    }

    private Boolean bulkRestructureUnclustered(ArrayList<BTreeClusteredFile.RidChange> ridChanges, String tableName, int indexType, int attrIndex, Tuple tuplerecord) throws FileNotFoundException {
        String indexFileName = tableName + '-' + indexType + '-' + attrIndex;

        try {
            if (indexType == UNCLUSTERED_HASH) {
                unclusteredHashFile = new UnclusteredHashFile(indexFileName);
            } else {
                bTreeUnclusteredFile = new BTreeFile(indexFileName);
                bTreeUnclusteredFile = new BTreeFile(indexFileName);
            }
        } catch (Exception e) {
            System.out.println("Failed to initialize unclustered index file!");
            e.printStackTrace();
        }
        Boolean isNewUpdate = false;
        BTreeClusteredFile.RidChange ridChange = null;
        Tuple t;
        for (int i = 0; i < ridChanges.size(); i++) {
            ridChange = ridChanges.get(i);
            try {
                if (ridChange.keyData != null) {
                    t = ((Tuple) ((ClusteredLeafData) ridChange.keyData.data).getData());
                } else {
                    t = tuplerecord;
                }
                if (indexType == UNCLUSTERED_HASH) {
                    if (ridChange.newRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(t.getFloFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid != null && ridChange.newRid != null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(t.getIntFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(t.getFloFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        } else {
                            hash.StringKey key = new hash.StringKey(t.getStrFld(attrIndex));
                            unclusteredHashFile.deleteRecord(key, ridChange.oldRid);
                            unclusteredHashFile.insertRecord(key, ridChange.newRid);
                        }
                    }
                } else {
                    if (ridChange.newRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(t.getFloFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid == null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(t.getFloFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);
                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);
                        }
                        isNewUpdate = true;
                    } else if (ridChange.oldRid != null && ridChange.newRid != null) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(t.getIntFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);

                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(t.getFloFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);

                        } else {
                            StringKey key = new StringKey(t.getStrFld(attrIndex));
                            bTreeUnclusteredFile.Delete(key, ridChange.oldRid);
                            bTreeUnclusteredFile.insert(key, ridChange.newRid);
                        }
                    }
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        try {
            if (indexType == UNCLUSTERED_HASH) {
                unclusteredHashFile.close();
            } else {
                bTreeUnclusteredFile.close();
            }
            System.out.println("Unclustered index updated on attr " + attrIndex);
        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to close files");
            e.printStackTrace();
        }
        return isNewUpdate;
    }

    private void bulkUpdateUnclustered(ArrayList<RidTuplePair> rtPair, String tableName, int indexType, int attrIndex, Boolean isInsert) throws FileNotFoundException {
//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;
//        }


        if (status == OK) {
            String indexFileName = tableName + '-' + indexType + '-' + attrIndex;

            try {
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile = new UnclusteredHashFile(indexFileName);
                } else {
                    bTreeUnclusteredFile = new BTreeFile(indexFileName);
                }
            } catch (Exception e) {
                System.out.println("Failed to initialize unclustered index file!");
                e.printStackTrace();
            }

            RidTuplePair ridtuple = null;
            for (int i = 0; i < rtPair.size(); i++) {
                ridtuple = rtPair.get(i);
                try {
                    if (indexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(ridtuple.tuple.getIntFld(attrIndex));
                            if (isInsert) {
                                unclusteredHashFile.insertRecord(key, ridtuple.rid);
                            } else {
                                unclusteredHashFile.deleteRecord(key, ridtuple.rid);
                            }
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(ridtuple.tuple.getFloFld(attrIndex));
                            if (isInsert) {
                                unclusteredHashFile.insertRecord(key, ridtuple.rid);
                            } else {
                                unclusteredHashFile.deleteRecord(key, ridtuple.rid);
                            }
                        } else {
                            hash.StringKey key = new hash.StringKey(ridtuple.tuple.getStrFld(attrIndex));
                            if (isInsert) {
                                unclusteredHashFile.insertRecord(key, ridtuple.rid);
                            } else {
                                unclusteredHashFile.deleteRecord(key, ridtuple.rid);
                            }
                        }
                    } else {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(ridtuple.tuple.getIntFld(attrIndex));
                            if (isInsert) {
                                bTreeUnclusteredFile.insert(key, ridtuple.rid);
                            } else {
                                bTreeUnclusteredFile.Delete(key, ridtuple.rid);
                            }
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(ridtuple.tuple.getFloFld(attrIndex));
                            if (isInsert) {
                                bTreeUnclusteredFile.insert(key, ridtuple.rid);
                            } else {
                                bTreeUnclusteredFile.Delete(key, ridtuple.rid);
                            }
                        } else {
                            StringKey key = new StringKey(ridtuple.tuple.getStrFld(attrIndex));
                            bTreeUnclusteredFile.insert(key, ridtuple.rid);
                            if (isInsert) {
                                bTreeUnclusteredFile.insert(key, ridtuple.rid);
                            } else {
                                bTreeUnclusteredFile.Delete(key, ridtuple.rid);
                            }
                        }
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile.close();
                } else {
                    bTreeUnclusteredFile.close();
                }
            } catch (Exception e) {
                status = FAIL;
                System.out.println("Failed to close files");
                e.printStackTrace();
            }

            try {
                System.out.println("Unclustered index updated on attr " + attrIndex);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    private void delete_data(String tableName, String filename) throws IOException, FieldNumberOutOfBoundException, TupleUtilsException, InvalidTupleSizeException, hash.ConstructPageException, InvalidSlotNumberException, InvalidTypeException, UnknowAttrType, UnpinPageException, DeleteRecException, IndexSearchException, RedistributeException, PinPageException, FreePageException, DeleteFashionException, LeafRedistributeException, IndexInsertRecException, IndexFullDeleteException, InsertRecException, KeyNotMatchException, LeafDeleteException, RecordNotFoundException, ConstructPageException, IteratorException {

//        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
//                != SystemDefs.JavabaseBM.getNumBuffers()) {
//            System.err.println("*** The heap file has left pages pinned\n");
//            status = FAIL;
//        }

        if (status == OK) {

            getTableAttrsAndType(tableName);
          File file = new File("../../data/" + filename + ".csv");
//            File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + filename + ".csv");
            Scanner sc = new Scanner(file);
            int attrIndex = 1;
            int indexTypeIfExists = findIfIndexExists(tableName, -1);
            List<IndexDesc> allIndexes = getAllIndexesForRel(tableName);

            if (indexTypeIfExists != NO_INDEX) {
                for (int i = 0; i < allIndexes.size(); i++) {
                    if (allIndexes.get(i).indexType == indexTypeIfExists) {
                        attrIndex = allIndexes.get(i).attrIndex;
                        break;
                    }
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                    RidChanges = new ArrayList<BTreeClusteredFile.RidChange>();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                } else {
                    f = new Heapfile(tableName);
                    ridTuplePairs = new ArrayList<RidTuplePair>();
                }
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not open file ***\n");
                e.printStackTrace();
            }

            String columnMetaData;
            int attributeType;

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < attrType.length; i++) {
                columnMetaData = sc.nextLine().trim().split(",")[1];
                if (columnMetaData.equals("INT")) {
                    attributeType = AttrType.attrInteger;
                } else if (columnMetaData.equals("STR")) {
                    attributeType = AttrType.attrString;
                } else {
                    attributeType = AttrType.attrReal;
                }
                if (attributeType != attrType[i].attrType) {
                    System.out.println("Attributes in file do not match the table!");
                    return;
                }
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            Integer count = 0;
            Boolean match, removed = false, result;
            Scan scan;
            RID rid;
            RidTuplePair ridtuple;
            PCounter.initialize();
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");
                tuple1 = new Tuple(size);
                try {
                    tuple1.setHdr((short) nColumns, attrType, attrSizes);
                } catch (Exception e) {
                    System.err.println("*** error in Tuple.setHdr() ***");
                    status = FAIL;
                    e.printStackTrace();
                }
                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            tuple1.setIntFld(i + 1, Integer.parseInt(row[i]));
                        } else if (attrType[i].toInt().equals(AttrType.attrReal)) {
                            tuple1.setFloFld(i + 1, Float.parseFloat(row[i]));
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                if (indexTypeIfExists == NO_INDEX) {
                    scan = null;
                    rid = new RID();

                    try {
                        scan = f.openScan();
                    } catch (Exception e) {
                        status = FAIL;
                        System.err.println("*** Error opening scan\n");
                        e.printStackTrace();
                    }

                    Tuple t = new Tuple();

                    try {
                        t = scan.getNext(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    while (t != null) {
                        try {
                            t.setHdr((short) nColumns, attrType, attrSizes);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }
                        match = true;
                        try {
                            result = TupleUtils.Equal(tuple1, t, attrType, row.length);
                            if (!result) {
                                match = false;
                            }
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                            return;
                        }


                        if (match) {
                            try {
                                ridtuple = new RidTuplePair();
                                removed = f.deleteRecord(rid);

                                if (removed) {
                                    count++;
                                    ridtuple.rid = rid;
                                    ridtuple.tuple = tuple1;
                                    ridTuplePairs.add(ridtuple);
                                    System.out.print("Successfully deleted: ");
                                } else {
                                    System.out.print("Failed to remove: ");
                                }
                                t.print(attrType);
                                // break;
                            } catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }
                        }

                        try {
                            t = scan.getNext(rid);
                        } catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }
                    }
                    // clean up
                    try {
                        scan.closescan();
                    } catch (Exception e) {
                        status = FAIL;
//                        e.printStackTrace();
                    }
                } else if (indexTypeIfExists == CLUSTERED_BTREE) {
                    try {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
                            RidChanges = bTreeClusteredFile.Delete(key, tuple1);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            FloatKey key = new FloatKey(tuple1.getFloFld(attrIndex));
                            RidChanges = bTreeClusteredFile.Delete(key, tuple1);
                        } else {
                            StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
                            RidChanges = bTreeClusteredFile.Delete(key, tuple1);
                        }
                        IndexDesc index;
                        for (int i = 0; i < allIndexes.size(); i++) {
                            index = allIndexes.get(i);
                            if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                                removed = bulkRestructureUnclustered(RidChanges, tableName, index.indexType, index.attrIndex, tuple1);
                            }
                        }
                        for (int i = 0; i < RidChanges.size(); i++) {
                            if (RidChanges.get(i).newRid == null) {
                                count++;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    ridtuple = new RidTuplePair();
                    ridtuple.tuple = tuple1;
                    try {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                            ridtuple.rid = hashFile.deleteRecord(key, tuple1);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(tuple1.getFloFld(attrIndex));
                            ridtuple.rid = hashFile.deleteRecord(key, tuple1);
                        } else {
                            hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                            ridtuple.rid = hashFile.deleteRecord(key, tuple1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (ridtuple.rid != null) {
                        count++;
                        ridTuplePairs.add(ridtuple);
                    }
                }
            }

            try {
                if (indexTypeIfExists == CLUSTERED_BTREE) {
                    bTreeClusteredFile.close();
                } else if (indexTypeIfExists == CLUSTERED_HASH) {
                    hashFile.close();
                }
                System.out.println("Records removed: " + count);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (indexTypeIfExists != CLUSTERED_BTREE) {
                IndexDesc index;
                for (int i = 0; i < allIndexes.size(); i++) {
                    index = allIndexes.get(i);
                    if (index.indexType == UNCLUSTERED_BTREE || index.indexType == UNCLUSTERED_HASH) {
                        bulkUpdateUnclustered(ridTuplePairs, tableName, index.indexType, index.attrIndex, false);
                    }
                }
            }
            PCounter.printStats();
        }
    }

    private void bulkRemoveUnclustered(String filename, String tableName, int indexType, int attrIndex) throws FileNotFoundException {
        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap file has left pages pinned\n");
            status = FAIL;
        }


        if (status == OK) {
          File file = new File("../../data/" + filename + ".csv");
//            File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + filename + ".csv");
            Scanner sc = new Scanner(file);

            nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

            for (int i = 0; i < nColumns; i++) {
                sc.nextLine();
            }

            String indexFileName = tableName + '-' + indexType + '-' + attrIndex;

            try {
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile = new UnclusteredHashFile(indexFileName);
                } else {
                    bTreeUnclusteredFile = new BTreeFile(indexFileName);
                }
            } catch (Exception e) {
                System.out.println("Failed to initialize unclustered index file!");
                e.printStackTrace();
            }

            Tuple tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            short size = tuple1.size();
            System.out.println("Size: " + size);
            tSize = size;

            tuple1 = new Tuple(size);
            try {
                tuple1.setHdr((short) nColumns, attrType, attrSizes);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            UnclusteredHashFileScan hfScan;
            BTFileScan btScan;
            UnclusteredHashRecord record = null;
            while (sc.hasNextLine()) {
                // create a tuple1 of appropriate size
                String[] row = sc.nextLine().trim().split(",");


                for (int i = 0; i < row.length; i++) {
                    try {
                        if (attrType[i].toInt().equals(AttrType.attrInteger)) {
                            int value = Integer.parseInt(row[i]);
                            tuple1.setIntFld(i + 1, value);
                        }
                        if (attrType[i].toInt().equals(AttrType.attrReal)) {
                            float value = Float.parseFloat(row[i]);
                            tuple1.setFloFld(i + 1, value);
                        } else {
                            tuple1.setStrFld(i + 1, row[i]);
                        }

                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }


                try {
                    if (indexType == UNCLUSTERED_HASH) {
                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
                            hash.IntegerKey key = new hash.IntegerKey(tuple1.getIntFld(attrIndex));
                            hfScan = unclusteredHashFile.newScan(key, key);
                            record = hfScan.getNextRecord();
                            rid = record.getRid();
                            unclusteredHashFile.deleteRecord(key, rid);
                        } else if (attrType[attrIndex - 1].toInt().equals(AttrType.attrReal)) {
                            hash.FloatKey key = new hash.FloatKey(tuple1.getFloFld(attrIndex));
                            hfScan = unclusteredHashFile.newScan(key, key);
                            record = hfScan.getNextRecord();
                            rid = record.getRid();
                            unclusteredHashFile.deleteRecord(key, rid);
                        } else {
                            hash.StringKey key = new hash.StringKey(tuple1.getStrFld(attrIndex));
                            hfScan = unclusteredHashFile.newScan(key, key);
                            record = hfScan.getNextRecord();
                            rid = record.getRid();
                            unclusteredHashFile.deleteRecord(key, rid);
                        }
                    } else {
//                        if (attrType[attrIndex - 1].toInt().equals(AttrType.attrInteger)) {
//                            IntegerKey key = new IntegerKey(tuple1.getIntFld(attrIndex));
//                            btScan = bTreeUnclusteredFile.new_scan(key,key);
//                            KeyDataEntry kd = btScan.get_next();
//                            bTreeUnclusteredFile.Delete();
//                        } else {
//                            StringKey key = new StringKey(tuple1.getStrFld(attrIndex));
//                            bTreeUnclusteredFile.insert(key, rid);
//                        }
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            try {
                sc.close();
                if (indexType == UNCLUSTERED_HASH) {
                    unclusteredHashFile.close();
                } else {
                    bTreeUnclusteredFile.close();
                }
            } catch (Exception e) {
                status = FAIL;
                System.out.println("Failed to close scan");
                e.printStackTrace();
            }

            try {
                System.out.println("Unclustered index updated on attr " + attrIndex);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }
    }

    public static void runNestedLoopSky(String hf, Boolean outputResultToTable, String outputTableName) throws
            PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanNested = initialiseFileScan(hf);
        NestedLoopsSky nested = null;
        try {
            nested = new NestedLoopsSky(attrType, attrType.length, attrSizes, fscanNested, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(nested, outputResultToTable, outputTableName);

        try {
            nested.close();
            fscanNested.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runBNLSky(String hf, Boolean outputResultToTable, String outputTableName) throws
            PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscanBlock = initialiseFileScan(hf);
        Iterator block = null;
        try {
            block = new BlockNestedLoopsSky(attrType, attrType.length, attrSizes, fscanBlock, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(block, outputResultToTable, outputTableName);

        try {
            fscanBlock.close();
            block.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void runSortFirstSky(String hf, Boolean outputResultToTable, String outputTableName) throws
            PageNotFoundException, BufMgrException, HashOperationException, PagePinnedException {
        FileScan fscan = initialiseFileScan(hf);
        Iterator sort = null;
        try {
            sort = new SortFirstSky(attrType, attrType.length, attrSizes, fscan, hf, pref_list, pref_list.length, _n_pages);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        getNextAndPrintAllSkyLine(sort, outputResultToTable, outputTableName);

        try {
            sort.close();
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
        }
    }

    private void runBtreeSky(String hf, Boolean outputResultToTable, String outputTableName) throws Exception {
        System.out.println("Running BTreeSky");
        System.out.println("DataFile: " + hf);
        System.out.println("Preference list: " + Arrays.toString(pref_list));
        System.out.println("Number of pages: " + _n_pages);
        System.out.println("Pref list length: " + pref_list.length);
        if (!indexesCreated) {
            BTreeUtil.createBtreesForPrefList(hf, f, attrType, attrSizes, pref_list);
            indexesCreated = true;
        }

        // autobox to IndexFile type
        IndexFile[] index_file_list = BTreeUtil.getBTrees(pref_list);
        SystemDefs.JavabaseBM.flushPages();

        BTreeSky btreesky = new BTreeSky(attrType, nColumns, attrSizes, null, hf, pref_list,
                pref_list.length, index_file_list, _n_pages);

        PCounter.initialize();
        String tempHeapFile = btreesky.findBTreeSky();

        if (tempHeapFile != null) {
            runSortFirstSky(tempHeapFile, outputResultToTable, outputTableName);
            Heapfile tempHF = new Heapfile(tempHeapFile);
            tempHF.deleteFile();
        }

        System.out.println("BTreeSky Complete\n");
    }

    public void runBTreeSortedSky(String hf, Boolean outputResultToTable, String outputTableName) {
        try {
            BTreeCombinedIndex obj = new BTreeCombinedIndex();
            IndexFile indexFile = obj.combinedIndex(hf, attrType, attrSizes, pref_list, pref_list.length);
            System.out.println("Index created!");
            SystemDefs.JavabaseBM.flushPages();

            System.out.println("CombinedBTreeIndex scanning");
            String fileName = BTreeCombinedIndex.random_string1;

            BTreeSortedSky btree = new BTreeSortedSky(attrType, attrType.length, attrSizes, null, fileName, pref_list, pref_list.length, indexFile, _n_pages);
            PCounter.initialize();
            String tempHeapFile = btree.computeSkyline();

            if (tempHeapFile != null) {
                runSortFirstSky(tempHeapFile, outputResultToTable, outputTableName);
                Heapfile tempHF = new Heapfile(tempHeapFile);
                tempHF.deleteFile();
            }

            System.out.println("BTreeSortSky Complete");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getNextAndPrintAllSkyLine(Iterator iter, Boolean outputResultToTable, String outputTableName) {
// this needs to be before the fn call since call to any algo 1,2,3 from 4,5 reinitializes counter
//        PCounter.initialize();
        outputTable = null;
        if (outputResultToTable && outputTableName != null) {
            try {
                outputTable = new Heapfile(outputTableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        int count = -1;
        Tuple tuple1 = null;
        System.out.println("\n -- Skyline Objects -- ");
        do {
            try {
                if (tuple1 != null) {
                    tuple1.print(attrType);
                    if (outputTable != null && outputResultToTable && outputTableName != null) {
                        outputTable.insertRecord(tuple1.returnTupleByteArray());
                    }
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

        System.out.println("\nRead statistics " + PCounter.rcounter);
        System.out.println("Write statistics " + PCounter.wcounter);

        System.out.println("\nNumber of Skyline objects: " + count + "\n");
    }

    public static FileScan initialiseFileScan(String hf) {
        FileScan fscan = null;

        try {
            fscan = new FileScan(hf, attrType, attrSizes, (short) attrType.length, attrType.length, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return fscan;
    }

    private void setAttrDesc(String tableName) throws IOException, FieldNumberOutOfBoundException {
        try {
            f = new Heapfile(tableName + METAFILE_POSTFIX);
            f.deleteFile();
            f = new Heapfile(tableName + METAFILE_POSTFIX);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        File file = new File("../../data/" + tableName + ".csv");
//        File file = new File("..\\cse510dbmsi\\minjava\\javaminibase\\data\\" + tableName + ".csv");
        Scanner sc = new Scanner(file);

        nColumns = Integer.valueOf(sc.nextLine().trim().split(",")[0]);

        attrType = new AttrType[nColumns];


        String columnMetaData[];
        String attribute;
        attrNames = new String[nColumns];

        int stringColumns = 0;
        for (int i = 0; i < attrType.length; i++) {
            columnMetaData = sc.nextLine().trim().split(",");
            attribute = columnMetaData[1];
            attrNames[i] = columnMetaData[0];

            if (attribute.equals("INT")) {
                attrType[i] = new AttrType(AttrType.attrInteger);
            }
            else if (attribute.equals("STR")) {
                attrType[i] = new AttrType(AttrType.attrString);
                stringColumns++;
            } else {
                attrType[i] = new AttrType(AttrType.attrReal);
            }
        }

        attrSizes = new short[stringColumns];
        for (int i = 0; i < stringColumns; i++) {
            attrSizes[i] = attrStringSize;
        }

        for (int i = 0; i < attrType.length; i++) {
            metaTuple.setStrFld(1, attrNames[i]);
            metaTuple.setIntFld(2, attrType[i].attrType);
            if (attrType[i].attrType == AttrType.attrString) {
                metaTuple.setIntFld(3, attrStringSize);
            } else {
                metaTuple.setIntFld(3, 0);
            }

            try {
                rid = f.insertRecord(metaTuple.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        projlist = new FldSpec[nColumns];

        for (int i = 0; i < nColumns; i++) {
            projlist[i] = new FldSpec(rel, i + 1);
        }
    }

    private void setTableMeta(String tableName, AttrType[] attrT, short[] attrS, String[] attrN) throws IOException, FieldNumberOutOfBoundException {
        Heapfile filehf = null;
        try {
            filehf = new Heapfile(tableName + METAFILE_POSTFIX);
            filehf.deleteFile();
            filehf = new Heapfile(tableName + METAFILE_POSTFIX);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < attrT.length; i++) {
            metaTuple.setStrFld(1, attrN[i]);
            metaTuple.setIntFld(2, attrT[i].attrType);
            if (attrT[i].attrType == AttrType.attrString) {
                metaTuple.setIntFld(3, attrStringSize);
            } else {
                metaTuple.setIntFld(3, 0);
            }

            try {
                rid = filehf.insertRecord(metaTuple.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
    }

    public void getTableAttrsAndType(String tableName) {
        // Read data and construct tuples
        FileScan fscan = null;

        FldSpec[] projections = new FldSpec[metaAttrTypes.length];

        for (int i = 0; i < metaAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        nColumns = 0;
        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int stringColumns = 0;
        while (t != null) {
            try {
                if (t.getIntFld(2) == AttrType.attrString) {
                    stringColumns++;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            nColumns++;

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        attrType = new AttrType[nColumns];
        attrSizes = new short[stringColumns];
        attrNames = new String[nColumns];
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int i = 0, j = 0;
        while (t != null) {
            try {
                attrNames[i] = t.getStrFld(1);
                attrType[i] = new AttrType(t.getIntFld(2));
                if (t.getIntFld(2) == AttrType.attrString) {
                    attrSizes[j] = (short) t.getIntFld(3);
                    j++;
                }

            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            i++;
            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        projlist = new FldSpec[nColumns];

        for (i = 0; i < nColumns; i++) {
            projlist[i] = new FldSpec(rel, i + 1);
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public void hashJoinTest(){
        System.out.println("------------------------ TEST 1 --------------------------");
        System.out.println("Enter the Query: ");
        String[] tokens = GetStuff.getStringChoice().split(" ");
        String fileName1 = null;
        String fileName2 = null;
        HashJoin hashJoin = null;
        HashMap<String, Short> operationMap = new HashMap<>();
        operationMap.put("=", EQUAL);
        operationMap.put(">", GREATER);
        operationMap.put("<", LESS);
        operationMap.put(">=", GREATEROREQUAL);
        operationMap.put("<=", LESSOREQUAL);



        // Join NLS 2,3 r_sii2000_10_10_10_dup 10 MATER
//        Join HJ r_sii2000_1_75_200 2 r_sii2000_10_10_10 2 == 10 mater
//        JOIN NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP NPAGES [MATER OUTTABLENAME]
        if(tokens.length>1){
            fileName1 = tokens[2];
            fileName2 = tokens[4];
            String operation = tokens[6];
            int attr1 = Integer.parseInt(tokens[3]);
            int attr2 = Integer.parseInt(tokens[5]);
            outputTableName = null;
            outputResultToTable = false;

            if(tokens.length>9){

                if(tokens[8].toLowerCase().equals("mater")){
                    try {
                        outputTableName = tokens[9];
                        outputResultToTable = true;
                        outputTable = new Heapfile(outputTableName);
                        outputTable.deleteFile();
                        outputTable = new Heapfile(outputTableName);
                        AttrType[] type = new AttrType[attrType.length+attrType.length];
                        int count = 0;
                        for (AttrType att: attrType) {
                            type[count] = att;
                            count++;
                        }

                        for (AttrType att: attrType) {
                            type[count] = att;
                            count++;
                        }

                        short  []  Jsizes = new short[attrSizes.length + attrSizes.length];

                        count = 0;
                        for (short x: attrSizes) {
                            Jsizes[count] = x;
                            count++;
                        }

                        for (short x: attrSizes) {
                            Jsizes[count] = x;
                            count++;
                        }
                        String[] attrNameJoin = new String[attrNames.length+ attrNames.length];
                        for(int i = 0; i<attrNames.length; i++){
                            attrNameJoin[i] = attrNames[i];
                        }
                        for(int i = 0; i<attrNames.length; i++){
                            attrNameJoin[i] = attrNames[i];
                        }
//                        setTableMeta(outputTableName, type, Jsizes, attrNameJoin);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }



            if(tokens[1].equals("NLJ") || tokens[1].toLowerCase().equals("nlj")){
//                if(!tokens[6].equals("=")){
//                    System.out.println("Hash Join supports only equijoin, will consider your operation as ==");
//                }
                try {


                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    IndexJoin idx = new IndexJoin(attrType, attrType.length, attrSizes,
                            attrType, attrType.length, attrSizes, 10, fileName1, fileName2, attrType.length - 1, attr1, attr2, NO_INDEX, operationMap.get(operation));
                    java.util.Iterator i = idx.getNext();
                    AttrType[] outType = idx.getOutputAttrType();
                    while((i.hasNext())){
                        Tuple tuple = (Tuple) i.next();
                        tuple.print(idx.getOutputAttrType());
                        Tuple tempTpl = new Tuple(tuple.size());
                        tempTpl.tupleCopy(tuple);
                        if(outputTable!=null){
                            outputTable.insertRecord(tempTpl.getTupleByteArray());
                        }
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }
            }else if(tokens[1].equals("HJ") || tokens[1].toLowerCase().equals("hj")){
                try {
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    CustomScan cs1 = new CustomScan(fileName1);
                    CustomScan cs2 = new CustomScan(fileName2);

                    hashJoin = new HashJoin(cs2, attrType, cs1, attrType, attr2, attr1, attrSizes, attrSizes, false);
                    java.util.Iterator t = hashJoin.get_next();

                    while((t.hasNext())){
                        Tuple tuple = (Tuple) t.next();
                        tuple.print(hashJoin.getOutputAttrType());
                        Tuple tempTpl = new Tuple(tuple.size());
                        tempTpl.tupleCopy(tuple);
                        if(outputTable!=null){
                            outputTable.insertRecord(tempTpl.getTupleByteArray());
                        }
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }
                //hj
            }else if(tokens[1].equals("INLJ") || tokens[1].toLowerCase().equals("inlj")){
                int indexTypeIfExists = findIfIndexExists(fileName2, attr2);
                try {
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    IndexJoin idx = new IndexJoin(attrType, attrType.length, attrSizes,
                            attrType, attrType.length, attrSizes, 10, fileName1, fileName2, attrType.length - 1, attr1, attr2, indexTypeIfExists, operationMap.get(operation));
                    java.util.Iterator i = idx.getNext();
                    while((i.hasNext())){
                        Tuple tuple = (Tuple) i.next();
                        tuple.print(idx.getOutputAttrType());
                        Tuple tempTpl = new Tuple(tuple.size());
                        tempTpl.tupleCopy(tuple);
                        if(outputTable!=null){
                            outputTable.insertRecord(tempTpl.getTupleByteArray());
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
                //index nlj
            }else if(tokens[1].equals("SMJ") || tokens[1].toLowerCase().equals("smj")){
                try{
                    SystemDefs.JavabaseBM.flushPages();
                    PCounter.initialize();
                    IndexJoin idx = new IndexJoin(attrType, attrType.length, attrSizes,
                            attrType, attrType.length, attrSizes, 10, fileName1, fileName2, attrType.length - 1, attr1, attr2, NO_INDEX, operationMap.get(operation));
                    java.util.Iterator i = idx.getNext();
                    AttrType[] outType = idx.getOutputAttrType();
                    while((i.hasNext())){
                        Tuple tuple = (Tuple) i.next();
                        tuple.print(idx.getOutputAttrType());
                        Tuple tempTpl = new Tuple(tuple.size());
                        tempTpl.tupleCopy(tuple);
                        if(outputTable!=null){
                            outputTable.insertRecord(tempTpl.getTupleByteArray());
                        }
                    }
                    RID tempRID = new RID();

                }catch (Exception e){
                    e.printStackTrace();
                }
                //smj
            }

            System.out.println("\nRead statistics "+PCounter.rcounter);
            System.out.println("Write statistics "+PCounter.wcounter);






        }else{

            String c = null;
            System.out.println("Enter Outer file name ");
            c = GetStuff.getStringChoice();
            fileName1 = c;
            System.out.println("Enter Inner file name");
            c = GetStuff.getStringChoice();
            fileName2 = c;

            System.out.println("Enter the inner join attribute index");
            int attrIndex= GetStuff.getChoice();

            System.out.println("Enter the outer join attrbute index");
            int attrIndex2 = GetStuff.getChoice();

            getTableAttrsAndType(fileName1);
            getSecondTableAttrsAndType(fileName2);


    //            try {
    //                createTable(file1, false, NO_INDEX, 0);
    //                createTable(file2, false, NO_INDEX, 0);
    //            }catch (Exception e){
    //                e.printStackTrace();
    //            }

    //            short [] JJsize = new short[1];
    //            JJsize[0] = 30;

    //            FileScan fscan = null;

                // Sort "test3.in" on the int attribute (field 3) -- Ascending
                System.out.println("\n -- Hash Join results -- ");

    //            try {
    //                fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
    //            } catch (Exception e) {
    //                status = FAIL;
    //                e.printStackTrace();
    //            }

    //            int[] pref_list = new int[] {1,2};
                HashJoin hashjoin = null;
                short  []  Jsizes = new short[2];
                Jsizes[0] = 30;
                Jsizes[1] = 30;
                try {

                    CustomScan cs1 = new CustomScan(fileName1);
                    CustomScan cs2 = new CustomScan(fileName2);
                    hashjoin = new HashJoin(cs1, attrType, cs2,attrType, attrIndex, attrIndex2, attrSizes, attrSizes, false);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

    //            count = 0;
    //            t = null;

                try {
    //                Tuple t = new Tuple();
                    java.util.Iterator t = hashjoin.get_next();

                    while((t.hasNext())){
                        Tuple tuple = (Tuple) t.next();
                        tuple.print(hashjoin.getOutputAttrType());
                    }
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
             System.out.println("\nRead statistics "+PCounter.rcounter);
            System.out.println("Write statistics "+PCounter.wcounter);
        }
            System.out.println("------------------- Hash Join Test completed ---------------------\n");


    }

    //Index join
    public void IndexJoin_CondExpr(CondExpr[] expr) {

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        expr[1] = null;
    }
    public void indexJoin(){
        System.out.println("------------------------ TEST 1 --------------------------");
        String fileName1 = null;
        String fileName2 = null;
        String c = null;
        System.out.println("Enter Outer file name ");
        c = GetStuff.getStringChoice();
        fileName1 = c;
        System.out.println("Enter Inner file name");
        c = GetStuff.getStringChoice();
        fileName2 = c;

        System.out.println("Choose operator");
        System.out.println("[0] =\n" +
                "[1] <\n" +
                "[2] >\n" +
                "[3] <\n" +
                "[4] >=\n" +
                " [5] >");
        int op= GetStuff.getChoice();
        short operation=0;
        if(op==0){
            operation = EQUAL;
        }else if(op == 1){
            operation = LESS;
        }else if(op==2){
            operation = GREATER;
        }else if(op==3){
            operation = LESSOREQUAL;
        }else if(op==4){
            operation = GREATEROREQUAL;
        }




//            System.out.println("\n -- Testing BlockNestedLoopsSky on correlated tuples -- ");
//            boolean status = OK;
//            String file1 = "r_sii2000_1_75_200";
//            String file2 = "r_sii2000_10_10_10";


        System.out.println("Enter the outer attribute");
        int attrIndex= GetStuff.getChoice();
        System.out.println("Enter the inner attribute");
        int attrIndex2 = GetStuff.getChoice();

//
//        String fileName1 = "r_sii2000_10_10_10";
//        String fileName2 = "r_sii2000_1_75_200";
//



//        try {
//            createTable(fileName1, false, NO_INDEX, 0);
//            createTable(fileName2, false, NO_INDEX, 0);
//        }catch (Exception e){
//            e.printStackTrace();
//        }

        int indexTypeIfExists = findIfIndexExists(fileName2, -1);

        CondExpr [] outFilter  = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        IndexJoin_CondExpr(outFilter);

        FldSpec [] Sprojection = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 1),
        };



        Iterator am1 = null;

        //FileScan(fileName1,attrType,attrStringSize,attrType.length, attrType.length,)
        try {
//            short operation = 0;

//            System.out.println(attrType2);
//            System.out.println(attrType);
//            System.out.println(attrSizes);
            IndexJoin idx = new IndexJoin(attrType, attrType.length, attrSizes,
                    attrType, attrType.length, attrSizes, 10, fileName1, fileName2,attrType.length-1, attrIndex,attrIndex2, indexTypeIfExists, operation);
//            Tuple tpl=idx.get_next();
//            if(tpl==null){
//            if(idx.nestedLoopsJoins!=null){
//                Tuple t = idx.nestedLoopsJoins.get_next();
//                while (t!=null){
//                    t.print(attrType);
//                    t = idx.nestedLoopsJoins.get_next();
//                }
//
//            }

//            while (tpl!=null){
//                tpl.print(attrType);
//                tpl = idx.get_next();
//            }
            java.util.Iterator i = idx.getNext();

            while((i.hasNext())){
                Tuple tuple = (Tuple) i.next();
                tuple.print(idx.getOutputAttrType());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("\nRead statistics "+PCounter.rcounter);
        System.out.println("Write statistics "+PCounter.wcounter);
        System.out.println("------------------- TEST 1 completed ---------------------\n");

    }

    private void nlj() {
        System.out.println("------------------------ TEST 1 --------------------------");
        String fileName1 = null;
        String fileName2 = null;
        String c = null;
        System.out.println("Enter Outer file name ");
        c = GetStuff.getStringChoice();
        fileName1 = c;
        System.out.println("Enter Inner file name");
        c = GetStuff.getStringChoice();
        fileName2 = c;

        System.out.println("Choose operator");
        System.out.println("[0] =\n" +
                "[1] <\n" +
                "[2] >\n" +
                "[3] <\n" +
                "[4] >=\n" +
                " [5] >");
        int op= GetStuff.getChoice();
        short operation=0;
        if(op==0){
            operation = EQUAL;
        }else if(op == 1){
            operation = LESS;
        }else if(op==2){
            operation = GREATER;
        }else if(op==3){
            operation = LESSOREQUAL;
        }else if(op==4){
            operation = GREATEROREQUAL;
        }

        System.out.println("Enter the outer attribute");
        int attrIndex= GetStuff.getChoice();

        System.out.println("Enter the inner attribute");
        int attrIndex2 = GetStuff.getChoice();

//        int indexTypeIfExists = findIfIndexExists(fileName2, -1);
        int indexTypeIfExists = NO_INDEX; // means NO INDEX only heapfile
//
        CondExpr [] outFilter  = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        IndexJoin_CondExpr(outFilter);

        FldSpec [] Sprojection = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 1),
        };



        Iterator am1 = null;

        //FileScan(fileName1,attrType,attrStringSize,attrType.length, attrType.length,)
        try {
//            short operation = 0;

//            System.out.println(attrType2);
//            System.out.println(attrType);
//            System.out.println(attrSizes);
            IndexJoin idx = new IndexJoin(attrType, attrType.length, attrSizes,
                    attrType, attrType.length, attrSizes, 10, fileName1, fileName2, attrType.length-1, attrIndex,attrIndex2, NO_INDEX, operation);
            java.util.Iterator i = idx.getNext();

            while((i.hasNext())){
                Tuple tuple = (Tuple) i.next();
                tuple.print(idx.getOutputAttrType());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("\nRead statistics "+PCounter.rcounter);
        System.out.println("Write statistics "+PCounter.wcounter);
        System.out.println("------------------- NLJ Test completed ---------------------\n");

    }

    private void smj() {
        System.out.println("------------------------ SMJ TEST 1 --------------------------");
        String fileName1 = null;
        String fileName2 = null;
        String c = null;
        System.out.println("Enter Outer file name ");
        c = GetStuff.getStringChoice();
        fileName1 = c;
        System.out.println("Enter Inner file name");
        c = GetStuff.getStringChoice();
        fileName2 = c;

        System.out.println("Choose operator");
        System.out.println("[0] =\n" +
                "[1] <\n" +
                "[2] >\n" +
                "[3] <\n" +
                "[4] >=\n" +
                " [5] >");
        int op= GetStuff.getChoice();
        short operation=0;
        if(op==0){
            operation = EQUAL;
        }else if(op == 1){
            operation = LESS;
        }else if(op==2){
            operation = GREATER;
        }else if(op==3){
            operation = LESSOREQUAL;
        }else if(op==4){
            operation = GREATEROREQUAL;
        }

        System.out.println("Enter the outer attribute");
        int attrIndex= GetStuff.getChoice();
        System.out.println("Enter the inner attribute");
        int attrIndex2 = GetStuff.getChoice();

//        int indexTypeIfExists = findIfIndexExists(fileName2, -1);
        int indexTypeIfExists = NO_INDEX; // means NO INDEX only heapfile
//
        CondExpr [] outFilter  = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        IndexJoin_CondExpr(outFilter);

        FldSpec [] Sprojection = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 1),
        };



        Iterator am1 = null;

        //FileScan(fileName1,attrType,attrStringSize,attrType.length, attrType.length,)
        try {
//            short operation = 0;

//            System.out.println(attrType2);
//            System.out.println(attrType);
//            System.out.println(attrSizes);
            IndexJoin idx = new IndexJoin(attrType, attrType.length, attrSizes,
                    attrType, attrType.length, attrSizes, 10, fileName1, fileName2, attrType.length-1, attrIndex,attrIndex2, indexTypeIfExists, operation);
            java.util.Iterator i = idx.getNext();

            while((i.hasNext())){
                Tuple tuple = (Tuple) i.next();
                tuple.print(idx.getOutputAttrType());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("\nRead statistics "+PCounter.rcounter);
        System.out.println("Write statistics "+PCounter.wcounter);
        System.out.println("------------------- SMJ Test completed ---------------------\n");

    }


    public String createTempHeapFileForSkyline(String tableName) {
        Tuple t;
        int indexTypeIfExists = findIfIndexExists(tableName, -1);

        Heapfile tempHF = null;
        String tempHfName = null;

        if (status != OK) {
            return null;
        }
        rid = new RID();
        ArrayList<Tuple> allRows = new ArrayList<Tuple>();
        try {
            if (indexTypeIfExists == NO_INDEX) {
                try {
                    f = new Heapfile(tableName);
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Could not create heap file\n");
                    e.printStackTrace();
                }

                Scan scan = null;
                RID rid = new RID();

                try {
                    scan = f.openScan();
                } catch (Exception e) {
                    status = FAIL;
                    System.err.println("*** Error opening scan\n");
                    e.printStackTrace();
                }

                t = new Tuple();

                try {
                    t = scan.getNext(rid);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                while (t != null) {
                    try {
                        //                        tempHF.insertRecord(t.returnTupleByteArray());
                        t.setHdr((short) nColumns, attrType, attrSizes);
                        allRows.add(t);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    try {
                        t = scan.getNext(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                // clean up
                try {
                    scan.closescan();
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            } else if (indexTypeIfExists == CLUSTERED_BTREE) {
                bTreeClusteredFile = new BTreeClusteredFile(tableName, (short) nColumns, attrType, attrSizes);
                BTClusteredFileScan scan = null;
                try {
                    scan = bTreeClusteredFile.new_scan(null, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeyNotMatchException e) {
                    e.printStackTrace();
                } catch (IteratorException e) {
                    e.printStackTrace();
                } catch (ConstructPageException e) {
                    e.printStackTrace();
                } catch (PinPageException e) {
                    e.printStackTrace();
                } catch (UnpinPageException e) {
                    e.printStackTrace();
                }

                KeyDataEntry data = null;
                data = scan.get_next(rid);

                while (data != null) {
                    if (data != null) {
                        try {
                            t = ((Tuple) ((ClusteredLeafData) data.data).getData());
//                            tempHF.insertRecord(t.returnTupleByteArray());
                            allRows.add(t);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    try {
                        data = scan.get_next(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                bTreeClusteredFile.close();

            } else if (indexTypeIfExists == CLUSTERED_HASH) {
                hashFile = new ClusteredHashFile(tableName, (short) nColumns, attrType, attrSizes);
                ClusteredHashFileScan fscan = null;
                fscan = hashFile.newScan(null, null);
                t = null;
                t = fscan.getNextTuple(rid);

                while (t != null) {
                    try {
//                        tempHF.insertRecord(t.returnTupleByteArray());
                        allRows.add(t);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    try {
                        t = fscan.getNextTuple(rid);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                hashFile.close();
            }
//            sysDef.JavabaseBM.flushPages();
            try {
                tempHfName = Heapfile.getRandomHFName();
                tempHF = new Heapfile(tempHfName);
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Could not create heap file\n");
                e.printStackTrace();
            }

            for (int i = 0; i < allRows.size(); i++) {
                tempHF.insertRecord(allRows.get(i).returnTupleByteArray());
            }

            return tempHfName;
        } catch (Exception e) {
            status = FAIL;
            System.out.println("Failed to open heapfile for skyline!");
            e.printStackTrace();
        }
        return null;
    }

    public void getSecondTableAttrsAndType(String tableName) {
        // Read data and construct tuples
        FileScan fscan = null;

        FldSpec[] projections = new FldSpec[metaAttrTypes.length];

        for (int i = 0; i < metaAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        nColumns2 = 0;
        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int stringColumns = 0;
        while (t != null) {
            try {
                if (t.getIntFld(2) == AttrType.attrString) {
                    stringColumns++;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            nColumns2++;

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        attrType2 = new AttrType[nColumns2];
        attrSizes2 = new short[stringColumns];
        attrNames2 = new String[nColumns2];
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int i = 0, j = 0;
        while (t != null) {
            try {
                attrNames2[i] = t.getStrFld(1);
                attrType2[i] = new AttrType(t.getIntFld(2));
                if (t.getIntFld(2) == AttrType.attrString) {
                    attrSizes2[j] = (short) t.getIntFld(3);
                    j++;
                }

            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            i++;
            try {
                t = fscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        projlist2 = new FldSpec[nColumns2];

        for (i = 0; i < nColumns2; i++) {
            projlist2[i] = new FldSpec(rel, i + 1);
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            QueryInterface qi = new QueryInterface();
            qi.runTests();
        } catch (Exception e) {
            System.err.println("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }


}

/**
 * To get the integer off the command line
 */
class GetStuff {
    GetStuff() {
    }

    public static int getChoice() {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        int choice = -1;

        try {
            choice = Integer.parseInt(in.readLine());
        } catch (NumberFormatException e) {
            return -1;
        } catch (IOException e) {
            return -1;
        }

        return choice;
    }

    public static String getStringChoice() {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String choice;

        try {
            choice = in.readLine();
        } catch (NumberFormatException e) {
            return "Error";
        } catch (IOException e) {
            return "Error";
        }

        return choice;
    }

    public static void getReturn() {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        try {
            String ret = in.readLine();
        } catch (IOException e) {
        }
    }


}

class IndexDesc {
    public int indexType;
    public int attrIndex;

    public IndexDesc(int indexType, int attrIndex) {
        this.attrIndex = attrIndex;
        this.indexType = indexType;
    }
}

class RidTuplePair {
    public RID rid;
    public Tuple tuple;

//    public RidTuplePair(RID rid, Tuple tuple) {
//        this.rid = rid;
//        this.tuple = tuple;
//    }
}