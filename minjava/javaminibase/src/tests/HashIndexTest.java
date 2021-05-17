package tests;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
//import btree.*;
import bufmgr.*;
import hash.*;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import hash.ClusteredHashFile;
import heap.InvalidSlotNumberException;
import heap.Tuple;
import heap.*;
import java.io.IOException;
import java.util.ArrayList;


class HashIndexDriver extends TestDriver
        implements GlobalConst {

    private static String data1[] = {
            "raghu", "xbao", "cychan", "leela", "ketola", "soma", "ulloa",
            "dhanoa", "dsilva", "kurniawa", "dissoswa", "waic", "susanc", "kinc",
            "marc", "scottc", "yuc", "ireland", "rathgebe", "joyce", "daode",
            "yuvadee", "he", "huxtable", "muerle", "flechtne", "thiodore", "jhowe",
            "frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch",
            "binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
            "chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel",
            "newell", "schiesl", "neuman", "heitzman", "wan", "gunawan", "djensen",
            "juei-wen", "josephin", "harimin", "xin", "zmudzin", "feldmann",
            "joon", "wawrzon", "yi-chun", "wenchao", "seo", "karsono", "dwiyono",
            "ginther", "keeler", "peter", "lukas", "edwards", "mirwais", "schleis",
            "haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
            "honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey",
            "awny", "savoy", "meltz"};

    private static String data2[] = {
            "andyw", "awny", "azat", "barthel", "binh", "bloch", "bradw",
            "chingju", "chui", "chung-pi", "cychan", "dai", "daode", "dhanoa",
            "dissoswa", "djensen", "dsilva", "dwiyono", "edwards", "evgueni",
            "feldmann", "flechtne", "frankief", "ginther", "gray", "guangshu",
            "gunawan", "hai", "handi", "harimin", "haris", "he", "heitzman",
            "honghu", "huxtable", "ireland", "jhowe", "joon", "josephin", "joyce",
            "jsong", "juei-wen", "karsono", "keeler", "ketola", "kinc", "kurniawa",
            "leela", "lukas", "mak", "marc", "markert", "meltz", "meyers",
            "mirwais", "muerle", "muthiah", "neuman", "newell", "peter", "raghu",
            "randal", "rathgebe", "robert", "savoy", "schiesl", "schleis",
            "scottc", "seo", "shi", "shun-kit", "siddiqui", "soma", "sonthi",
            "sungk", "susanc", "tak", "thiodore", "ulloa", "vharvey", "waic",
            "wan", "wawrzon", "wenchao", "wlau", "xbao", "xiaoming", "xin",
            "yi-chun", "yiching", "yuc", "yung", "yuvadee", "zmudzin"};

    private static int NUM_RECORDS = data2.length;
    private static int LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private static int SORTPGNUM = 12;


    public HashIndexDriver() {
        super("hashindextest");
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 10000, 10000, "Clock");

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
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.println("\n" + "..." + testName() + " tests ");
        System.out.println(_pass == OK ? "completely successfully" : "failed");
        System.out.println(".\n\n");

        return _pass;
    }

    protected boolean test1() {
        return true;
    }


    protected boolean test2() {
        return true;
    }

    protected boolean test3() {
//        UnclusteredHashFile blah1 = null;
//        try {
//            UnclusteredHashFile blah = new UnclusteredHashFile("lol.in", AttrType.attrInteger, 4, 75);
//            blah.close();
//            blah1 = new UnclusteredHashFile("lol.in");
//
//        } catch (GetFileEntryException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (AddFileEntryException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException | InvalidSlotNumberException e) {
//            e.printStackTrace();
//        }
//
//        for (int i = 0; i < 1000; i++) {
//            try {
//                IntegerKey key = new IntegerKey(i);
//                blah1.insertRecord(key, new RID(new PageId(1), 1 ));
//                blah1.insertRecord(key, new RID(new PageId(1), 1 ));
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ConstructPageException e) {
//                e.printStackTrace();
//            } catch (InvalidSlotNumberException | PageUnpinnedException | InvalidFrameNumberException | HashEntryNotFoundException | ReplacerException | PagePinnedException | PageNotFoundException | BufMgrException | HashOperationException e) {
//                e.printStackTrace();
//            }
//        }
//
//        IntegerKey key = new IntegerKey(682);
//        IntegerKey key1 = new IntegerKey(900);
//        UnclusteredHashFileScan scan = blah1.newScan(key, key1);
//
//        UnclusteredHashRecord record = null;
//        try {
//            record = scan.getNextRecord();
//            System.out.println(record.toString());
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException e) {
//            e.printStackTrace();
//        }
//
//        while (record != null) {
//            try {
//                record = scan.getNextRecord();
//                System.out.println(record.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (PageUnpinnedException e) {
//                e.printStackTrace();
//            } catch (InvalidFrameNumberException e) {
//                e.printStackTrace();
//            } catch (HashEntryNotFoundException e) {
//                e.printStackTrace();
//            } catch (ReplacerException e) {
//                e.printStackTrace();
//            }
//        }


//
//        for (int i = 0; i < 1000; i++) {
//            try {
//                IntegerKey key = new IntegerKey(i);
//                blah1.insertRecord(key, new RID(new PageId(1), 1 ));
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ConstructPageException e) {
//                e.printStackTrace();
//            } catch (InvalidSlotNumberException e) {
//                e.printStackTrace();
//            } catch (InvalidFrameNumberException e) {
//                e.printStackTrace();
//            } catch (PageNotFoundException e) {
//                e.printStackTrace();
//            } catch (PageUnpinnedException e) {
//                e.printStackTrace();
//            } catch (HashEntryNotFoundException e) {
//                e.printStackTrace();
//            } catch (HashOperationException e) {
//                e.printStackTrace();
//            } catch (ReplacerException e) {
//                e.printStackTrace();
//            } catch (PagePinnedException e) {
//                e.printStackTrace();
//            } catch (BufMgrException e) {
//                e.printStackTrace();
//            }
//        }
//
//        try {
//            blah1.close();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException e) {
//            e.printStackTrace();
//        }
//        try {
//            blah1 = new UnclusteredHashFile("lol.in");
//        } catch (GetFileEntryException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (AddFileEntryException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (InvalidSlotNumberException e) {
//            e.printStackTrace();
//        }
//        try {
//            blah1.printIndex();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InvalidTupleSizeException e) {
//            e.printStackTrace();
//        } catch (InvalidTypeException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            IntegerKey key = new IntegerKey(10001);
//            blah1.deleteRecord(key, new RID(new PageId(1), 1 ));
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InvalidSlotNumberException e) {
//            e.printStackTrace();
//        } catch (InvalidTypeException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (TupleUtilsException e) {
//            e.printStackTrace();
//        } catch (UnknowAttrType unknowAttrType) {
//            unknowAttrType.printStackTrace();
//        } catch (InvalidTupleSizeException e) {
//            e.printStackTrace();
//        }
//        System.out.println();
//        try {
//            blah1.printIndex();
//        } catch (IOException | InvalidTupleSizeException | InvalidTypeException e) {
//            e.printStackTrace();
//        }
//        System.out.println();
//
//        for (int i = 0; i < 500; i++) {
//            try {
//                IntegerKey key = new IntegerKey(i);
//                blah1.deleteRecord(key, new RID(new PageId(1), 1 ));
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InvalidSlotNumberException | ConstructPageException | InvalidTypeException | UnknowAttrType | TupleUtilsException | InvalidTupleSizeException e) {
//                e.printStackTrace();
//            }
//        }
//        try {
//            blah1.printIndex();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InvalidTupleSizeException e) {
//            e.printStackTrace();
//        } catch (InvalidTypeException e) {
//            e.printStackTrace();
//        }

        return true;
    }

    protected boolean test4() {
//        AttrType[] attrType = new AttrType[2];
//        attrType[0] = new AttrType(AttrType.attrReal);
//        attrType[1] = new AttrType(AttrType.attrReal);
//        short[] attrSize = new short[0];
//
//        Tuple t = new Tuple();
//
//        try {
//            t.setHdr((short) 2, attrType, attrSize);
//        } catch (Exception e) {
//            System.err.println("*** error in Tuple.setHdr() ***");
//            e.printStackTrace();
//        }
//
//        float inum1 = 1.0F;
//        float inum2 = 1.0F;
//
//        try {
//                t.setFloFld(1, inum1);
//                t.setFloFld(2, inum2);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        HashFile blah1 = null;
//        try {
//            HashFile blah = new ClusteredHashFile("lol.in", 75, AttrType.attrString, 10, (short) 2, attrType, attrSize);
//            blah.close();
//            blah1 = new ClusteredHashFile("lol.in", (short) 2, attrType, attrSize);
//
//        } catch (GetFileEntryException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (AddFileEntryException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException | InvalidSlotNumberException e) {
//            e.printStackTrace();
//        }
//
//        ArrayList<String> stringList = new ArrayList<String>();
//
//        for (int i = 0; i < 1000; i++) {
//            try {
//                String blash = Heapfile.getRandomHFName();
////                System.out.println(blash);
//                stringList.add(blash);
//                StringKey key = new StringKey(blash);
//                blah1.insertRecord(key, new ClusteredHashRecord(key, t));
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ConstructPageException e) {
//                e.printStackTrace();
//            } catch (InvalidSlotNumberException e) {
//                e.printStackTrace();
//            }
//        }
//
////        for (int i = 0; i < 1000; i++) {
////            try {
////
////                IntegerKey key = new IntegerKey(i);
////                blah1.insertRecord(key, new ClusteredHashRecord(key, t));
////            } catch (IOException e) {
////                e.printStackTrace();
////            } catch (ConstructPageException e) {
////                e.printStackTrace();
////            } catch (InvalidSlotNumberException e) {
////                e.printStackTrace();
////            }
////        }
//
//
//        try {
//            blah1.close();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException e) {
//            e.printStackTrace();
//        }
//        try {
//            blah1 = new ClusteredHashFile("lol.in", (short) 2, attrType, attrSize);
//        } catch (GetFileEntryException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (AddFileEntryException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (InvalidSlotNumberException e) {
//            e.printStackTrace();
//        }
//        try {
//            blah1.printIndex();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InvalidTupleSizeException e) {
//            e.printStackTrace();
//        } catch (InvalidTypeException e) {
//            e.printStackTrace();
//        }
//
////
////        for (int i = 0; i < 2000; i++) {
////            try {
////                IntegerKey key = new IntegerKey(1);
////                blah1.deleteRecord(key, new ClusteredHashRecord(key, t));
////            } catch (IOException e) {
////                e.printStackTrace();
////            } catch (InvalidSlotNumberException e) {
////                e.printStackTrace();
////            } catch (InvalidTypeException e) {
////                e.printStackTrace();
////            } catch (TupleUtilsException e) {
////                e.printStackTrace();
////            } catch (UnknowAttrType unknowAttrType) {
////                unknowAttrType.printStackTrace();
////            } catch (InvalidTupleSizeException | ConstructPageException e) {
////                e.printStackTrace();
////            }
////        }
////        try {
////            blah1.printIndex();
////        } catch (IOException e) {
////            e.printStackTrace();
////        } catch (InvalidTupleSizeException e) {
////            e.printStackTrace();
////        } catch (InvalidTypeException e) {
////            e.printStackTrace();
////        }
////
////        for (int i = 0; i < 500; i++) {
////            try {
////                IntegerKey key = new IntegerKey(i);
////                blah1.deleteRecord(key, new ClusteredHashRecord(key.getKey(), t));
////            } catch (IOException e) {
////                e.printStackTrace();
////            } catch (InvalidSlotNumberException e) {
////                e.printStackTrace();
////            } catch (InvalidTypeException e) {
////                e.printStackTrace();
////            } catch (TupleUtilsException e) {
////                e.printStackTrace();
////            } catch (UnknowAttrType unknowAttrType) {
////                unknowAttrType.printStackTrace();
////            } catch (InvalidTupleSizeException | ConstructPageException e) {
////                e.printStackTrace();
////            }
////        }
////        try {
////            blah1.printIndex();
////        } catch (IOException | InvalidTupleSizeException | InvalidTypeException e) {
////            e.printStackTrace();
////        }
        return true;
    }

    protected boolean test5() {
        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[0];

        Tuple t = new Tuple();

        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        float inum1 = 1.0F;
        float inum2 = 1.0F;

        try {
            t.setFloFld(1, inum1);
            t.setFloFld(2, inum2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        ClusteredHashFile blah1 = null;
        try {
            ClusteredHashFile blah = new ClusteredHashFile("lol.in", 75, AttrType.attrString, 10, (short) 2, attrType, attrSize);
            blah.close();
            blah1 = new ClusteredHashFile("lol.in", (short) 2, attrType, attrSize);

        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AddFileEntryException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (PageUnpinnedException e) {
            e.printStackTrace();
        } catch (InvalidFrameNumberException e) {
            e.printStackTrace();
        } catch (HashEntryNotFoundException e) {
            e.printStackTrace();
        } catch (ReplacerException | InvalidSlotNumberException e) {
            e.printStackTrace();
        }

//        try {
//            StringKey key = new StringKey("MUSAB");
//            blah1.insertRecord(key, new ClusteredHashRecord(key, t));
//            blah1.insertRecord(key, new ClusteredHashRecord(key, t));
////            blah1.insertRecord(new StringKey("MUSAB"), new ClusteredHashRecord(new StringKey("MUSAB"), t));
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (InvalidSlotNumberException e) {
//            e.printStackTrace();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException e) {
//            e.printStackTrace();
//        } catch (PageNotFoundException e) {
//            e.printStackTrace();
//        } catch (PagePinnedException e) {
//            e.printStackTrace();
//        } catch (HashOperationException e) {
//            e.printStackTrace();
//        } catch (BufMgrException e) {
//            e.printStackTrace();
//        }

        ArrayList<String> stringList = new ArrayList<String>();

        for (int i = 0; i < 1000; i++) {
            try {
                String blash = Heapfile.getRandomHFName();
//                System.out.println(blash);
                stringList.add(blash);
                StringKey key = new StringKey(blash);
//                PCounter.initialize();
                blah1.insertRecord(key, new ClusteredHashRecord(key, t));
//                SystemDefs.JavabaseBM.flushPages();
//                PCounter.printStats();
//                blah1.insertRecord(key, new ClusteredHashRecord(key, t));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ConstructPageException e) {
                e.printStackTrace();
            } catch (InvalidSlotNumberException | PageUnpinnedException | InvalidFrameNumberException | HashEntryNotFoundException | ReplacerException e) {
                e.printStackTrace();
            } catch (PageNotFoundException e) {
                e.printStackTrace();
            } catch (PagePinnedException e) {
                e.printStackTrace();
            } catch (HashOperationException e) {
                e.printStackTrace();
            } catch (BufMgrException e) {
                e.printStackTrace();
            }
        }

        try {
            blah1.printIndex();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        }


        for (int i = 0; i < 1000; i++) {
            try {
//                String blash = Heapfile.getRandomHFName();
//                System.out.println(blash);
//                stringList.add(blash);
                StringKey key = new StringKey(stringList.get(i));
                blah1.insertRecord(key, new ClusteredHashRecord(key, t));
//                blah1.insertRecord(key, new ClusteredHashRecord(key, t));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ConstructPageException e) {
                e.printStackTrace();
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            } catch (InvalidFrameNumberException e) {
                e.printStackTrace();
            } catch (PageNotFoundException e) {
                e.printStackTrace();
            } catch (PageUnpinnedException e) {
                e.printStackTrace();
            } catch (HashEntryNotFoundException e) {
                e.printStackTrace();
            } catch (HashOperationException e) {
                e.printStackTrace();
            } catch (ReplacerException e) {
                e.printStackTrace();
            } catch (PagePinnedException e) {
                e.printStackTrace();
            } catch (BufMgrException e) {
                e.printStackTrace();
            }
        }


//        for (int i = 0; i < 1000; i++) {
//            try {
//
//                IntegerKey key = new IntegerKey(i);
//                blah1.insertRecord(key, new ClusteredHashRecord(key, t));
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ConstructPageException e) {
//                e.printStackTrace();
//            } catch (InvalidSlotNumberException e) {
//                e.printStackTrace();
//            }
//        }
//
//
//        try {
//            blah1.close();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException e) {
//            e.printStackTrace();
//        }
//
//        ClusteredHashFileScan scan = null;
//        try {
//            blah1 = new ClusteredHashFile("lol.in", (short) 2, attrType, attrSize);
//            StringKey key = new StringKey("MUSAB");
//            StringKey key1 = new StringKey("MUSAB");
//            scan = blah1.newScan(key, null);
//        } catch (GetFileEntryException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (AddFileEntryException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (InvalidSlotNumberException e) {
//            e.printStackTrace();
//        }
//        PCounter.initialize();
//        Tuple t1 = null;
//        try {
//            t1 = scan.getNextTuple();
//            t1.print(attrType);
////            ClusteredDataPage dataPage = new ClusteredDataPage(new PageId(rid.pageNo.pid));
////            Tuple t1 = dataPage.getTupleFromSlot(rid.slotNo);
////            t1.setHdr((short) 2, attrType, attrSize);
////            t1.print(attrType);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InvalidTupleSizeException e) {
//            e.printStackTrace();
//        } catch (InvalidTypeException e) {
//            e.printStackTrace();
//        } catch (PageUnpinnedException e) {
//            e.printStackTrace();
//        } catch (InvalidFrameNumberException e) {
//            e.printStackTrace();
//        } catch (HashEntryNotFoundException e) {
//            e.printStackTrace();
//        } catch (ReplacerException e) {
//            e.printStackTrace();
//        } catch (PageNotFoundException e) {
//            e.printStackTrace();
//        } catch (PagePinnedException e) {
//            e.printStackTrace();
//        } catch (HashOperationException e) {
//            e.printStackTrace();
//        } catch (BufMgrException e) {
//            e.printStackTrace();
//        }
//
//        while (t1 != null) {
//            try {
//                t1 = scan.getNextTuple();
//                if (t1 != null) {
//                    t1.print(attrType);
//                }
//
////                ClusteredDataPage dataPage = new ClusteredDataPage(new PageId(rid.pageNo.pid));
////                Tuple t1 = dataPage.getTupleFromSlot(rid.slotNo);
////                t1.setHdr((short) 2, attrType, attrSize);
////                t1.print(attrType);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InvalidTupleSizeException e) {
//                e.printStackTrace();
//            } catch (InvalidTypeException e) {
//                e.printStackTrace();
//            } catch (PageUnpinnedException e) {
//                e.printStackTrace();
//            } catch (InvalidFrameNumberException e) {
//                e.printStackTrace();
//            } catch (HashEntryNotFoundException e) {
//                e.printStackTrace();
//            } catch (ReplacerException e) {
//                e.printStackTrace();
//            } catch (PageNotFoundException e) {
//                e.printStackTrace();
//            } catch (PagePinnedException e) {
//                e.printStackTrace();
//            } catch (HashOperationException e) {
//                e.printStackTrace();
//            } catch (BufMgrException e) {
//                e.printStackTrace();
//            }
//        }
//        PCounter.printStats();
//
//        ClusteredHashRecord record = null;
//        try {
//            record = scan.getNextRecord();
//            System.out.println(record.getKey().toString() +",");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        while (record != null) {
//            try {
//                record = scan.getNextRecord();
//                System.out.println(record.getKey().toString()+",");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//
//        for (int i = 0; i < 1000; i++) {
//            try {
//                StringKey key = new StringKey(stringList.get(i));
//                blah1.deleteRecord(key, t);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InvalidSlotNumberException e) {
//                e.printStackTrace();
//            } catch (InvalidTypeException e) {
//                e.printStackTrace();
//            } catch (TupleUtilsException e) {
//                e.printStackTrace();
//            } catch (UnknowAttrType unknowAttrType) {
//                unknowAttrType.printStackTrace();
//            } catch (InvalidTupleSizeException | ConstructPageException e) {
//                e.printStackTrace();
//            }
//        }
//        try {
//            blah1.printIndex();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InvalidTupleSizeException e) {
//            e.printStackTrace();
//        } catch (InvalidTypeException e) {
//            e.printStackTrace();
//        }
//
//        for (int i = 0; i < 500; i++) {
//            try {
//                IntegerKey key = new IntegerKey(i);
//                blah1.deleteRecord(key, new ClusteredHashRecord(key.getKey(), t));
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InvalidSlotNumberException e) {
//                e.printStackTrace();
//            } catch (InvalidTypeException e) {
//                e.printStackTrace();
//            } catch (TupleUtilsException e) {
//                e.printStackTrace();
//            } catch (UnknowAttrType unknowAttrType) {
//                unknowAttrType.printStackTrace();
//            } catch (InvalidTupleSizeException | ConstructPageException e) {
//                e.printStackTrace();
//            }
//        }
//        try {
//            blah1.printIndex();
//        } catch (IOException | InvalidTupleSizeException | InvalidTypeException e) {
//            e.printStackTrace();
//        }
        return true;
    }

    protected boolean test6() {
        return true;
    }

    protected String testName() {
        return "Skyline";
    }
}

public class HashIndexTest {
    public static void main(String argv[]) {
        boolean sortstatus;

        HashIndexDriver hash = new HashIndexDriver();

        sortstatus = hash.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        } else {
            System.out.println("Skyline tests completed successfully");
        }
    }
}

