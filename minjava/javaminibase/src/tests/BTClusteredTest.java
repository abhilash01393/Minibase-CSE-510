package tests;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;


class BTClusteredDriver extends TestDriver
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


    public BTClusteredDriver() {
        super("btclusteredtest");
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
//        System.out.println("------------------------ TEST 1 --------------------------");
//
//        System.out.println("\n -- Testing BTClustered with Float key and data  -- ");
//        boolean status = OK;
//
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
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        int size = t.size();
//
//        t = new Tuple(size);
//        try {
//            t.setHdr((short) 2, attrType, attrSize);
//        } catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//        float inum1 = 0;
//        float inum2 = 0;
//
//        BTreeClusteredFile file = null;
//        try {
//            file = new BTreeClusteredFile("test1.in", AttrType.attrReal, 4, 1, (short) 2, attrType, attrSize);
//        } catch (GetFileEntryException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (AddFileEntryException e) {
//            e.printStackTrace();
//        }
//
//        ArrayList<ArrayList<Float>> list = new ArrayList<>();
//
//        System.out.println("\n -- Inserting shuffled tuples -- ");
//        int num_elements = 1000;
//        for (int i = 0; i < num_elements; i++) {
//            // setting fields
//            inum1 = i + 1;
//            inum2 = num_elements - i;
//
//            ArrayList<Float> elem = new ArrayList<>();
//            elem.add(inum1);
//            elem.add(inum2);
//
//            list.add(elem);
//        }
//
//        Collections.shuffle(list);
//
//        for (int i = 0; i < num_elements; i++) {
//
//            try {
//                t.setFloFld(1, list.get(i).get(0));
//                t.setFloFld(2, list.get(i).get(1));
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            try {
//                file.insert(new FloatKey(list.get(i).get(0)), t);
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            System.out.println("fld1 = " + list.get(i).get(0) + " fld2 = " + list.get(i).get(1));
//        }
//        System.out.println("\n -- Scanning from 22.0 to 998.0, results should be sorted ");
//        FloatKey key1 = new FloatKey(22.0F);
//        FloatKey key2 = new FloatKey(998.0F);
//        BTClusteredFileScan scan = null;
//        try {
//
//            scan = file.new_scan(key1, key2);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (KeyNotMatchException e) {
//            e.printStackTrace();
//        } catch (IteratorException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (PinPageException e) {
//            e.printStackTrace();
//        } catch (UnpinPageException e) {
//            e.printStackTrace();
//        }
//        KeyDataEntry data = null;
//        try {
//            data = scan.get_next();
//            if (data != null) {
//                try {
//                    ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        } catch (ScanIteratorException e) {
//            e.printStackTrace();
//        }
//
//        while (data != null) {
//            try {
//                data = scan.get_next();
//                if (data != null) {
//                    try {
//                        ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//
//            } catch (ScanIteratorException e) {
//                e.printStackTrace();
//            }
//
//        }
//
//        System.out.println("------------------- TEST 1 completed ---------------------\n");

        return false;
    }

    public static String getRandomStr() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 6) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }

    protected boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");
        System.out.println("\n -- Testing BTClustered with String key and data -- ");
        boolean status = OK;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[2];
        attrSize[0] = 32;
        attrSize[1] = 32;

        Tuple t = new Tuple();

        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        int inum1 = 0;
        int inum2 = 0;

        BTreeClusteredFile file = null;
        try {
            file = new BTreeClusteredFile("test2.in", AttrType.attrString, 32, 1, 1, (short) 2, attrType, attrSize);
            file.close();
            file = new BTreeClusteredFile("test2.in", (short) 2, attrType, attrSize);
        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AddFileEntryException e) {
            e.printStackTrace();
        } catch (PageUnpinnedException e) {
            e.printStackTrace();
        } catch (InvalidFrameNumberException e) {
            e.printStackTrace();
        } catch (HashEntryNotFoundException e) {
            e.printStackTrace();
        } catch (ReplacerException e) {
            e.printStackTrace();
        } catch (PinPageException e) {
            e.printStackTrace();
        }

        ArrayList<ArrayList<Integer>> list = new ArrayList<>();

        System.out.println("\n -- Generating random tuples -- ");
        int num_elements = 1000;
        for (int i = 0; i < num_elements; i++) {
            // setting fields
            inum1 = i + 1;
            inum2 = num_elements - i;

            ArrayList<Integer> elem = new ArrayList<>();
            elem.add(inum1);
            elem.add(inum2);

            list.add(elem);
        }

        Collections.shuffle(list);

        for (int i = 0; i < num_elements; i++) {
            String randStr = getRandomStr();
            try {
                t.setStrFld(1, randStr);
                t.setStrFld(2, "Musab" + list.get(i).get(1));
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                file.insert(new StringKey(t.getStrFld(1)), t);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            System.out.println("fld1 = " + randStr+ " fld2 = " + "Musab" + list.get(i).get(1));
        }
        System.out.println("\n -- Scanning whole index, results should be sorted ");
        BTClusteredFileScan scan = null;
        try {
            scan = file.new_scan(null, null);
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
        RID tempRid = null;
        try {
             tempRid = scan.get_next_rid();
//            System.out.println(tempRid.slotNo);
        } catch (ScanIteratorException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        }

        while (tempRid != null) {
            try {
                tempRid = scan.get_next_rid();
//                System.out.println(tempRid.slotNo);
            } catch (ScanIteratorException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidTypeException e) {
                e.printStackTrace();
            } catch (InvalidSlotNumberException e) {
                e.printStackTrace();
            } catch (InvalidTupleSizeException e) {
                e.printStackTrace();
            }

        }

        System.out.println("------------------- TEST 2 completed ---------------------\n");
        return status;
    }

    protected boolean test3() {
//        System.out.println("------------------------ TEST 3 --------------------------");
//
//        System.out.println("\n -- Testing BTClustered Deletion  -- ");
//        boolean status = OK;
//
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
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        int size = t.size();
//
//        t = new Tuple(size);
//        try {
//            t.setHdr((short) 2, attrType, attrSize);
//        } catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//        float inum1 = 0;
//        float inum2 = 0;
//
//        BTreeClusteredFile file = null;
//        try {
//            file = new BTreeClusteredFile("test3.in", AttrType.attrReal, 4, 1, (short) 2, attrType, attrSize);
//        } catch (GetFileEntryException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (AddFileEntryException e) {
//            e.printStackTrace();
//        }
//
//        ArrayList<ArrayList<Float>> list = new ArrayList<>();
//
//        System.out.println("\n -- Inserting random tuples -- ");
//        int num_elements = 1000;
//        for (int i = 0; i < num_elements; i++) {
//            // setting fields
//            inum1 = i + 1;
//            inum2 = num_elements - i;
//
//            ArrayList<Float> elem = new ArrayList<>();
//            elem.add(inum1);
//            elem.add(inum2);
//
//            list.add(elem);
//        }
//
//        Collections.shuffle(list);
//
//        for (int i = 0; i < num_elements; i++) {
//
//            try {
//                t.setFloFld(1, list.get(i).get(0));
//                t.setFloFld(2, list.get(i).get(1));
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            try {
//                file.insert(new FloatKey(list.get(i).get(0)), t);
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            System.out.println("fld1 = " + list.get(i).get(0) + " fld2 = " + list.get(i).get(1));
//        }
//        FloatKey key1 = new FloatKey(22.0F);
//        FloatKey key2 = new FloatKey(998.0F);
//        BTClusteredFileScan scan = null;
//        try {
//
//            scan = file.new_scan(key1, key2);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (KeyNotMatchException e) {
//            e.printStackTrace();
//        } catch (IteratorException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (PinPageException e) {
//            e.printStackTrace();
//        } catch (UnpinPageException e) {
//            e.printStackTrace();
//        }
//        KeyDataEntry data = null;
//        try {
//            data = scan.get_next();
//            if (data != null) {
//                try {
//                    ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        } catch (ScanIteratorException e) {
//            e.printStackTrace();
//        }
//
//        while (data != null) {
//            try {
//                data = scan.get_next();
//                if (data != null) {
//                    try {
//                        ((Tuple) ((ClusteredLeafData) data.data).getData()).print(attrType);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//
//            } catch (ScanIteratorException e) {
//                e.printStackTrace();
//            }
//
//        }
//
//        System.out.println("\n -- Deleting 500 tuples -- ");
//        for (int i = 0; i < num_elements-500; i++) {
//            // setting fields
//            inum1 = i+1;
//            inum2 = num_elements-i;
//
//            try {
//                t.setFloFld(1, inum1);
//                t.setFloFld(2, inum2);
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            try {
//                file.Delete(new FloatKey(inum1), t);
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            System.out.println("fld1 = " + inum1 + " fld2 = " + inum2);
//        }
//        System.out.println("\n -- Scanning from 22.0 to 998.0, after deletion ");
//        try {
//            scan = file.new_scan(key1, key2);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (KeyNotMatchException e) {
//            e.printStackTrace();
//        } catch (IteratorException e) {
//            e.printStackTrace();
//        } catch (ConstructPageException e) {
//            e.printStackTrace();
//        } catch (PinPageException e) {
//            e.printStackTrace();
//        } catch (UnpinPageException e) {
//            e.printStackTrace();
//        }
//        try {
//            data = scan.get_next();
//            if (data!= null) {
//                try {
//                    ((Tuple)((ClusteredLeafData)data.data).getData()).print(attrType);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }}
//        } catch (ScanIteratorException e) {
//            e.printStackTrace();
//        }
//
//        while (data != null) {
//            try {
//                data = scan.get_next();
//                if (data!= null) {
//                    try {
//                        ((Tuple)((ClusteredLeafData)data.data).getData()).print(attrType);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//
//            } catch (ScanIteratorException e) {
//                e.printStackTrace();
//            }
//
//        }
//
//
//
//
//        System.out.println("------------------- TEST 3 completed ---------------------\n");

        return true;
    }

    protected boolean test4() {
        return true;
    }

    protected boolean test5() {
        return true;
    }

    protected boolean test6() {
        return true;
    }

    protected String testName() {
        return "BTClustered";
    }
}

public class BTClusteredTest {
    public static void main(String argv[]) {
        boolean sortstatus;

        BTClusteredDriver btreeClustered = new BTClusteredDriver();

        sortstatus = btreeClustered.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        } else {
            System.out.println("BTClustered tests completed successfully");
        }
    }
}

