package tests;

import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;

import java.io.IOException;


class SortFirstSkylineDriver extends TestDriver
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


    public SortFirstSkylineDriver() {
        super("skylinetest");
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
        System.out.println("------------------------ TEST 1 --------------------------");

        System.out.println("\n -- Testing SortFirstSky on correlated tuples -- ");
        boolean status = OK;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[0];

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        Tuple t = new Tuple();

        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("test1.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        float inum1 =0;
        float inum2 =0;

        float fnum = 0.1567f;
        int count = 0;
        int j = 10;

        System.out.println("\n -- Generating correlated tuples -- ");
        int num_elements = 20;
        for (int i = 0; i < num_elements; i++) {
            // setting fields
            inum1 = i+1;
            inum2 = i+1;

            try {
                t.setFloFld(1, inum1);
                t.setFloFld(2, inum2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            System.out.println("fld1 = " + inum1 + " fld2 = " + inum2);
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        FileScan fscan = null;

        // Sort "test3.in" on the int attribute (field 3) -- Ascending
        System.out.println("\n -- Skyline candidates -- ");

        try {
            fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int[] pref_list = new int[] {1,2};
        SortFirstSky sort = null;
        try {
            sort = new SortFirstSky(attrType, (short) 2, attrSize, fscan, "test1.in", pref_list, 2, 5);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        count = 0;
        t = null;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        while (t != null) {
            try {
                System.out.println("fld1 = " + t.getFloFld(1) + " fld2 = " + t.getFloFld(2));
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count++;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.out.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }


    protected boolean test2() {
        System.out.println("------------------------ TEST 2 --------------------------");

        System.out.println("\n -- Testing SortFirstSky on anti-correlated tuples -- ");
        boolean status = OK;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        short[] attrSize = new short[0];

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        Tuple t = new Tuple();

        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("test2.in");
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 2, attrType, attrSize);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        float inum1 =0;
        float inum2 =0;

        float fnum = 0.1567f;
        int count = 0;
        int j = 10;

        System.out.println("\n -- Generating anti-correlated tuples -- ");
        int num_elements = 7000;
        for (int i = 0; i < num_elements; i++) {
            // setting fields
            inum1 = i+1;
            inum2 = num_elements-i;

            try {
                t.setFloFld(1, inum1);
                t.setFloFld(2, inum2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            System.out.println("fld1 = " + inum1 + " fld2 = " + inum2);
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        FileScan fscan = null;

        // Sort "test3.in" on the int attribute (field 3) -- Ascending
        System.out.println("\n -- Skyline candidates -- ");

        try {
            fscan = new FileScan("test2.in", attrType, attrSize, (short) 2, 2, projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int[] pref_list = new int[] {1,2};
        SortFirstSky sort = null;
        try {
            sort = new SortFirstSky(attrType, (short) 2, attrSize, fscan, "test2.in", pref_list, 2, 5);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        count = 0;
        t = null;

        try {
            t = sort.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        while (t != null) {
            try {
                System.out.println("fld1 = " + t.getFloFld(1) + " fld2 = " + t.getFloFld(2));
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count++;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // clean up
        try {
            sort.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.out.println("------------------- TEST 2 completed ---------------------\n");

        return status;
    }

    protected boolean test3() {
        return true;
    }

    protected boolean test4() {
        return true;
    }

    protected boolean test5() {
        return true;    }

    protected boolean test6() {
        return true;
    }

    protected String testName() {
        return "Skyline";
    }
}

public class SortFirstSkylineTest {
    public static void main(String argv[]) {
        boolean sortstatus;

        SortFirstSkylineDriver sortt = new SortFirstSkylineDriver();

        sortstatus = sortt.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        } else {
            System.out.println("Skyline tests completed successfully");
        }
    }
}

