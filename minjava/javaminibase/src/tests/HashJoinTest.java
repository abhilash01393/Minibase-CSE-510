package tests;

import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.BlockNestedLoopsSky;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.HashJoin;

import java.io.IOException;

        import global.*;
        import heap.Heapfile;
        import heap.Tuple;
        import iterator.*;

        import java.io.IOException;


class HashJoinTestDriver extends TestDriver
        implements GlobalConst {

    // private static int NUM_RECORDS = data2.length;
    private static int LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private static int SORTPGNUM = 12;


    public HashJoinTestDriver() {
        super("HashJoinTest");
    }

    public boolean runTests() {

        System.out.println("\n" + "Hash Join Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 10000, NUMBUF, "Clock");

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
        test1();
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

        System.out.println("\n -- Testing BlockNestedLoopsSky on correlated tuples -- ");
        boolean status = OK;

        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrInteger);
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
        int inum1 =0;
        int inum2 =0;

        float fnum = 0.1567f;
        int count = 0;
        int j = 10;

        System.out.println("\n -- Generating correlated tuples -- ");
        int num_elements = 20;
        for (int i = 0; i < num_elements; i++) {
            // setting fields
            inum1 = i+1;
            inum2 = i;

            try {
                t.setIntFld(1, inum1);
                t.setIntFld(2, inum2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
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
        HashJoin blockNestedLoop = null;
        try {

//            blockNestedLoop = new HashJoin("test1.in", "test1.in", 0, 10);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        count = 0;
        t = null;

        try {
//            blockNestedLoop.get_next(false);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

//        while (t != null) {
//            try {
//                System.out.println("fld1 = " + t.getFloFld(1) + " fld2 = " + t.getFloFld(2));
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            count++;
//
//            try {
//                t = blockNestedLoop.get_next();
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//        }

        // clean up
//        try {
//            blockNestedLoop.close();
//        } catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }

        System.out.println("------------------- TEST 1 completed ---------------------\n");

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
        return "HashJoin";
    }
}

public class HashJoinTest {
    public static void main(String argv[]) {
        boolean sortstatus;

        HashJoinTestDriver sortt = new HashJoinTestDriver();

        sortstatus = sortt.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        } else {
            System.out.println("HashJoin tests completed successfully");
        }
    }
}