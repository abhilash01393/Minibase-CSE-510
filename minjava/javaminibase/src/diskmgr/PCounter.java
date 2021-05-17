package diskmgr;

import java.util.HashSet;

public class PCounter {
    public static int rcounter;
    public static int wcounter;

    public static HashSet<Integer> rh = new HashSet<Integer>();
    public static HashSet<Integer> wh = new HashSet<Integer>();

    public static void initialize() {
        rh.clear();
        wh.clear();
        rcounter =0;
        wcounter =0;
    }
    public static void readIncrement() {
        rcounter++;
    }
    public static void writeIncrement() {
        wcounter++;
    }

    public static void printStats() {
        System.out.println("\nRead statistics "+rcounter);
        System.out.println("Write statistics "+wcounter);
    }
}