package iterator;

import bufmgr.*;
import global.AggType;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;


public class GroupByWithHash extends Iterator{

    private static Sort fileSort;
    private static AttrType[] attrType;
    private static FldSpec groupByAttr;
    private static int lenIn;
    private static boolean status = true;
    private static short[] attrSizes;
    private static AggType aggType;
    private static FldSpec[] aggList;
    public static List<Tuple> res;

    private Heapfile skylineHeapFile;
    private String skyLineFileName;

    private int tupleSize;

    private static float lastPolled = 0.0f;

    private int nPages;

    private Tuple _next;

    ListIterator<Tuple> iter;

    private static float[] aggrVals;
    private static float groupSize;
    private static float[] grpResults;
    private static Iterator am;

    public GroupByWithHash(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            Iterator am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages
    ){

        attrType = in1;
        lenIn = len_in1;
        attrSizes = t1_str_sizes;
        aggType = agg_type;
        nPages = n_pages;
        aggList = agg_list;
        groupByAttr = group_by_attr;
        am = am1;


        skyLineFileName = Heapfile.getRandomHFName();

        try {
            Tuple tuple_candidate = new Tuple();
            tuple_candidate.setHdr((short) this.lenIn, this.attrType, this.attrSizes);
            this.tupleSize = tuple_candidate.size();
        } catch (Exception e) {
            e.printStackTrace();
        }

        res = new ArrayList<>();
        iter = res.listIterator();

        try {
            skylineHeapFile = new Heapfile(skyLineFileName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        aggrVals = new float[aggList.length];
        for (int i = 0; i < aggList.length; i++) {
            if (aggType.aggType == AggType.AVG) {
                aggrVals[i] = 0.0f;
            } else if (aggType.aggType == AggType.MIN) {
                aggrVals[i] = Float.MAX_VALUE;
            } else {
                aggrVals[i] = -Float.MIN_VALUE;
            }
        }
        groupSize = 0;
        grpResults = new float[aggList.length];
        for (int i = 0; i < aggList.length; i++) {
            grpResults[i] = 0.0f;
        }

        try {
            fileSort = new Sort(attrType, (short) lenIn, attrSizes, am, group_by_attr.offset, new TupleOrder(TupleOrder.Descending), 4, 3);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static FileScan initialiseFileScan(String hf) {
        FileScan fscan = null;

        try {
            FldSpec[] projlist = new FldSpec[attrType.length];
            for (int i = 0; i < attrType.length; i++) {
                projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }
            fscan = new FileScan(hf, attrType, attrSizes, (short) attrType.length, attrType.length, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return fscan;
    }

    public List<Tuple> getNextGroup() throws IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException {
        res = new ArrayList<>();

        Tuple t = null;

        try {
            t = _next == null ? fileSort.get_next() : _next;

            if(t == null)
                return null;

            lastPolled = t.getFloFld(groupByAttr.offset);
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        RID rid;

        while(t != null && t.getFloFld(groupByAttr.offset) == lastPolled){
            Tuple outer = new Tuple(this.tupleSize);
            try {
                outer.setHdr((short) lenIn, attrType, attrSizes);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidTypeException e) {
                e.printStackTrace();
            } catch (InvalidTupleSizeException e) {
                e.printStackTrace();
            }
            outer.tupleCopy(t);

            groupSize += 1;

            for (int i = 0; i < aggList.length; i ++) {

                if (aggType.aggType == AggType.MIN) {
                    aggrVals[i] = Math.min(aggrVals[i], outer.getFloFld(aggList[i].offset));
                    grpResults[i] = aggrVals[i];
                } else if (aggType.aggType == AggType.MAX) {
                    aggrVals[i] = Math.max(aggrVals[i], outer.getFloFld(aggList[i].offset));
                    grpResults[i] = aggrVals[i];
                } else if (aggType.aggType == AggType.AVG) {
                    aggrVals[i] += outer.getFloFld(aggList[i].offset);
                    grpResults[i] = aggrVals[i] / groupSize;
                }
            }

            if (aggType.aggType == AggType.SKYLINE) {
                try {
                    rid = skylineHeapFile.insertRecord(outer.getTupleByteArray());
                } catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }

            lastPolled = outer.getFloFld(groupByAttr.offset);

            try {
                t = fileSort.get_next();
                _next = t;
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }
        }

        // Compute Skyline here
        if(aggType.aggType == AggType.SKYLINE) {
            try {
                computeSkyline(skyLineFileName, aggList, attrType, attrSizes, nPages - 3);
            } catch (Exception e) {
                e.printStackTrace();
            }
            createSkylineHeap();
            resetResult();
            return res;
        }

        Tuple result = new Tuple();
        AttrType[] tempTypes = new AttrType[aggList.length+1];
        tempTypes[0] = new AttrType(AttrType.attrReal);

        for (int i = 0; i < aggList.length; i++) {
            tempTypes[i+1] = new AttrType(AttrType.attrReal);
        }

        result.setHdr((short) (aggList.length + 1), tempTypes, new short[0]);

        int size = result.size();

        result = new Tuple(size);

        result.setHdr((short) (aggList.length + 1), tempTypes, new short[0]);
        if (attrType[groupByAttr.offset -1].attrType == AttrType.attrInteger) {
            result.setFloFld(1, lastPolled);
            int temp = result.getIntFld(1);
            result.setFloFld(1,  temp);
        } else {
            result.setFloFld(1, lastPolled);
        }

        for (int i = 0; i < aggList.length; i++) {
            if (attrType[aggList[i].offset -1].attrType == AttrType.attrInteger) {
                result.setFloFld(i+2,  grpResults[i]);
                int temp = result.getIntFld(i+2);
                result.setFloFld(i+2,  temp);
            } else {
                result.setFloFld(i+2,  grpResults[i]);
            }
        }

        res.add(result);
        resetResult();
        return res;
    }

    public Tuple get_next() throws IndexException, UnknowAttrType, SortException, UnknownKeyTypeException, JoinsException, HFDiskMgrException, InvalidTypeException, FieldNumberOutOfBoundException, TupleUtilsException, HFBufMgrException, InvalidSlotNumberException, FileAlreadyDeletedException, PageNotReadException, LowMemException, IOException, InvalidTupleSizeException, PredEvalException {
        if (!didFirst) {
            computeRes();
            iter = finalRes.listIterator();
        }
        if (iter.hasNext()) {
            return iter.next();
        } else {
            return null;
        }
    }

    boolean didFirst = false;
    List<Tuple> finalRes;

    public void computeRes() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, FieldNumberOutOfBoundException, FileAlreadyDeletedException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        List<List<Tuple>> resL = new ArrayList<>();
        getNextGroup();
        while (res.size() > 0) {
            List<Tuple> temp = new ArrayList<>(res);
            resL.add(temp);
            getNextGroup();
        }
        Collections.shuffle(resL);
        finalRes = new ArrayList<>();
        for (List<Tuple> listTup: resL) {
            finalRes.addAll(listTup);
        }
        didFirst = true;
    }

    public void resetResult(){
        aggrVals = new float[aggList.length];
        for (int i = 0; i < aggList.length; i++) {
            if (aggType.aggType == AggType.AVG) {
                aggrVals[i] = 0.0f;
            } else if (aggType.aggType == AggType.MIN) {
                aggrVals[i] = Float.MAX_VALUE;
            } else {
                aggrVals[i] = -Float.MIN_VALUE;
            }
        }
        groupSize = 0;
        grpResults = new float[aggList.length];
        for (int i = 0; i < aggList.length; i++) {
            grpResults[i] = 0.0f;
        }
    }

    public void createSkylineHeap(){
        // delete heap file
        try {
            skyLineFileName = Heapfile.getRandomHFName();
            skylineHeapFile = new Heapfile(skyLineFileName);
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException, SortException, JoinsException, IndexException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
        am.close();
        fileSort.close();
        fileSort = null;
        am = null;
    }

    public void computeSkyline(String skyline_grp_heap, FldSpec[] pref_list, AttrType[] attrType, short[] attrSize, int buffer) throws IOException, IndexException, SortException, JoinsException, InvalidTupleSizeException, InvalidTypeException {
        SortFirstSky sortFirstSky = null;

        int[] preference_list = new int[pref_list.length];
        for(int i=0; i<pref_list.length; i++){
            preference_list[i] = pref_list[i].offset;
        }
        try {
            FileScan fscan = initialiseFileScan(skyline_grp_heap);
            sortFirstSky = new SortFirstSky(attrType,
                    (short)attrType.length,
                    attrSize,
                    fscan,
                    skyline_grp_heap,
                    preference_list,
                    preference_list.length,
                    buffer);

            Tuple temp;
            try {
                temp = sortFirstSky.get_next();
                while (temp!=null) {
                    Tuple tup = new Tuple();
                    tup.setHdr((short) attrType.length, attrType, attrSize);
                    tup = new Tuple(tup.size());
                    tup.setHdr((short) attrType.length, attrType, attrSize);
                    tup.tupleCopy(temp);
                    res.add(tup);
                    temp = sortFirstSky.get_next();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (IOException | TupleUtilsException | SortException e) {
            e.printStackTrace();
        } finally {
            sortFirstSky.close();
        }
    }
}