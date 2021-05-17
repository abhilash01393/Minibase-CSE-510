package iterator;

import global.AttrType;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import static tests.TestDriver.OK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import btree.BTClusteredFileScan;
import btree.BTreeClusteredFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import btree.ClusteredLeafData;

public class TopK_NRAJoin {

    private AttrType[] in1, in2;
	private int len_in1, len_in2;
    private short[] t1_str_sizes, t2_str_sizes;
    private FldSpec joinAttr1, joinAttr2, mergeAttr1, mergeAttr2;
	private String relationName1, relationName2;
    private int k;
    private String oTable;
    boolean val;
    int i = 1;
    private int n_pages;
    private BTreeClusteredFile[] file = new BTreeClusteredFile[2];
    Object ub1, ub2;
    private KeyDataEntry[] data = new KeyDataEntry[2];
    private BTClusteredFileScan[] scan;
    boolean status = OK;
    List<Tuple> result = new ArrayList<>(); 
    private Heapfile f;
    int nColumns;
    short[] oAttrSize;
    AttrType[] oAttrTypes;
    String[] oAttrName;
    Tuple tuple1;
    short size;
    int type;
    private Map<Object, TupleList> tList = new HashMap<>();

    public TopK_NRAJoin(){

    }

    public TopK_NRAJoin(AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1, FldSpec mergeAttr1, AttrType[] in2, int len_in2, 
        short[] t2_str_sizes, FldSpec joinAttr2, FldSpec mergeAttr2, String relationName1, String relationName2, int k,int n_pages, String oTable){
        this.relationName1 = relationName1;
        this.relationName2 = relationName2;
        this.in1 = in1;
        this.in2 = in2;
        this.len_in1 = len_in1;
        this.len_in2 = len_in2;
        this.t1_str_sizes = t1_str_sizes;
        this.t2_str_sizes = t2_str_sizes;
        this.joinAttr1 = joinAttr1;
        this.joinAttr2 = joinAttr2;
        this.mergeAttr1 = mergeAttr1;
        this.mergeAttr2 = mergeAttr2;
        this.k = k;
        this.n_pages = n_pages; 
        this.oTable = oTable;    
    }

    public void computeTopK_NRA(AttrType[] oAttrTypes, short[] oAttrSize, String[] oAttrName){
        try {
            file[0] = new BTreeClusteredFile(relationName1, (short) in1.length, in1, t1_str_sizes);
            file[1] = new BTreeClusteredFile(relationName2, (short) in2.length, in2, t2_str_sizes);
        } catch (PinPageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        }

        this.oAttrTypes = oAttrTypes;
        this.oAttrSize = oAttrSize;
        this.oAttrName = oAttrName;

        nColumns = oAttrTypes.length;

        tuple1 = new Tuple();
            try {
                tuple1.setHdr((short) nColumns, oAttrTypes, oAttrSize);
            } catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                e.printStackTrace();
            }

            size = tuple1.size();

        if(!oTable.equals("null")){
            try {
                f = new Heapfile(oTable);
            } catch (HFException e) {
                e.printStackTrace();
            } catch (HFBufMgrException e) {
                e.printStackTrace();
            } catch (HFDiskMgrException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        type = in1[mergeAttr1.offset - 1].attrType;

        // System.out.println("\n -- Scanning BTreeClusteredFile");

        scan = new BTClusteredFileScan[2];
        try {
            scan[0] = file[0].new_scan(null, null);
            scan[1] = file[1].new_scan(null, null);
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

        computeLB_UB(in1[joinAttr1.offset - 1].attrType, in2[joinAttr2.offset - 1].attrType, in1[mergeAttr1.offset - 1].attrType, in2[mergeAttr2.offset - 1].attrType);

        while (data[0] != null || data[1] != null) {
            val = sortByValue(result, k);
            
            if(val){
                break;
            }
            computeLB_UB(in1[joinAttr1.offset - 1].attrType, in2[joinAttr2.offset - 1].attrType, in1[mergeAttr1.offset - 1].attrType, in2[mergeAttr2.offset - 1].attrType);
        }
        int len = Math.min(result.size(), k);

        System.out.println("\n---Top K Tuples---");
        System.out.println("Total Join count: "+result.size());
        for(int i = 0; i < len; i++){
            if(oTable != "null") insert_data(result.get(i));
            try {
                result.get(i).print();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            scan[0].DestroyBTreeFileScan();
            scan[1].DestroyBTreeFileScan();
        } catch (InvalidFrameNumberException e) {
            e.printStackTrace();
        } catch (ReplacerException e) {
            e.printStackTrace();
        } catch (PageUnpinnedException e) {
            e.printStackTrace();
        } catch (HashEntryNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void computeLB_UB(int attrTypejoin_1, int attrTypejoin_2, int attrTypemerge_1, int attrTypemerge_2){

        Object t1join = null, t2join = null, t1merge = null, t2merge = null;
        RID rid = new RID();
        try {
            data[0] = scan[0].get_next(rid);
            data[1] = scan[1].get_next(rid);
            Tuple t1 = null, t2 = null;
            
            if (data[0] != null) {
                t1 = (Tuple) ((ClusteredLeafData) data[0].data).getData();
                t1merge = getField(t1,attrTypemerge_1,mergeAttr1.offset);
                t1join = getField(t1, attrTypejoin_1, joinAttr1.offset);
                TupleList tl = tList.getOrDefault(t1join, new TupleList(new ArrayList<>(), new ArrayList<>()));
                tl.getList1().add(t1);
                tList.put(t1join, tl);
            }
            if (data[1] != null){
                t2 = (Tuple) ((ClusteredLeafData) data[1].data).getData();
                t2join = getField(t2,attrTypejoin_2,joinAttr2.offset);   
                t2merge = getField(t2, attrTypemerge_2, mergeAttr2.offset);
                TupleList tl = tList.getOrDefault(t2join, new TupleList(new ArrayList<>(), new ArrayList<>()));
                tl.getList2().add(t2);
                tList.put(t2join, tl);
            } 
            
            if (data[0] != null) {
                if(type == AttrType.attrInteger) ub2 = (Integer) t2merge + (Integer) t1merge;
                else ub2 = (Float) t2merge + (Float) t1merge;
                if(tList.get(t1join).getList2().isEmpty()){

                    result.add(setTuple(t1, null, t1merge, ub2));
                }else{
                    for(int i = 0; i < tList.get(t1join).getList2().size(); i++){
                        Tuple tup = tList.get(t1join).getList2().get(i);
                        
                        Object put;
                        if(type == AttrType.attrInteger){
                            put = (Integer) t1merge + (Integer) getField(tup, AttrType.attrInteger, mergeAttr2.offset);
                        }else{
                            put = (Float) t1merge + (Float) getField(tup, AttrType.attrReal, mergeAttr2.offset);
                        }
                        result.add(setTuple(t1, tup, put, put));
                    }
                }
            }
            if (data[1] != null) {
                if(type == AttrType.attrInteger) ub1 = (Integer) t2merge + (Integer) t1merge;
                else ub1 = (Float) t2merge + (Float) t1merge;
                    if(tList.get(t2join).getList1().isEmpty()){
                        result.add(setTuple(null, t2, t2merge, ub1));
                    }else{
                        for(int i = 0; i < tList.get(t2join).getList1().size(); i++){
                            Tuple tup = tList.get(t2join).getList1().get(i);
                            Object put;
                            if(type == AttrType.attrInteger)
                                put = (Integer) t2merge + (Integer) getField(tup, AttrType.attrInteger, mergeAttr1.offset);
                            else{
                                put = (Float) t2merge + (Float) getField(tup, AttrType.attrReal, mergeAttr1.offset);
                            }
                            result.add(setTuple(tup, t2, put, put));
                        }
                    }
            }
        } catch (ScanIteratorException e) {
            e.printStackTrace();
        }
        
    }

    //functon to get values by type
    public Object getField(Tuple t, int type, int offset){
        if(type == AttrType.attrString){
            try {
                return t.getStrFld(offset);
            } catch (FieldNumberOutOfBoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if(type == AttrType.attrInteger){
            try {
                return t.getIntFld(offset);
            } catch (FieldNumberOutOfBoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            try {
                return t.getFloFld(offset);
            } catch (FieldNumberOutOfBoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // function to sort hashmap by values
    public boolean sortByValue(List<Tuple> list, int k)
    {
          
        // Sort the list
        Collections.sort(list, new Comparator<Tuple>() {
            public int compare(Tuple o1, 
                               Tuple o2)
            {   
                if(type == AttrType.attrInteger)
                    return ((Integer) getField(o1, AttrType.attrInteger, nColumns -1)).compareTo((Integer) getField(o2, AttrType.attrInteger, nColumns -1));
                else
                    return ((Float) getField(o1, AttrType.attrReal, nColumns -1)).compareTo((Float) getField(o2, AttrType.attrReal, nColumns -1));
            }
        });

        // System.out.println("\nPass "+(i++));

        if(list.size() > k){
            // System.out.println("Condition "+(Integer) getField(list.get(k), AttrType.attrInteger, nColumns)+" "+(Integer) getField(list.get(k - 1), AttrType.attrInteger, nColumns - 1));
            boolean temp;
            if(type == AttrType.attrInteger)
                temp = (Integer) getField(list.get(k), AttrType.attrInteger, nColumns) >= (Integer) getField(list.get(k - 1), AttrType.attrInteger, nColumns - 1);
            else
                temp = (Float) getField(list.get(k), AttrType.attrReal, nColumns) >= (Float) getField(list.get(k - 1), AttrType.attrReal, nColumns - 1);
            if(temp)
                return true;
            else
                return false;
        }
        return false;
    }
    

    void insert_data(Tuple tuple1){

        RID rid = new RID();
        try {
            rid = f.insertRecord(tuple1.returnTupleByteArray());
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (SpaceNotAvailableException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    

    Tuple setTuple(Tuple t1, Tuple t2, Object lb, Object ub){
        tuple1 = new Tuple(size);
        try {
            tuple1.setHdr((short) nColumns, oAttrTypes, oAttrSize);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() *** "+oAttrTypes.length);
            e.printStackTrace();
        }
        try {
            if(t1 == null){
                for(int i = 0; i < in1.length; i++){
                    if(oAttrTypes[i].attrType == AttrType.attrInteger)
                        tuple1.setIntFld(i + 1, -1);
                    else if(oAttrTypes[i].attrType == AttrType.attrString)
                        tuple1.setStrFld(i + 1, "-1");
                    else
                        tuple1.setFloFld(i + 1, -1.00f);
                    
                }

                for(int i = in1.length; i < nColumns - 2; i++){
                    if(oAttrTypes[i].attrType == AttrType.attrInteger){
                        if(i - in1.length + 1 == mergeAttr2.offset)
                            tuple1.setIntFld(i + 1, -t2.getIntFld(i - in1.length + 1));
                        else
                            tuple1.setIntFld(i + 1, t2.getIntFld(i - in1.length + 1));
                        
                    }else if(oAttrTypes[i].attrType == AttrType.attrString)
                        tuple1.setStrFld(i + 1, t2.getStrFld(i - in1.length + 1));
                    else{
                        if(i - in1.length + 1 == mergeAttr2.offset)
                            tuple1.setFloFld(i + 1, -t2.getFloFld(i - in1.length + 1));
                        else{
                            t2.getFloFld(i - in1.length + 1);
                            tuple1.setFloFld(i + 1, t2.getFloFld(i - in1.length + 1));
                        }
                    }
                }
            }else if(t2 == null){
                for(int i = 0; i < in1.length; i++){
                    if(oAttrTypes[i].attrType == AttrType.attrInteger){
                        if(i + 1 == mergeAttr2.offset)
                            tuple1.setIntFld(i + 1, -t1.getIntFld(i + 1));
                        else
                            tuple1.setIntFld(i + 1, t1.getIntFld(i + 1));
                    }else if(oAttrTypes[i].attrType == AttrType.attrString)
                        tuple1.setStrFld(i + 1, t1.getStrFld(i + 1));
                    else{
                            if(i + 1 == mergeAttr2.offset)
                                tuple1.setFloFld(i + 1, -t1.getFloFld(i + 1));
                            else{
                                System.out.println(t1.getFloFld(i + 1));
                                tuple1.setFloFld(i + 1, t1.getFloFld(i + 1));
                            }
                        }
                }

                for(int i = in1.length; i < nColumns - 2; i++){
                    if(oAttrTypes[i].attrType == AttrType.attrInteger)
                        tuple1.setIntFld(i + 1, -1);
                    else if(oAttrTypes[i].attrType == AttrType.attrString)
                        tuple1.setStrFld(i + 1, "-1");
                    else
                        tuple1.setFloFld(i + 1, -1.00f);
                }
            }else{
                for(int i = 0; i < in1.length; i++){
                    if(oAttrTypes[i].attrType == AttrType.attrInteger){
                        if(i + 1 == mergeAttr2.offset)
                            tuple1.setIntFld(i + 1, -t1.getIntFld(i + 1));
                        else
                            tuple1.setIntFld(i + 1, t1.getIntFld(i + 1));
                    }else if(oAttrTypes[i].attrType == AttrType.attrString)
                        tuple1.setStrFld(i + 1, t1.getStrFld(i + 1));
                    else{
                        if(i + 1 == mergeAttr2.offset)
                            tuple1.setFloFld(i + 1, -t1.getFloFld(i + 1));
                        else
                            tuple1.setFloFld(i + 1, t1.getFloFld(i + 1));
                    }
                }

                for(int i = in1.length; i < nColumns - 2; i++){
                    if(oAttrTypes[i].attrType == AttrType.attrInteger){
                        if(i - in1.length + 1 == mergeAttr2.offset)
                            tuple1.setIntFld(i + 1, -t2.getIntFld(i - in1.length + 1));
                        else
                            tuple1.setIntFld(i + 1, t2.getIntFld(i - in1.length + 1));
                    }else if(oAttrTypes[i].attrType == AttrType.attrString)
                        tuple1.setStrFld(i + 1, t2.getStrFld(i - in1.length + 1));
                    else{

                        if(i - in1.length + 1 == mergeAttr2.offset)
                            tuple1.setFloFld(i + 1, -t2.getFloFld(i - in1.length + 1));
                        else
                            tuple1.setFloFld(i + 1, t2.getFloFld(i - in1.length + 1));
                    }
                }
            }
            if(in1[mergeAttr1.offset - 1].attrType == AttrType.attrInteger){
                tuple1.setIntFld(nColumns - 1, (Integer) lb);
                tuple1.setIntFld(nColumns, (Integer) ub);
            }else{
                tuple1.setFloFld(nColumns - 1, (Float) lb);
                tuple1.setFloFld(nColumns, (Float) ub);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return tuple1;
    }
}

class TupleList{
    List<Tuple> list1;
    List<Tuple> list2;

    public TupleList(List<Tuple> list1, List<Tuple> list2){
        this.list1 = list1;
        this.list2 = list2;
    }

    public List<Tuple> getList1() {
        return list1;
    }

    public List<Tuple> getList2() {
        return list2;
    }
}
