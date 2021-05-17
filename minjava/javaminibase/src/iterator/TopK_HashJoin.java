package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import global.AttrType;
import global.RID;
import heap.*;
import tests.TestDriver;

import java.io.IOException;
import java.util.*;

import static tests.TestDriver.OK;

public class TopK_HashJoin {


  private AttrType[] in1, in2;
  private int len_in1, len_in2;
  private short[] t1_str_sizes, t2_str_sizes;
  private FldSpec joinAttr1, joinAttr2, mergeAttr1, mergeAttr2;
  private CustomScan relationName1, relationName2;
  private int k;
  private String oTable;
  boolean val;
  int i = 1;
  private int n_pages;
  private BTreeClusteredFile[] file = new BTreeClusteredFile[2];
//  private HashMap<Object, Integer[]> lub = new HashMap<Object, Integer[]>();
//  int ub1=0, ub2=0;
  private KeyDataEntry[] data = new KeyDataEntry[2];
  private BTClusteredFileScan[] scan;
  boolean status = OK;
//  List<Map.Entry<?, Integer[]> > list;
  private Heapfile f;
  int nColumns;
  short[] oAttrSize;
  AttrType[] oAttrTypes;
  String[] oAttrName;
  Tuple tuple1;
  short size;
  short[] Jsizes;

  // ADDITIONAL from HashJoin class
  AttrType[] Jtypes;
  boolean isString;
  private int hashAttr1;
  private int hashAttr2;
  private Map<Integer, Heapfile> innerPartitionMap;
  private Map<Integer, Heapfile> outerPartitionMap;
  private int BUCKET_NUMBER=5;
//  private String BUCKET_NAME_PREFIX = "bucket_";
//  int tempHeapCounter= 0;
  PriorityQueue<MyEntry> result;

  private boolean debug = true;


  public TopK_HashJoin(AttrType[] in1, int len_in1, short[] t1_str_sizes, FldSpec joinAttr1, FldSpec mergeAttr1, AttrType[] in2, int len_in2,
                       short[] t2_str_sizes, FldSpec joinAttr2, FldSpec mergeAttr2, String relationName1, String relationName2, int k, int n_pages, String oTable){
    try{
      this.relationName1 = new CustomScan(relationName1);
      this.relationName2 = new CustomScan(relationName2);
    } catch (Exception e) {
      e.printStackTrace();
    }
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

    // Additional from HashJoin class
    hashAttr1 = joinAttr1.offset;
    hashAttr2 = joinAttr2.offset;
    innerPartitionMap = new HashMap<>();
    outerPartitionMap = new HashMap<>();

    this.Jtypes = new AttrType[len_in1+len_in2+1];
    getOutputAttrType();

    result = new PriorityQueue<MyEntry>(k, new MergeValueComparator());
  }

  //functon to get values by type
  private Object getField(Tuple t, int type, int offset){
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


  ////////////////////////////////////


  public static class Value{
    public String strValue;
    public int intValue;

    public Value(int v){
      intValue = v;
    }

    public Value(String s){
      strValue= s;
    }
  }

  public AttrType[] getOutputAttrType(){
    int index = 0;
    for(int ind=0; ind<(len_in1) ; ind++){

      if(in1[ind].toInt() == AttrType.attrInteger){
        Jtypes[index++] = new AttrType(AttrType.attrInteger);
      } else if(in1[ind].toInt() == AttrType.attrReal){
        Jtypes[index++] = new AttrType(AttrType.attrReal);
      } else{
        Jtypes[index++] = new AttrType(AttrType.attrString);
      }
    }
    for(int ind=0; ind<(len_in2) ; ind++){

      if(in2[ind].toInt() == AttrType.attrInteger){
        Jtypes[index++] = new AttrType(AttrType.attrInteger);
      } else if(in2[ind].toInt() == AttrType.attrReal){
        Jtypes[index++] = new AttrType(AttrType.attrReal);
      } else{
        Jtypes[index++] = new AttrType(AttrType.attrString);
      }
    }
    Jtypes[index] = new AttrType(AttrType.attrReal);
    return Jtypes;
  }

  public void partition(){
    try{

      // build phase start
      int hashVal = 0;
      //Inner partitionning
//      Heapfile innerHf = new Heapfile(relationName1);
//      Scan sc = innerHf.openScan();
//      RID inRID = new RID();
      Tuple inTup = null;
      try {
        while ((inTup = relationName2.get_next()) != null) {
          //call the function
          inTup.setHdr((short) len_in1, in1, t1_str_sizes);
//                 hashVal = hashFunction(inTup);
          if (isString) {
            hashVal = hashFunctionString(inTup.getStrFld(hashAttr1));
          } else {
            hashVal = hashFunctionInteger(inTup.getIntFld(hashAttr1));
          }
//                 System.out.println(hashVal);
//                 inTup.print(attrTypes1);
          if (innerPartitionMap.get(hashVal) == null) {
//                     System.out.println(tempHeapCounter);
            Heapfile hf1 = new Heapfile(Heapfile.getRandomHFName());
//          Heapfile hf1 = new Heapfile(BUCKET_NAME_PREFIX+tempHeapCounter);
//          tempHeapCounter++;
            innerPartitionMap.put(hashVal, hf1);
            insertRecordInBucket(inTup, hf1);
          } else {
            insertRecordInBucket(inTup, innerPartitionMap.get(hashVal));
          }
        }
      }catch (Exception e){
        e.printStackTrace();
      }
      try {
//        sc.closescan();
        relationName2.close();
      } catch (Exception e) {
//        status = TestDriver.FAIL;
//        e.printStackTrace();
      }

      hashVal = 0;
      //Outer partitionning
//      RID outRID = new RID();
//      Tuple outTup = null;
//      Heapfile outterHf = new Heapfile(relationName2);
//      Scan outerSc = outterHf.openScan();
//      while ((outTup=relationName1.get_next())!= null){
//        outTup.setHdr((short) len_in2, in2, t2_str_sizes);
//        //call the hash functin
////                hashVal = hashFunction(outTup);
//        if(isString){
//          hashVal = hashFunctionString(outTup.getStrFld(hashAttr2));
//        }else{
//          hashVal = hashFunctionInteger(outTup.getIntFld(hashAttr2));
//        }
//
//        if(outerPartitionMap.get(hashVal)==null){
//          // TODO : check if bucket heapfile name should be different
////          Heapfile hf2 = new Heapfile(BUCKET_NAME_PREFIX+relationName2+"_"+hashVal);
//          Heapfile hf2 = new Heapfile(Heapfile.getRandomHFName());
////          tempHeapCounter++;
//          outerPartitionMap.put(hashVal, hf2);
//          insertRecordInBucket(outTup, hf2);
//        }else{
//          insertRecordInBucket(outTup, outerPartitionMap.get(hashVal));
//        }
//      }
//      try {
////        outerSc.closescan();
//        relationName1.close();
//      } catch (Exception e) {
//        status = TestDriver.FAIL;
//        e.printStackTrace();
//      }

      // build phase end
    }catch (Exception e){
      e.printStackTrace();
    }

  }


  //HAsh function to be used by both tables.
  public int hashFunction(Tuple tple){
    //implement hash function
    int hashValue =0;
    AttrType[] type = tple.getTypes();

    try{
      //Integer hashing
//            System.out.println(type);
//            System.out.println(type[hashAttr]);
      if(type[hashAttr1].attrType== AttrType.attrInteger){
        int field = tple.getIntFld(hashAttr1);
        hashValue = field % BUCKET_NUMBER;
      }
    }catch (Exception e){
      e.printStackTrace();
    }

    try{
      //String hashing
      if(type[hashAttr1].attrType== AttrType.attrString){
        String field = tple.getStrFld(hashAttr1);
        hashValue = Math.abs(field.hashCode())%BUCKET_NUMBER;
      }
    }catch (Exception e){

    }

    return hashValue;
  }

  public void insertRecordInBucket(Tuple tpl, Heapfile heap){
    //Insert
    try{
      byte [] tempBytes = tpl.returnTupleByteArray();
      heap.insertRecord(tempBytes);
    }catch (Exception e){
      e.printStackTrace();
    }

  }

  public int hashFunctionInteger(int val){
    return val%BUCKET_NUMBER;
  }

  public int hashFunctionString(String s){
    return Math.abs(s.hashCode())%BUCKET_NUMBER;
  }

  public boolean tupleMatchOnField(Tuple tp1, Tuple tp2, int field1, int field2, boolean isString){
    boolean equals = false;
    try{
      if(isString){

        String val1 = tp1.getStrFld(field1);
        String val2 = tp2.getStrFld(field2);
//                System.out.println(val1+" "+val2);
        if(val1.equals(val2)) {
          equals = true;
        }
      }else{
        int val1 = tp1.getIntFld(field1);
        int val2 = tp2.getIntFld(field2);
        if(val1==val2) {
          equals = true;
        }
      }
    }catch (Exception e){
      e.printStackTrace();
    }
    return equals;
  }
  public void get(boolean isString){
    //get the result the join
    partition();


    try {
//            System.out.println(hashedValue);
//            System.out.println(hashFunctionString("1aaaaaaaa"));

//            Heapfile outterHeapFile = outerPartitionMap.get(hashedValue);


      Tuple outterTuple = new Tuple();
      RID outRid = new RID();
//            outterSc.getNextAndCountRecords(outRid);
//            System.out.println("number of outer elements "+ outterSc.getNumberOfRecordsPerOnePage());
      Tuple Jtuple = new Tuple();

      short[] t_size;


      Jsizes = new short[2];
      Jsizes[0] = t2_str_sizes[0];
      Jsizes[1] = t1_str_sizes[0];


      Jtuple.setHdr((short) Jtypes.length, Jtypes, Jsizes);

      // Probe phase start
//        Heapfile outterHeapFile = new Heapfile(relationName2);
//        Scan outterSc = outterHeapFile.openScan();

      int joinTuplesCount = 0;
      while ((outterTuple = relationName1.get_next()) != null) {

          outterTuple.setHdr((short) len_in2, in2, t2_str_sizes);
//          if(debug)
//            outterTuple.print(in2);

          boolean addedToResults = false;

          RID innerRid = new RID();
          Tuple innerTuple = new Tuple();
  //                innerSc.getNextAndCountRecords(innerRid);
  //                System.out.println("number of outer elements "+ innerSc.getNumberOfRecordsPerOnePage());
          int hashedValue = 0;
          if (!isString) {
            hashedValue = hashFunctionInteger(outterTuple.getIntFld(hashAttr2));
          } else {
            hashedValue = hashFunctionString(outterTuple.getStrFld(hashAttr2));
          }
          //outterTuple.print(in2);
          //outterTuple.getTupleByteArray();
          Heapfile innerHeapFile = innerPartitionMap.get(hashedValue);
          if(innerHeapFile !=null){
            Scan innerSc = innerHeapFile.openScan();
            while ((innerTuple = innerSc.getNext(innerRid)) != null) {
              innerTuple.setHdr((short) len_in1, in1, t1_str_sizes);

    //                        Value v = new Value(value);
    //                    check where they match
              boolean match = tupleMatchOnField(innerTuple, outterTuple, hashAttr1, hashAttr2, isString);
    //                    System.out.println(match);
              if (match) {

    //                        t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
    //                                attrTypes1, len_in1, attrTypes2, len_in2,
    //                                _t1_str_sizes, _t2_str_sizes,
    //                                proj, 6);


    //                        if(!addedToResults){


                Jtuple.setStrFld(1, outterTuple.getStrFld(1));
                Jtuple.setIntFld(2, outterTuple.getIntFld(2));
                Jtuple.setIntFld(3, outterTuple.getIntFld(3));
                Jtuple.setStrFld(4, innerTuple.getStrFld(1));
                Jtuple.setIntFld(5, innerTuple.getIntFld(2));
                Jtuple.setIntFld(6, innerTuple.getIntFld(3));

                float mergeAvg = (float)((outterTuple.getIntFld(mergeAttr2.offset)
                                          + innerTuple.getIntFld(mergeAttr1.offset)) / 2.0);

                Jtuple.setFloFld(7, mergeAvg);

    //                            Projection.Join(outterTuple, attrTypes2,
    //                                    innerTuple, attrTypes1,
    //                                    Jtuple, proj, 2);
    //
                joinTuplesCount++;
                if (result.size() < k){
                  result.offer(new MyEntry(Jtuple, mergeAvg));
                }else {
                  MyEntry peek = result.peek();
                  if(mergeAvg > peek.getValue()){
                    result.remove();
                    result.offer(new MyEntry(Jtuple, mergeAvg));
                  }
                }

                Jtuple = new Tuple();
                Jtuple.setHdr((short) Jtypes.length, Jtypes, Jsizes);
              }

            }
            try {
              innerSc.closescan();
            } catch (Exception e) {
              status = TestDriver.FAIL;
              e.printStackTrace();
            }

          }
        }
      try {
//        outterSc.closescan();
        relationName1.close();
      } catch (Exception e) {
        status = TestDriver.FAIL;
        e.printStackTrace();
      }

      System.out.println("\nHash Join total tuples count: "+joinTuplesCount);

    }catch (Exception e){
      e.printStackTrace();
    }
  }
  //
  public java.util.Iterator get_next(){
    get(isString);
    List<Tuple> topKTuples = new ArrayList<>();
    for(MyEntry e : result){
      topKTuples.add(e.getKey());
    }
    if(oTable != null){
      try {
        Heapfile outFile = new Heapfile(oTable);

        for(Tuple t : topKTuples){
          t.setHdr((short) Jtypes.length, Jtypes, Jsizes);
          outFile.insertRecord(t.getTupleByteArray());
        }
        System.out.println("\nJoin table "+oTable+" created with "+topKTuples.size()+" records.\n");
      } catch (HFException e) {
        e.printStackTrace();
      } catch (HFBufMgrException e) {
        e.printStackTrace();
      } catch (HFDiskMgrException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InvalidTupleSizeException e) {
        e.printStackTrace();
      } catch (InvalidSlotNumberException e) {
        e.printStackTrace();
      } catch (SpaceNotAvailableException e) {
        e.printStackTrace();
      } catch (InvalidTypeException e) {
        e.printStackTrace();
      }

    }
    java.util.Iterator i = topKTuples.iterator();
    return i;

  }

  public void printAll(AttrType [] tp){
    try {
      java.util.Iterator i = result.iterator();
      while (i.hasNext()){
        Tuple t = (Tuple) i.next();
        t.print(tp);

      }
    }catch (Exception e){
      e.printStackTrace();
    }

  }


}

final class MyEntry implements Map.Entry<Tuple, Float> {
  private final Tuple key;
  private Float value;

  MyEntry(Tuple key, Float value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public Tuple getKey() {
    return key;
  }

  @Override
  public Float getValue() {
    return value;
  }

  @Override
  public Float setValue(Float aFloat) {
    Float old = this.value;
    this.value = value;
    return old;
  }
}

class MergeValueComparator implements Comparator<MyEntry>{
  @Override
  public int compare(MyEntry t1, MyEntry t2) {
    return (t1.getValue()).compareTo(t2.getValue());
  }
}
