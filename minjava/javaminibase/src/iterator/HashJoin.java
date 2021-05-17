//   /**
//    * class constructor. set up the index scan.
//    * @param index type of the index (B_Index, Hash)
//    * @param relName name of the input relation
//    * @param indName name of the input index
//    * @param types array of types in this relation
//    * @param str_sizes array of string sizes (for attributes that are string)
//    * @param noInFlds number of fields in input tuple
//    * @param noOutFlds number of fields in output tuple
//    * @param outFlds fields to project
//    * @param selects conditions to apply, first one is primary
//    * @param fldNum field number of the indexed field
//    * @param indexOnly whether the answer requires only the key or the tuple
//    * @exception IndexException error from the lower layer
//    * @exception InvalidTypeException tuple type not valid
//    * @exception InvalidTupleSizeException tuple size not valid
//    * @exception UnknownIndexTypeException index type unknown
//    * @exception IOException from the lower layer
//    */


// try {
//     am = new IndexScan ( b_index, "sailors.in",
//         "BTreeIndex", Stypes, Ssizes, 4, 2,
//         Sprojection, null, 1, false);
// }catch(Exception e){
//     ;
// }


package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;


public class HashJoin{
    private   Heapfile  _innerhf;
    private Heapfile _outerHf;
    private Scan _innserScan;
    private Scan _outerScan;
    private CustomScan innerRelName;
    private CustomScan outterRelName;
    private HashMap<Integer, Heapfile> innerPartitionMap;
    private HashMap<Integer, Heapfile> outerPartitionMap;
    private int BUCKET_NUMBER=5;
    private String BUCKET_NAME_PREFIX = "bucket_";
//    private AttrType[] attrTypes;
    private AttrType [] attrTypes1;
    private AttrType [] attrTypes2;
    int hashAttr;
    int hashAttr2;
    ArrayList<Tuple> result;
//    HashSet<Tuple> result;
    int value;
    short[] _t1_str_sizes;
    short[] _t2_str_sizes;
    AttrType[] Jtypes;
    int len_in1;
    int len_in2;
    int tempHeapCounter= 0;
    Iterator outer;
    FldSpec[] proj;
    boolean isString;
    Tuple Jtuple;
    short[] JSizes;




    public HashJoin(
            CustomScan inRelName,
                     AttrType[] attrType1,
            CustomScan outRelName,
                     AttrType[] attrType2,
                     int attr,
                     int attr2,
                     short[] _t1_str_sizes,
                     short[] _t2_str_sizes,
                     boolean isString)
    {
        innerRelName = inRelName;
        outterRelName = outRelName;
        hashAttr = attr;
        hashAttr2 = attr2;
        result = new ArrayList<>();
        this._t1_str_sizes = _t1_str_sizes;
        this._t2_str_sizes = _t2_str_sizes;
        this.attrTypes1 = attrType1;
        this.attrTypes2 = attrType2;
//        this.attrTypes1=attrTypes1;
//        this.attrTypes2 = attrTypes2;
//
//        this.len_in1 = len_in1;
//        this.len_in2 = len_in2;
        this.isString = isString;


        innerPartitionMap = new HashMap<>();
        outerPartitionMap = new HashMap<>();
        Jtuple = new Tuple();
        Jtypes = setuptOutputType(attrType1, attrType2);
        JSizes =  setupOutputSize(_t1_str_sizes, _t2_str_sizes);


    }

    public AttrType[] setuptOutputType(AttrType[] t1, AttrType[] t2){
        AttrType[] type = new AttrType[t1.length+t2.length];
        int count = 0;
        for (AttrType att: t1) {
            type[count] = att;
            count++;
        }

        for (AttrType att: t2) {
            type[count] = att;
            count++;
        }
        return type;
    }

    public short[] setupOutputSize(short[] s1, short[] s2){
        short  []  Jsizes = new short[s1.length + s2.length];

        int count = 0;
        for (short x: s1) {
            Jsizes[count] = x;
            count++;
        }

        for (short x: s2) {
            Jsizes[count] = x;
            count++;
        }
        return Jsizes;
    }

    public void setupOutputTuple(Tuple t1, AttrType[] att1, Tuple t2, AttrType[] att2){
        try {
            int count = 0;
            int fieldCount = 0;

            for (AttrType x : att1) {
                if (x.attrType==AttrType.attrString) {
                    Jtuple.setStrFld(count + 1, t1.getStrFld(fieldCount + 1));
                }

                if (x.attrType==AttrType.attrInteger) {
                    Jtuple.setIntFld(count + 1, t1.getIntFld(fieldCount + 1));
                }
                fieldCount++;
                count++;

            }
            fieldCount = 0;
            for (AttrType x : att2) {
                if (x.attrType==AttrType.attrString) {
                    Jtuple.setStrFld(count + 1, t2.getStrFld(fieldCount + 1));
                }

                if (x.attrType==AttrType.attrInteger) {
                    Jtuple.setIntFld(count + 1, t2.getIntFld(fieldCount + 1));
                }
                fieldCount++;
                count++;

            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public AttrType[] getOutputAttrType(){
        return Jtypes;
    }

    public void partition(){
        try{

            int hashVal = 0;
            //Inner partitionning
//            Heapfile innerHf = new Heapfile(innerRelName);
//            Scan sc = innerHf.openScan();
//            RID inRID = new RID();
            Tuple inTup = null;
//            while ((inTup=sc.getNext(inRID))!= null){
            while ((inTup=innerRelName.get_next())!= null){
                //call the function
                inTup.setHdr((short) attrTypes1.length, attrTypes1, _t1_str_sizes);
                Tuple tempTpl = new Tuple(inTup.size());
                tempTpl.tupleCopy(inTup);

//                 hashVal = hashFunction(inTup);
                if(isString){
                    hashVal = hashFunctionString(inTup.getStrFld(hashAttr));
                }else{
                    hashVal = hashFunctionInteger(inTup.getIntFld(hashAttr));
                }
//                 System.out.println(hashVal);
//                 inTup.print(attrTypes1);
                 if(innerPartitionMap.get(hashVal)==null){
//                     System.out.println(tempHeapCounter);
                     Heapfile hf1 = new Heapfile(BUCKET_NAME_PREFIX+tempHeapCounter);

                     tempHeapCounter++;
                     innerPartitionMap.put(hashVal, hf1);
                     insertRecordInBucket(tempTpl, hf1);
                 }else {
                     insertRecordInBucket(tempTpl, innerPartitionMap.get(hashVal));
                 }
            }

            hashVal = 0;
            //Outer partitionning
            RID outRID = new RID();
            Tuple outTup = null;
//            Heapfile outterHf = new Heapfile(outterRelName);
//            Scan outerSc = outterHf.openScan();
//            while ((outTup=outerSc.getNext(outRID))!= null){
            while ((outTup=outterRelName.get_next())!= null){
                outTup.setHdr((short) attrTypes2.length, attrTypes2, _t2_str_sizes);
                Tuple tempTpl = new Tuple(outTup.size());
                tempTpl.tupleCopy(outTup);

                //call the hash functin
//                hashVal = hashFunction(outTup);
                if(isString){
                    hashVal = hashFunctionString(outTup.getStrFld(hashAttr2));
                }else{
                    hashVal = hashFunctionInteger(outTup.getIntFld(hashAttr2));
                }

                if(outerPartitionMap.get(hashVal)==null){
                    Heapfile hf2 = new Heapfile(BUCKET_NAME_PREFIX+tempHeapCounter);
                    tempHeapCounter++;
                    outerPartitionMap.put(hashVal, hf2);
                    insertRecordInBucket(tempTpl, hf2);
                }else{
                    insertRecordInBucket(tempTpl, outerPartitionMap.get(hashVal));
                }
//                outTup = new Tuple(outTup.size());
            }

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
            if(type[hashAttr].attrType== AttrType.attrInteger){
                int field = tple.getIntFld(hashAttr);
                hashValue = field % BUCKET_NUMBER;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        try{
            //String hashing
            if(type[hashAttr].attrType== AttrType.attrString){
                String field = tple.getStrFld(hashAttr);
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

    public boolean tupleMatchOnField(Tuple tp1, Tuple tp2, int fieldNo, int fieldNo2, boolean isString){
        boolean equals = false;
        try{
            if(isString){

                String val1 = tp1.getStrFld(fieldNo);
                String val2 = tp2.getStrFld(fieldNo2);
//                System.out.println(val1+" "+val2);
                if(val1.equals(val2)) {
                    equals = true;
                }
            }else{
                int val1 = tp1.getIntFld(fieldNo);
                int val2 = tp2.getIntFld(fieldNo2);
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


        try{
            innerRelName = new CustomScan(innerRelName.getRelName());
            outterRelName = new CustomScan(outterRelName.getRelName());

//            System.out.println(hashedValue);
//            System.out.println(hashFunctionString("1aaaaaaaa"));

//            Heapfile outterHeapFile = outerPartitionMap.get(hashedValue);


            Tuple outterTuple = new Tuple();
            RID outRid = new RID();
//            outterSc.getNextAndCountRecords(outRid);
//            System.out.println("number of outer elements "+ outterSc.getNumberOfRecordsPerOnePage());
//            Tuple Jtuple = new Tuple();

//            short[]    t_size;


//            short  []  Jsizes = new short[2];
//            Jsizes[0] = _t2_str_sizes[0];
//            Jsizes[1] = _t1_str_sizes[0];


            Jtuple.setHdr((short)(attrTypes1.length+attrTypes2.length), Jtypes, JSizes);
//            Heapfile outterHeapFile = new Heapfile(outterRelName);
//            Scan outterSc = outterHeapFile.openScan();

//            while ((outterTuple=outterSc.getNext(outRid))!=null){
//            System.out.println(outterRelName.get_next());
            RID innerRid = new RID();
            while ((outterTuple=outterRelName.get_next())!=null){
                outterTuple.setHdr((short) attrTypes2.length, attrTypes2, _t2_str_sizes);
                boolean addedToResults =  false;


                Tuple innerTuple = new Tuple();
//                innerSc.getNextAndCountRecords(innerRid);
//                System.out.println("number of outer elements "+ innerSc.getNumberOfRecordsPerOnePage());
                int hashedValue = 0;
                if(!isString){
                    hashedValue =  hashFunctionInteger(outterTuple.getIntFld(hashAttr));
                }else{
                    hashedValue = hashFunctionString(outterTuple.getStrFld(hashAttr));
                }
                Heapfile innerHeapFile = innerPartitionMap.get(hashedValue);
                Scan innerSc = innerHeapFile.openScan();
                int op = 0;


                while((innerTuple=innerSc.getNext(innerRid))!=null){
                    innerTuple.setHdr((short) attrTypes1.length, attrTypes1, _t1_str_sizes);

//                  Value v = new Value(value);
//                  check where they match
                    boolean match = tupleMatchOnField(innerTuple, outterTuple, hashAttr,hashAttr2, isString);
//                    System.out.println(innerTuple.getIntFld(hashAttr)+"=="+""+outterTuple.getIntFld(hashAttr)+" "+match);


                    if(match){

//                        t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
//                                attrTypes1, len_in1, attrTypes2, len_in2,
//                                _t1_str_sizes, _t2_str_sizes,
//                                proj, 6);


//                        if(!addedToResults){

//                            Jtuple.setStrFld(1, outterTuple.getStrFld(1));
//                            Jtuple.setStrFld(2, innerTuple.getStrFld(1));
//                            Jtuple.setIntFld(3, innerTuple.getIntFld(2));
//                            Jtuple.setIntFld(4, outterTuple.getIntFld(3));
//                            Jtuple.setIntFld(5, innerTuple.getIntFld(3));
                        setupOutputTuple(outterTuple, attrTypes1, innerTuple, attrTypes2);

//                            Projection.Join(outterTuple, attrTypes2,
//                                    innerTuple, attrTypes1,
//                                    Jtuple, proj, 2);
//
                            result.add(Jtuple);
                         Jtuple = new Tuple();
                        innerTuple = new Tuple();
                        innerTuple.setHdr((short) attrTypes1.length, attrTypes1, _t1_str_sizes);

                        Jtuple.setHdr((short)(attrTypes1.length+attrTypes2.length), Jtypes, JSizes);
//                      op++;
                    }


                }
                innerSc.closescan();
                SystemDefs.JavabaseBM.flushPages();

            }

//            printAll(Jtypes);
            System.out.println(result.size());

        }catch (Exception e){
            e.printStackTrace();
        }
    }
//
    public java.util.Iterator get_next(){
        get(isString);
        java.util.Iterator i = result.iterator();
//        while (i.hasNext()){
            return i;
//        }

//        return null;
    }
//    nlj2 = new NestedLoopsJoins (Jtypes, 2, Jsizes,
//                                 Btypes, 3, Bsizes,
// 				   10,
//                                 nlj, "boats.in",
//                                 outFilter2, null, proj2, 1);

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
