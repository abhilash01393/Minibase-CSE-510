package iterator;

import btree.*;
import bufmgr.PageNotReadException;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import hash.ClusteredHashFile;
import hash.ClusteredHashFileScan;
import heap.*;
import index.IndexException;
import index.IndexScan;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class IndexJoin extends Iterator
    {
        private AttrType      _in1[],  _in2[];
        private   int        in1_len, in2_len;
        private   Iterator  theouter;
        private   short t2_str_sizescopy[];
        private short t1_str_sizecopy[];
        private   CondExpr OutputFilter[];
        private   CondExpr RightFilter[];
        private   int        n_buf_pgs;        // # of buffer pages available.
        private   boolean        done,         // Is the join complete
                get_from_outer;                 // if TRUE, a tuple is got from outer
        private   Tuple     outer_tuple, inner_tuple;
        private   Tuple     Jtuple;           // Joined tuple
        private   FldSpec   perm_mat[];
        private   int        nOutFlds;
        private   Heapfile  hf;
        private   Scan      inner;
        private String relName;
        private Iterator innerIterrator;
        public NestedLoopsJoins nestedLoopsJoins;
        private int indexAttrNumber;
        private int indexAttrNumber2;
        public static final short CLUSTERED_HASH = 0;
        public static final short CLUSTERED_BTREE = 1;
        public static final short UNCLUSTERED_HASH = 2;
        public static final short UNCLUSTERED_BTREE = 3;
        public static final short NO_INDEX = 4;
        private int indexTypeIfExists;
        private ArrayList<Tuple> result;
        AttrType[] Jtypes;
        String relName1;
        public static final short EQUAL=0;
        public static final short LESS=1;
        public static final short GREATER=2;
        public static final short LESSOREQUAL=3;
        public static final short GREATEROREQUAL=4;
        public short operation;
        short  []  Jsizes;



        /**constructor
         *Initialize the two relations which are joined, including relation type,
         *@param in1  Array containing field types of R.
         *@param len_in1  # of columns in R.
         *@param t1_str_sizes shows the length of the string fields.
         *@param in2  Array containing field types of S
         *@param len_in2  # of columns in S
         *@param  t2_str_sizes shows the length of the string fields.
         *@param amt_of_mem  IN PAGES
         *@param relationName  access hfapfile for right i/p to join

         *@param n_out_flds number of outer relation fileds
         *@exception IOException some I/O fault
         *@exception NestedLoopException exception from this class
         */
        public IndexJoin( AttrType    in1[],
                                 int     len_in1,
                                 short   t1_str_sizes[],
                                 AttrType    in2[],
                                 int     len_in2,
                                 short   t2_str_sizes[],
                                 int     amt_of_mem,
//                                 Iterator     am1,
                                 String relationName1,
                                 String relationName,
//                                 CondExpr outFilter[],
//                                 CondExpr rightFilter[],
//                                 FldSpec   proj_list[],
                                 int        n_out_flds,
                          int indexAttrNumber,
                          int indexAttrNumber2,
                          int indexTypeIfExists,
                          short operation
        ) throws IOException,NestedLoopException
        {

            _in1 = new AttrType[in1.length];
            _in2 = new AttrType[in2.length];
            System.arraycopy(in1,0,_in1,0,in1.length);
            System.arraycopy(in2,0,_in2,0,in2.length);
            in1_len = len_in1;
            in2_len = len_in2;
            relName = relationName;
            this.result = new ArrayList<>();
            this.relName1 = relationName1;
            this.operation = operation;

//            outer = am1;
            t1_str_sizecopy = t1_str_sizes;
            t2_str_sizescopy =  t2_str_sizes;
            inner_tuple = new Tuple();
            Jtuple = new Tuple();
//            OutputFilter = outFilter;
//            RightFilter  = rightFilter;
            this.indexTypeIfExists = indexTypeIfExists;

            n_buf_pgs    = amt_of_mem;
            inner = null;
            done  = false;
            get_from_outer = true;
            Jtypes = setuptOutputType(in1, in2);
            Jsizes= setupOutputSize(t1_str_sizes, t2_str_sizes);
//
//            Jtypes = new AttrType[5];
//
//            Jtypes[0] = new AttrType(AttrType.attrString);
//            Jtypes[1] = new AttrType(AttrType.attrString);
//            Jtypes[2] = new AttrType(AttrType.attrInteger);
//            Jtypes[3] = new AttrType(AttrType.attrInteger);
//            Jtypes[4] = new AttrType(AttrType.attrInteger);

            short[]    t_size;

//            perm_mat = proj_list;
            nOutFlds = n_out_flds;
            this.indexAttrNumber = indexAttrNumber;
            this.indexAttrNumber2 = indexAttrNumber2;
//            try {
//                t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
//                        in1, len_in1, in2, len_in2,
//                        t1_str_sizes, t2_str_sizes,
//                        proj_list, nOutFlds);
//            }catch (TupleUtilsException e){
//                throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
//            }



            try {
                hf = new Heapfile(relationName);

            }
            catch(Exception e) {
                throw new NestedLoopException(e, "Create new heapfile failed.");
            }


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
            return this.Jtypes;
        }

        /**
         *@return The joined tuple is returned
         *@exception IOException I/O errors
         *@exception JoinsException some join exception
         *@exception IndexException exception from super class
         *@exception InvalidTupleSizeException invalid tuple size
         *@exception InvalidTypeException tuple type not valid
         *@exception PageNotReadException exception from lower layer
         *@exception TupleUtilsException exception from using tuple utilities
         *@exception PredEvalException exception from PredEval class
         *@exception SortException sort exception
         *@exception LowMemException memory error
         *@exception UnknowAttrType attribute type unknown
         *@exception UnknownKeyTypeException key type unknown
         *@exception Exception other exceptions
         */
        public Tuple get_next()
                throws IOException,
                JoinsException ,
                IndexException,
                InvalidTupleSizeException,
                InvalidTypeException,
                PageNotReadException,
                TupleUtilsException,
                PredEvalException,
                SortException,
                LowMemException,
                UnknowAttrType,
                UnknownKeyTypeException,
                Exception
        {
            // This is a DUMBEST form of a join, not making use of any key information...


            if(indexTypeIfExists==NO_INDEX){
                //do nested loop join
//                nestedLoopsJoins= new NestedLoopsJoins(_in1, in1_len, t1_str_sizecopy, _in2, in2_len, t2_str_sizescopy, 10, outer, relName, OutputFilter, RightFilter,perm_mat, nOutFlds );
                nestedLoopJoin(relName1, relName);
            }else{
                short keySize = 4;
                if (_in2[indexAttrNumber2 - 1].attrType == AttrType.attrString) {
                    keySize = 32;
                }
                BTreeClusteredFile btree = null; // new BTreeClusteredFile(relName, (short) in2_len, _in2, t2_str_sizescopy);
                btree = new BTreeClusteredFile(relName, _in2[indexAttrNumber2 - 1].toInt(), keySize, indexAttrNumber2, 0, (short) _in2.length, _in2, t2_str_sizescopy);

                BTClusteredFileScan scan = null;

                ClusteredHashFile hash = new ClusteredHashFile(relName, 75,
                        _in2[indexAttrNumber2 - 1].toInt(), keySize, (short) _in2.length, _in2, t2_str_sizescopy);
                RID hashRid = new RID();
                ClusteredHashFileScan fscan = null;


//                Heapfile outterHeap = new Heapfile(relName1);
//                Scan outer = outterHeap.openScan();
                RID outerRid = new RID();

                Jtuple.setHdr((short)(_in1.length+_in2.length), Jtypes, Jsizes);
                RID btRid = new RID();



                CustomScan sc = new CustomScan(relName1);
//                while((outer_tuple = outer.getNext(outerRid))!=null){
                while((outer_tuple = sc.get_next())!=null){
                    outer_tuple.setHdr((short)in1_len, _in1, t1_str_sizecopy);
                    if(indexTypeIfExists==CLUSTERED_HASH){

//                        innerIterrator = new IndexScan(new IndexType(IndexType.Hash), relName, "HashIndex", _in2, t2_str_sizescopy,
//                                in2_len, in2_len, perm_mat, OutputFilter, indexAttrNumber, false);


                        hash.IntegerKey key1 = new hash.IntegerKey(outer_tuple.getIntFld(indexAttrNumber));

                        fscan = null;
                        if(operation==LESS || operation== LESSOREQUAL){
                            fscan = hash.newScan(key1,null);
                        }else if(operation==GREATER || operation==GREATEROREQUAL){
                            fscan = hash.newScan(null, key1);
                        }else{
                            fscan = hash.newScan(key1, null);
                        }
                        Tuple t = fscan.getNextTuple(hashRid);


                        while (t != null) {
                            inner_tuple = t;
                            inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
                            boolean match = tupleMatchOnField( outer_tuple, inner_tuple, indexAttrNumber, indexAttrNumber2, false, operation);
                            if(match){

                                setupOutputTuple(outer_tuple, _in1, inner_tuple, _in2);

                                result.add(Jtuple);
                                Jtuple = new Tuple();
                                Jtuple.setHdr((short)(_in1.length+_in2.length), Jtypes, Jsizes);
                            }

                            t = fscan.getNextTuple(hashRid);
                        }



                    }else if(indexTypeIfExists==CLUSTERED_BTREE){

//                        innerIterrator = new IndexScan(new IndexType(IndexType.B_Index), relName, "BTreeIndex", _in2, t2_str_sizescopy,
//                                in2_len, in2_len, perm_mat, OutputFilter, indexAttrNumber, false);
                        btree.IntegerKey key = new btree.IntegerKey(outer_tuple.getIntFld(indexAttrNumber));
                        scan = null;


                        if(operation==LESS || operation== LESSOREQUAL){
                            scan = btree.new_scan(key, null);
                        }else if(operation==GREATER || operation== GREATEROREQUAL){
                            scan = btree.new_scan(null, key);

                        }else{
                            scan = btree.new_scan(key, null);
                        }


                        KeyDataEntry data = null;
                        data = scan.get_next(btRid);


                        while (data != null) {

//                                System.out.println("Hey, I m here: "+ indexTypeIfExists);


                                inner_tuple = (Tuple) ((ClusteredLeafData) data.data).getData();
                                inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
                                outer_tuple.print(_in1);
                                inner_tuple.print(_in2);
                                boolean match = tupleMatchOnField( outer_tuple, inner_tuple, indexAttrNumber, indexAttrNumber2, false, operation);
                    System.out.println(match);
                                if(match){

//                        t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
//                                attrTypes1, len_in1, attrTypes2, len_in2,
//                                _t1_str_sizes, _t2_str_sizes,
//                                proj, 6);


//                        if(!addedToResults){

//                                    Jtuple.setStrFld(1, outer_tuple.getStrFld(1));
//                                    Jtuple.setStrFld(2, inner_tuple.getStrFld(1));
//                                    Jtuple.setIntFld(3, inner_tuple.getIntFld(2));
//                                    Jtuple.setIntFld(4, outer_tuple.getIntFld(3));
//                                    Jtuple.setIntFld(5, outer_tuple.getIntFld(3));
                                    setupOutputTuple(outer_tuple, _in1, inner_tuple, _in2);


                                    result.add(Jtuple);
                                    Jtuple = new Tuple();
                                    Jtuple.setHdr((short)(_in1.length+_in2.length), Jtypes, Jsizes);
                                }
                                data = scan.get_next(btRid);
                        }
                        scan = null;

                    }
                    SystemDefs.JavabaseBM.flushPages();


                }
            }
            return null;
        }

        public void nestedLoopJoin(String fileName1, String fileName2){
            try{
//                Heapfile outterHeapFile = new Heapfile(fileName1);
//                Heapfile innerHeapFile = new Heapfile(fileName2);

                CustomScan scan1 = new CustomScan(fileName1);



//                Scan outterSc = outterHeapFile.openScan();

                Tuple outterTuple = new Tuple();
                RID outRid = new RID();
                short[]    t_size;

                Jtuple.setHdr((short)(_in1.length+_in2.length), Jtypes, Jsizes);

//                while ((outterTuple=outterSc.getNext(outRid))!=null){
                while ((outterTuple=scan1.get_next())!=null){

                    outterTuple.setHdr((short) _in1.length, _in1, t1_str_sizecopy);

                    boolean addedToResults =  false;

                    RID innerRid = new RID();
                    Tuple innerTuple = new Tuple();
//                    Scan innerSc = innerHeapFile.openScan();
                    CustomScan scan2 = new CustomScan(fileName2);
                    while((innerTuple=scan2.get_next())!=null){
                        innerTuple.setHdr((short) _in2.length, _in2, t2_str_sizescopy);


//                        Value v = new Value(value);
//                    check where they match
                        boolean match = tupleMatchOnField( outterTuple, innerTuple, indexAttrNumber, indexAttrNumber2, false, operation);
                        if(match){


                            setupOutputTuple(outterTuple, _in1, innerTuple, _in2);
                            result.add(Jtuple);
                            Jtuple = new Tuple();
                            Jtuple.setHdr((short)(_in1.length+_in2.length), Jtypes, Jsizes);
//
                        }

                    }

                }


            }catch (Exception e){
                e.printStackTrace();
            }


        }

        public void SMJ(String fileName1, String fileName2){
            nestedLoopJoin(fileName1, fileName2);

            //sort the output
//            FileScan fscan = null;
//            try {
//                fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            // Sort "test1.in"
//            Sort sort = null;
//            try {
//                sort = new Sort(attrType, (short) 2, attrSize, fscan, 1, order[0], REC_LEN1, SORTPGNUM);
//            } catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }



        }

        public void joinWithIndex(String fileName1, BTClusteredFileScan sc){
            try{
//            System.out.println(hashedValue);
//            System.out.println(hashFunctionString("1aaaaaaaa"));
                Heapfile outterHeapFile =new Heapfile(fileName1);
//                Heapfile innerHeapFile = new Heapfile(fileName2);


                Scan outterSc = outterHeapFile.openScan();

                Tuple outterTuple = new Tuple();
                RID outRid = new RID();
//            outterSc.getNextAndCountRecords(outRid);
//            System.out.println("number of outer elements "+ outterSc.getNumberOfRecordsPerOnePage());
//                Tuple Jtuple = new Tuple();

                short[]    t_size;


//                short  []  Jsizes = new short[2];
//                Jsizes[0] = t2_str_sizescopy[0];
//                Jsizes[1] = t1_str_sizecopy[0];


                Jtuple.setHdr((short)(_in1.length+_in2.length), Jtypes, Jsizes);

                while ((outterTuple=outterSc.getNext(outRid))!=null){

                    outterTuple.setHdr((short) _in1.length, _in1, t1_str_sizecopy);

                    boolean addedToResults =  false;

                    RID innerRid = new RID();
                    Tuple innerTuple = new Tuple();
                    KeyDataEntry data = sc.get_next(innerRid);
//                innerSc.getNextAndCountRecords(innerRid);
//                System.out.println("number of outer elements "+ innerSc.getNumberOfRecordsPerOnePage());
//                    Scan innerSc = innerHeapFile.openScan();
                    inner_tuple = (Tuple) ((ClusteredLeafData) data.data).getData();
                    while(data!=null){
                        innerTuple.setHdr((short) _in2.length, _in2, t2_str_sizescopy);


//                        Value v = new Value(value);
//                    check where they match
                        boolean match = tupleMatchOnField(outterTuple, innerTuple, indexAttrNumber, indexAttrNumber2, false, operation);
//                    System.out.println(match);
                        if(match){

//                        t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
//                                attrTypes1, len_in1, attrTypes2, len_in2,
//                                _t1_str_sizes, _t2_str_sizes,
//                                proj, 6);


//                        if(!addedToResults){
//
//                            Jtuple.setStrFld(1, outterTuple.getStrFld(1));
//                            Jtuple.setStrFld(2, innerTuple.getStrFld(1));
//                            Jtuple.setIntFld(3, innerTuple.getIntFld(2));
//                            Jtuple.setIntFld(4, outterTuple.getIntFld(3));
//                            Jtuple.setIntFld(5, innerTuple.getIntFld(3));
                            setupOutputTuple(outterTuple, _in1, innerTuple, _in2);

//                            Projection.Join(outterTuple, attrTypes2,
//                                    innerTuple, attrTypes1,
//                                    Jtuple, proj, 2);
//

                            result.add(Jtuple);
                            Jtuple = new Tuple();
                            Jtuple.setHdr((short)(_in1.length+_in2.length), Jtypes, Jsizes);
//
                        }

                    }
                    data = sc.get_next(innerRid);
                    inner_tuple = (Tuple) ((ClusteredLeafData) data.data).getData();

                }

//            printAll(Jtypes);

            }catch (Exception e){
                e.printStackTrace();
            }
        }

        public boolean tupleMatchOnField(Tuple tp1, Tuple tp2, int fieldNo, int fieldNo2, boolean isString, short operation){
            boolean match=false;
            if(operation==EQUAL){
                try{
                    if(isString){

                        String val1 = tp1.getStrFld(fieldNo);
                        String val2 = tp2.getStrFld(fieldNo2);
    //                System.out.println(val1+" "+val2);
                        if(val1.equals(val2)) {
                            match = true;
                        }
                    }else{

                        int val1 = tp1.getIntFld(fieldNo);
                        int val2 = tp2.getIntFld(fieldNo2);
                        if(val1==val2) {
                            match = true;
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            else if(operation==LESS){
                try {
                    int val1 = tp1.getIntFld(fieldNo);
                    int val2 = tp2.getIntFld(fieldNo2);
                    if (val1 < val2) {
                        match = true;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }else if(operation==GREATER){
                try {
                int val1 = tp1.getIntFld(fieldNo);
                int val2 = tp2.getIntFld(fieldNo2);
                if (val1 > val2) {
                    match = true;
                }
            }catch (Exception e){
                e.printStackTrace();
            }

            }else if(operation==LESSOREQUAL){
                try {
                    int val1 = tp1.getIntFld(fieldNo);
                    int val2 = tp2.getIntFld(fieldNo2);
                    if (val1 <= val2) {
                        match = true;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }

            }else if(operation == GREATEROREQUAL){
                try {
                    int val1 = tp1.getIntFld(fieldNo);
                    int val2 = tp2.getIntFld(fieldNo2);
                    if (val1 >= val2) {
                        match = true;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            return match;
        }

        public java.util.Iterator getNext(){
            try {
                get_next();
            }catch (Exception e){
                e.printStackTrace();
            }

            java.util.Iterator i = result.iterator();
//        while (i.hasNext()){
            return i;
//        }

//        return null;
        }





        /**
         * implement the abstract method close() from super class Iterator
         *to finish cleaning up
         *@exception IOException I/O error from lower layers
         *@exception JoinsException join error from lower layers
         *@exception IndexException index access error
         */
        public void close() throws JoinsException, IOException,IndexException
        {
            if (!closeFlag) {

                try {
                    theouter.close();
                }catch (Exception e) {
                    throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
                }
                closeFlag = true;
            }
        }

//        public String indexAvailable(){
//            Iterator iter1 = null;
//            Iterator iter2 = null;
//            try {
////                        iter1 = new ClusteredHashFile(relName, 75, in2_len, t2_str_sizescop, (short) in2_len, _in2, t1_str_sizecopy);
//                BTreeClusteredFile btree = new BTreeClusteredFile(relName, (short) in2_len, _in2, t2_str_sizescopy);
//                BTClusteredFileScan scan = null;
//                scan = btree.new_scan(null, null);
//                RID btRid = new RID();
//                KeyDataEntry data = null;
//                data = scan.get_next(btRid);
//
//
//
//
//                ClusteredHashFile hash = new ClusteredHashFile(relName, (short) in2_len, _in2, t2_str_sizescopy);
//                RID hashRid = new RID();
//                ClusteredHashFileScan fscan = null;
//                fscan = hash.newScan(null, null);
//                Tuple t2 = fscan.getNextTuple(hashRid);
//
//
//                iter1 = new IndexScan(new IndexType(IndexType.B_Index), relName, "BTreeIndex", _in2, t2_str_sizescopy,
//                        in2_len, in2_len, perm_mat, OutputFilter, indexAttrNumber2, false);
//
//                iter1 = new IndexScan(new IndexType(IndexType.Hash), relName, "HashIndex", _in2, t2_str_sizescopy,
//                        in2_len, in2_len, perm_mat, OutputFilter, indexAttrNumber2, false);
//                if(data!=null){
//                    return "btree";
//                }else if (t2 !=null){
//                    return "hash";
//                }
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//            return null;
//        }

    }
