package iterator;

import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortFirstSky extends Iterator {
        private AttrType      _in1[];
        private   short        in1_len;
        private   Iterator  outer;
        private   int n_buf_pgs_for_window;        // # of buffer pages available.
        private int n_buf_pgs;
        private int[] pref_list_cls;
        private int pref_list_length_cls;
        private   Iterator  sort;
        private List<Tuple> inner;
        private short[] t1_str_sizes_cls;
        int maxWindowSize;
        String sortFirstHFName;
        private Heapfile tempHF = null;


        public SortFirstSky(
                         AttrType[] in1,
                         int     len_in1,
                         short[] t1_str_sizes,
                         Iterator am1,
                         String relationName,
                         int[] pref_list,
                         int pref_list_length,
                         int n_pages

        )
                throws
                TupleUtilsException,
                IOException,
                SortException

        {

                _in1 = new AttrType[in1.length];
                System.arraycopy(in1,0,_in1,0,in1.length);
                in1_len = (short) len_in1;
                outer = am1;
                inner = null;

                pref_list_cls = pref_list;
                pref_list_length_cls = pref_list_length;
                t1_str_sizes_cls = t1_str_sizes;

                //Getting the maximum number of records on one page.
                RID id = new RID();
                Heapfile hf;
                try {
                        hf = new Heapfile(relationName);
                }
                catch(Exception e) {
                        throw new SortException(e, "Create new heapfile failed.");
                }

                Scan sc;
                try{
                        sc = hf.openScan();
                }catch(Exception e){
                        throw new SortException(e, "openScan failed");
                }
                try{
                        sc.getNextAndCountRecords(id);

                }catch(Exception e){
                        throw new SortException(e, "Could not get number of records on page 1");
                }
//                System.out.println(sc.getNumberOfRecordsPerOnePage());

                n_buf_pgs = Math.max((n_pages * 8/ 10), 3);
                n_buf_pgs_for_window = n_pages - n_buf_pgs;

                maxWindowSize = sc.getNumberOfRecordsPerOnePage()* n_buf_pgs_for_window;

                try {
                        sort = new SortPref(_in1, in1_len, t1_str_sizes, outer, new TupleOrder(TupleOrder.Descending) , pref_list_cls, pref_list_length_cls, n_buf_pgs);
                } catch (Exception e) {
                        e.printStackTrace();
                }

                inner = new ArrayList<>();
        }

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

                Tuple skylineTuple = null;
                Tuple currentOuter = null;

                while (true) {
//                        SystemDefs.JavabaseBM.flushPages();

                        currentOuter = sort.get_next();

                        if (currentOuter == null) {
                                break;
                        }

                        boolean dominated = false;

                        for (Tuple innerTuple : inner) {
                                if (TupleUtils.Dominates(innerTuple, _in1, currentOuter, _in1, in1_len, t1_str_sizes_cls, pref_list_cls, pref_list_length_cls)) {
                                        dominated = true;
                                        break;
                                }
                        }

                        if (tempHF != null && !dominated) {
                                RID tempRid = new RID();
                                Scan scanTempHF = new Scan(tempHF);
                                while (true) {
                                        Tuple tempHFTuple = scanTempHF.getNext(tempRid);

                                        if (tempHFTuple == null) {
                                                break;
                                        }

                                        tempHFTuple.setHdr(in1_len, _in1, t1_str_sizes_cls);

                                        if (TupleUtils.Dominates(tempHFTuple, _in1, currentOuter, _in1, in1_len, t1_str_sizes_cls, pref_list_cls, pref_list_length_cls)) {
                                                dominated = true;
                                                break;
                                        }
                                }
                                scanTempHF.closescan();
                        }

                        if (SystemDefs.JavabaseBM.getNumBuffers() - SystemDefs.JavabaseBM.getNumUnpinnedBuffers() >= n_buf_pgs) {
                                SystemDefs.JavabaseBM.flushPages();
                        }

                        if (!dominated) {
                                Tuple temp = new Tuple(currentOuter);
                                if (inner.size() < maxWindowSize) {
                                        inner.add(temp);
                                } else {

                                        if (tempHF == null) {
                                                tempHF = new Heapfile(sortFirstHFName);
                                        }

                                        for (Tuple innerTuple : inner) {
                                                byte [] innerBytes = innerTuple.returnTupleByteArray();
                                                try {
                                                        tempHF.insertRecord(innerBytes);
                                                } catch (Exception e) {
                                                        e.printStackTrace();
                                                }
                                        }

                                        inner.clear();
                                        inner.add(temp);
                                }

                                skylineTuple = currentOuter;
                                break;
                        }
                }

                return skylineTuple;
        }

        public void close()
                throws JoinsException,
                IOException,
                IndexException, SortException {
                if (!closeFlag) {
//                        sort.close();
                        inner.clear();
                        closeFlag = true;
                }
        }

}
