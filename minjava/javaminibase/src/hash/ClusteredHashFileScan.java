package hash;

import btree.KeyNotMatchException;
import bufmgr.*;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

public class ClusteredHashFileScan {
    ClusteredHashFile hashFile;
    KeyClass lowKey;
    KeyClass highKey;
    short tupleFldCnt;
    AttrType[] tupleAttrType;
    short[] tupleStrSizes;

    int bucketCount;
    RID currRid;
    RID currDataRid;
    ClusteredHashPage bucketPage;

    boolean didFirst;

    ClusteredDataPage currPage;
    ClusteredHashRecord currRecord;

    boolean equalityAnswerReturned;



    ClusteredHashFileScan() {
        bucketCount = 0;
        didFirst = false;
        equalityAnswerReturned = false;
    }

    public Tuple getNextTuple(RID tupleRid) throws IOException, InvalidTupleSizeException, InvalidTypeException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        getNext();
//        SystemDefs.JavabaseBM.flushPages();
        if (currDataRid == null) {
            return null;
        }
        tupleRid.pageNo = currDataRid.pageNo;
        tupleRid.slotNo = currDataRid.slotNo;
        Tuple tup = currPage.getTupleFromSlot(currDataRid.slotNo);
        tup.setHdr(tupleFldCnt, tupleAttrType, tupleStrSizes);
        return tup;
    }

    public RID getNext() throws IOException, InvalidTupleSizeException, InvalidTypeException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        if (currDataRid == null) {
            currRecord = getNextRecord();
//            System.out.println(currRecord);
            if(currRecord==null){
                return null;
            }
            currPage = new ClusteredDataPage(currRecord.getPageId());
            currDataRid = currPage.firstRecord();
            return currDataRid;
        }
        currDataRid = currPage.nextRecord(currDataRid);
        if (currDataRid != null) {
//            System.out.println("lol");
            return currDataRid;
        } else if (currPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//            System.out.println("lol1");
//            SystemDefs.JavabaseBM.unpinPage(currPage.getCurPage(), true);
            currPage = new ClusteredDataPage(currPage.getNextPage());
            currDataRid = currPage.firstRecord();
            return currDataRid;
        } else {
//            System.out.println("lol2");
            currRecord = getNextRecord();
            if (currRecord != null) {
//                System.out.println(currRecord);
//                SystemDefs.JavabaseBM.unpinPage(currPage.getCurPage(), true);
                currPage = new ClusteredDataPage(currRecord.getPageId());
                currDataRid = currPage.firstRecord();
                return currDataRid;
            } else {
                return null;
            }
        }
    }

    public ClusteredHashRecord getNextRecord() throws IOException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        if (lowKey != null && highKey != null && lowKey.equals(highKey)) {
            if (equalityAnswerReturned) {
                return null;
            }
            int bucketKey = lowKey.getHash() % hashFile.headerPage.getNValue();
            if (bucketKey < hashFile.headerPage.getNextValue()) {
                bucketKey = lowKey.getHash() % (2 * hashFile.headerPage.getNValue());
            }
            if (!didFirst) {
                PageId bucketPageId = new PageId(hashFile.buckets.get(bucketKey));
                bucketPage = new ClusteredHashPage(bucketPageId);
                currRid = bucketPage.firstRecord();
                didFirst = true;
            }

            while (true) {
                if (currRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(currRid.slotNo);
                    ClusteredHashRecord record = new ClusteredHashRecord(bytesRecord, hashFile.headerPage.getKeyType(), hashFile.headerPage.getKeySize());
                    currRid = bucketPage.nextRecord(currRid);
                    if (!record.getKey().equals(lowKey)) {
                        continue;
                    }
                    equalityAnswerReturned = true;
                    return record;
                }
                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//                    System.out.println("lol");
//                    SystemDefs.JavabaseBM.unpinPage(bucketPage.getCurPage(), true);
                    bucketPage = new ClusteredHashPage(bucketPage.getNextPage());
                    ;
//                    System.out.println(bucketPage.empty());
                    currRid = bucketPage.firstRecord();
                } else {
                    break;
                }
            }

        } else {
            while (bucketCount <= hashFile.headerPage.getBucketCount()) {
//            System.out.println("Bucket: " + bucketCount);
                if (!didFirst) {
                    PageId bucketPageId = new PageId(hashFile.buckets.get(bucketCount));
                    bucketPage = new ClusteredHashPage(bucketPageId);
                    currRid = bucketPage.firstRecord();
                    didFirst = true;
                }

                while (true) {
                    if (currRid != null) {
                        byte[] bytesRecord = bucketPage.getBytesFromSlot(currRid.slotNo);
                        ClusteredHashRecord record = new ClusteredHashRecord(bytesRecord, hashFile.headerPage.getKeyType(), hashFile.headerPage.getKeySize());
                        currRid = bucketPage.nextRecord(currRid);
                        if (lowKey != null) {
                            if (HashFile.keyCompare(lowKey, record.getKey()) > 0) {
                                continue;
                            }
                        }
                        if (highKey != null) {
                            if (HashFile.keyCompare(highKey, record.getKey()) < 0) {
                                continue;
                            }
                        }
                        return record;
                    }
                    if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//                    System.out.println("lol");
//                        SystemDefs.JavabaseBM.unpinPage(bucketPage.getCurPage(), true);
                        bucketPage = new ClusteredHashPage(bucketPage.getNextPage());
                        ;
//                    System.out.println(bucketPage.empty());
                        currRid = bucketPage.firstRecord();
                    } else {
                        break;
                    }
                }
//            System.out.println();
                didFirst = false;
                bucketCount++;
            }
        }

        return null;
    }
}