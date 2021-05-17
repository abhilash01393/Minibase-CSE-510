package hash;

import bufmgr.*;
import global.AttrType;
import global.PageId;
import global.RID;
import heap.HFPage;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

public class UnclusteredHashFileScan {
    UnclusteredHashFile hashFile;
    KeyClass lowKey;
    KeyClass highKey;

    int bucketCount;
    RID currRid;
    UnclusteredHashPage bucketPage;

    boolean didFirst;


    UnclusteredHashFileScan() {
        bucketCount = 0;
        didFirst = false;
    }

    public UnclusteredHashRecord getNextRecord() throws IOException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        if (lowKey != null && highKey != null && lowKey.equals(highKey)) {
            int bucketKey = lowKey.getHash() % hashFile.headerPage.getNValue();
            if (bucketKey < hashFile.headerPage.getNextValue()) {
                bucketKey = lowKey.getHash() % (2 * hashFile.headerPage.getNValue());
            }
            if (!didFirst) {
                PageId bucketPageId = new PageId(hashFile.buckets.get(bucketKey));
                bucketPage = new UnclusteredHashPage(bucketPageId);
                currRid = bucketPage.firstRecord();
                didFirst = true;
            }

            while (true) {
                if (currRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(currRid.slotNo);
                    UnclusteredHashRecord record = new UnclusteredHashRecord(bytesRecord, hashFile.headerPage.getKeyType(), hashFile.headerPage.getKeySize());
                    currRid = bucketPage.nextRecord(currRid);
                    if (!record.getKey().equals(lowKey)) {
                        continue;
                    }
                    return record;
                }
                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//                    System.out.println("lol");
//                    SystemDefs.JavabaseBM.unpinPage(bucketPage.getCurPage(), true);
                    bucketPage = new UnclusteredHashPage(bucketPage.getNextPage());
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
                    bucketPage = new UnclusteredHashPage(bucketPageId);
                    currRid = bucketPage.firstRecord();
                    didFirst = true;
                }

                while (true) {
                    if (currRid != null) {
                        byte[] bytesRecord = bucketPage.getBytesFromSlot(currRid.slotNo);
                        UnclusteredHashRecord record = new UnclusteredHashRecord(bytesRecord, hashFile.headerPage.getKeyType(), hashFile.headerPage.getKeySize());
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
                        bucketPage = new UnclusteredHashPage(bucketPage.getNextPage());
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