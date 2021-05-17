package hash;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import bufmgr.*;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.IOException;

public class ClusteredHashFile extends HashFile {
    private short tupleFldCnt;
    private AttrType[] tupleAttrType;
    private short[] tupleStrSizes;

    public ClusteredHashFile(String filename, short tupleFldCnt,
                              AttrType[] tupleAttrType, short[] tupleStrSizes) throws ConstructPageException, AddFileEntryException, GetFileEntryException, InvalidSlotNumberException, IOException {
        super(filename,true);
        this.tupleFldCnt = tupleFldCnt;
        this.tupleAttrType = tupleAttrType;
        this.tupleStrSizes = tupleStrSizes;
    }

    public ClusteredHashFile(String filename, int targetUtilization, int keyType, int keySize, short tupleFldCnt,
                             AttrType[] tupleAttrType, short[] tupleStrSizes) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        super(filename, keyType, keySize, targetUtilization, true);
        this.tupleFldCnt = tupleFldCnt;
        this.tupleAttrType = tupleAttrType;
        this.tupleStrSizes = tupleStrSizes;
    }

    public RID insertRecord(KeyClass key, Tuple data) throws IOException, ConstructPageException, InvalidSlotNumberException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        return insertRecord(key, new ClusteredHashRecord(key, data));
    }

    public RID deleteRecord(KeyClass key, Tuple data) throws IOException, ConstructPageException, InvalidSlotNumberException, InvalidTypeException, UnknowAttrType, TupleUtilsException, InvalidTupleSizeException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
       return deleteRecord(key, new ClusteredHashRecord(key, data));
    }

    public RID insertRecord(KeyClass key, HashRecord data) throws IOException, ConstructPageException, InvalidSlotNumberException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        key.setKeyType(headerPage.getKeyType());
        key.setKeySize(headerPage.getKeySize());
//        SystemDefs.JavabaseBM.flushPages();

        int bucketKey = key.getHash() % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key.getHash() % (2 * headerPage.getNValue());
        }

        boolean dataPageFound = false;
        PageId dataPageId = null;
        int bucketPageId = buckets.get(bucketKey);
        ClusteredHashPage bucketPage = new ClusteredHashPage(new PageId(bucketPageId));
        RID tempRid = bucketPage.firstRecord();
        RID recordRid = null;
        while (true) {
            while (tempRid != null) {
                byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                ClusteredHashRecord record = new ClusteredHashRecord(bytesRecord, headerPage.getKeyType(), headerPage.getKeySize());
                dataPageFound = record.getKey().equals(key);
                if (dataPageFound) {
                    dataPageId = record.getPageId();
                    break;
                }
                tempRid = bucketPage.nextRecord(tempRid);
            }
            if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                SystemDefs.JavabaseBM.unpinPage(bucketPage.getCurPage(), true);
                bucketPage = new ClusteredHashPage(bucketPage.getNextPage());
                tempRid = bucketPage.firstRecord();
            } else {
                break;
            }

            if (dataPageFound) {
                break;
            }
        }
        if (dataPageFound) {
            ClusteredDataPage dataPage = new ClusteredDataPage(dataPageId);
            recordRid = insertRecordToDataPage(dataPage, ((ClusteredHashRecord)data).getTupleBytes());
//            SystemDefs.JavabaseBM.unpinPage(dataPage.getCurPage(), true);
        } else {
            ClusteredDataPage dataPage = new ClusteredDataPage(HashPageType.CLUSTERED_DATA);
            recordRid = insertRecordToDataPage(dataPage, ((ClusteredHashRecord)data).getTupleBytes());
            ((ClusteredHashRecord)data).setPageId(dataPage.getCurPage());
//            System.out.println("Adding page");
            super.insertRecord(key, data);
        }
        SystemDefs.JavabaseBM.unpinPage(bucketPage.getCurPage(), true);
        return recordRid;
    }

    public RID insertRecordToDataPage(ClusteredDataPage page, byte[] data) throws IOException, ConstructPageException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        RID recordRid = page.insertRecord(data);
        // record id is null if insufficient space
        boolean recordInserted = recordRid != null;
        if (recordInserted) {
            SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), true);
        }

        // if insufficient space, try to insert in overflow page if it is already there, else add overflow page
        ClusteredDataPage prevPage = page;
        ClusteredDataPage nextPage;
        while (!recordInserted) {
            // overflow pages are already there
            PageId nextPageId = prevPage.getNextPage();
            nextPage = null;
            if (nextPageId.pid != HFPage.INVALID_PAGE) {
                SystemDefs.JavabaseBM.unpinPage(prevPage.getCurPage(), true);
                nextPage = new ClusteredDataPage(nextPageId);
                recordRid = nextPage.insertRecord(data);
                recordInserted = recordRid != null;
                if (recordInserted) {
                    SystemDefs.JavabaseBM.unpinPage(nextPage.getCurPage(), true);
                    break;
                }
            } else {
                break;
            }
            // if even this page does not have space, try to go to next page
            if (nextPage != null) {
                prevPage = nextPage;
            } else {
                SystemDefs.JavabaseBM.unpinPage(nextPage.getCurPage(), true);
                break;
            }
        }

        // if record still not inserted, add overflow page and insert into it
        if (!recordInserted) {
            ClusteredDataPage overflowPage = new ClusteredDataPage(HashPageType.CLUSTERED_DATA_OVERFLOW);
            prevPage.setNextPage(overflowPage.getCurPage());
            overflowPage.setPrevPage(prevPage.getCurPage());
            recordRid = overflowPage.insertRecord(data);
            SystemDefs.JavabaseBM.unpinPage(overflowPage.getCurPage(), true);
        }
        return recordRid;
    }

    public RID deleteRecord(KeyClass key, HashRecord data) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidTypeException, UnknowAttrType, TupleUtilsException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        int bucketKey = key.getHash() % headerPage.getNValue();
        if (bucketKey < headerPage.getNextValue()) {
            bucketKey = key.getHash() % (2 * headerPage.getNValue());
        }
//        System.out.println("Key to delete: " + key + " Going to delete from bucket: " + bucketKey);
        boolean dataPageFound = false;
        PageId dataPageId = null;
        int bucketPageId = buckets.get(bucketKey);
        ClusteredHashPage bucketPage = new ClusteredHashPage(new PageId(bucketPageId));
        RID tempRid = bucketPage.firstRecord();
        RID returnDataRid = null;
        while (true) {
            while (tempRid != null) {
                byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                ClusteredHashRecord record = new ClusteredHashRecord(bytesRecord, headerPage.getKeyType(), headerPage.getKeySize());
                dataPageFound = record.getKey().equals(key);
                if (dataPageFound) {
                    dataPageId = record.getPageId();
                    break;
                }
                tempRid = bucketPage.nextRecord(tempRid);
            }
            if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                bucketPage = new ClusteredHashPage(bucketPage.getNextPage());
                tempRid = bucketPage.firstRecord();
            } else {
                break;
            }

            if (dataPageFound) {
                break;
            }
        }
        if (dataPageFound) {
            // try to find the record
            ClusteredDataPage dataPage = new ClusteredDataPage(dataPageId);
            ClusteredDataPage firstDataPage = dataPage;
            RID tempDataRid  = dataPage.firstRecord();
            ((ClusteredHashRecord)data).setPageId(dataPageId);
            boolean recordFound = false;
            while (true) {
                while (tempDataRid != null) {
                    Tuple tup = dataPage.getTupleFromSlot(tempDataRid.slotNo);
                    tup.setHdr(tupleFldCnt, tupleAttrType, tupleStrSizes);
                    recordFound = TupleUtils.Equal(((ClusteredHashRecord) data).getData(), tup, tupleAttrType, tupleFldCnt);
                    if (recordFound) {
                        System.out.print("Deleting Record: ");
                        tup.print(tupleAttrType);
                        dataPage.deleteRecord(tempDataRid);
                        returnDataRid = tempDataRid;
                        SystemDefs.JavabaseBM.unpinPage(dataPage.getCurPage(), true);
                        break;
                    }
                    tempDataRid = dataPage.nextRecord(tempDataRid);
                }
                if (dataPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                    dataPage = new ClusteredDataPage(dataPage.getNextPage());
                    tempDataRid = dataPage.firstRecord();
                } else {
                    break;
                }
                if (recordFound) {
                    break;
                }
            }
            boolean allEmpty = true;
            ClusteredDataPage checkDataPage = firstDataPage;
            while (true) {
                if (checkDataPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                    checkDataPage = new ClusteredDataPage(checkDataPage.getNextPage());
                } else {
                    break;
                }
                if (!checkDataPage.empty()) {
                    allEmpty = false;
                    break;
                }
            }
            if (allEmpty) {
                super.deleteRecord(key, data);
            }
        }
        return returnDataRid;
    }

    public ClusteredHashFileScan newScan(KeyClass lowKey, KeyClass highKey) {
        ClusteredHashFileScan scan = new ClusteredHashFileScan();
        scan.hashFile = this;
        scan.lowKey = lowKey;
        scan.highKey = highKey;
        scan.tupleFldCnt = tupleFldCnt;
        scan.tupleAttrType = tupleAttrType;
        scan.tupleStrSizes = tupleStrSizes;
        return scan;
    }

    public void printIndex() throws IOException, InvalidTupleSizeException, InvalidTypeException {
        for (int i = 0; i <= headerPage.getBucketCount(); i++) {
            PageId bucketPageId = new PageId(buckets.get(i));
            HashPage bucketPage = getHashPage(bucketPageId);
            System.out.println("Bucket: " + i);
            RID tempRid = bucketPage.firstRecord();
            while (true) {
                while (tempRid != null) {
                    byte[] bytesRecord = bucketPage.getBytesFromSlot(tempRid.slotNo);
                    HashRecord record = getHashRecord(bytesRecord);
                    System.out.print(record.toString() + ", ");

                    ClusteredDataPage dataPage = new ClusteredDataPage(((ClusteredHashRecord)record).getPageId());
                    RID tempDataRid  = dataPage.firstRecord();
                    while (true) {
                        while (tempDataRid != null) {
                            Tuple tup = dataPage.getTupleFromSlot(tempDataRid.slotNo);
                            tup.setHdr(tupleFldCnt, tupleAttrType, tupleStrSizes);
//                            tup.print(tupleAttrType);
                            tempDataRid = dataPage.nextRecord(tempDataRid);
                        }
                        if (dataPage.getNextPage().pid != HFPage.INVALID_PAGE) {
                            dataPage = new ClusteredDataPage(dataPage.getNextPage());
                            tempDataRid = dataPage.firstRecord();
                        } else {
                            break;
                        }
                    }

                    tempRid = bucketPage.nextRecord(tempRid);
                }

                if (bucketPage.getNextPage().pid != HFPage.INVALID_PAGE) {
//                    System.out.println("lol");
                    bucketPage = getHashPage(bucketPage.getNextPage());
//                    System.out.println(bucketPage.empty());
                    tempRid = bucketPage.firstRecord();
                } else {
                    break;
                }
            }
            System.out.println();
        }
    }

}
