package btree;

import global.*;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

public class BTClusteredFileScan implements GlobalConst {

    BTreeClusteredFile bfile;
    String treeFilename;     // B+ tree we're scanning
    BTClusteredLeafPage leafPage;   // leaf page containing current record
    RID curRid;       // position in current leaf; note: this is
    // the RID of the key/RID pair within the
    // leaf page.
    boolean didfirst;        // false only before getNext is called
    boolean deletedcurrent;  // true after deleteCurrent is called (read
    // by get_next, written by deleteCurrent).

    KeyClass endkey;    // if NULL, then go all the way right
    // else, stop when current record > this value.
    // (that is, implement an inclusive range
    // scan -- the only way to do a search for
    // a single value).
    int keyType;
    int keyIndex;
    int maxKeysize;

    short tupleFldCnt;
    AttrType[] tupleAttrType;
    short[] tupleStrSizes;

    public BTClusteredFileScan() {
    }

    /**
     * Iterate once (during a scan).
     *
     * @return null if done; otherwise next KeyDataEntry
     * @throws ScanIteratorException iterator error
     */
    public KeyDataEntry get_next(RID TupleRid)
            throws ScanIteratorException {

        KeyDataEntry entry;
        PageId nextpage;
        try {
            if (leafPage == null)
                return null;

            if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
                didfirst = true;
                deletedcurrent = false;
                entry = leafPage.getCurrent(curRid);
            } else {
                entry = leafPage.getNext(curRid);
            }

            while (entry == null) {
                nextpage = leafPage.getNextPage();
                SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
                if (nextpage.pid == INVALID_PAGE) {
                    leafPage = null;
                    return null;
                }

                leafPage = new BTClusteredLeafPage(nextpage, keyType, keyIndex, tupleFldCnt, tupleAttrType, tupleStrSizes);

                entry = leafPage.getFirst(curRid);
            }

            if (endkey != null)
                if (BT.keyCompare(entry.key, endkey) > 0) {
                    // went past right end of scan
                    SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
                    leafPage = null;
                    return null;
                }
            TupleRid.pageNo = curRid.pageNo;
            TupleRid.slotNo = curRid.slotNo;
            return entry;
        } catch (Exception e) {
//            e.printStackTrace();
//            throw new ScanIteratorException();
        }
        return null;
    }

    public RID get_next_rid()
            throws ScanIteratorException, IOException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidTypeException {
        RID tempRid = null;
        if (leafPage != null) {
//            Tuple record = leafPage.getRecord(curRid);
//            record.setHdr(tupleFldCnt, tupleAttrType, tupleStrSizes);
//            record.print(tupleAttrType);
            tempRid = curRid;

            RID testRid = new RID();
            get_next(testRid);
        }
        return tempRid;
    }


    /**
     * Delete currently-being-scanned(i.e., just scanned)
     * data entry.
     *
     * @throws ScanDeleteException delete error when scan
     */
    public void delete_current()
            throws ScanDeleteException {

        KeyDataEntry entry;
        try {
            if (leafPage == null) {
                System.out.println("No Record to delete!");
                throw new ScanDeleteException();
            }

            if ((deletedcurrent == true) || (didfirst == false))
                return;

            entry = leafPage.getCurrent(curRid);
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
            bfile.Delete(entry.key, ((ClusteredLeafData) entry.data).getData());
            leafPage = bfile.findRunStart(entry.key, curRid);

            deletedcurrent = true;
            return;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ScanDeleteException();
        }
    }

    /**
     * max size of the key
     *
     * @return the maxumum size of the key in BTFile
     */
    public int keysize() {
        return maxKeysize;
    }


    /**
     * destructor.
     * unpin some pages if they are not unpinned already.
     * and do some clearing work.
     *
     * @throws IOException                        error from the lower layer
     * @throws bufmgr.InvalidFrameNumberException error from the lower layer
     * @throws bufmgr.ReplacerException           error from the lower layer
     * @throws bufmgr.PageUnpinnedException       error from the lower layer
     * @throws bufmgr.HashEntryNotFoundException  error from the lower layer
     */
    public void DestroyBTreeFileScan()
            throws IOException, bufmgr.InvalidFrameNumberException, bufmgr.ReplacerException,
            bufmgr.PageUnpinnedException, bufmgr.HashEntryNotFoundException {
        if (leafPage != null) {
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
        }
        leafPage = null;
    }


}





