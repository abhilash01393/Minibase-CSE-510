package btree;

import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import heap.Tuple;

import java.io.IOException;


public class BTClusteredLeafPage extends BTSortedPage {

    private short tupleFldCnt;
    private AttrType[] tupleAttrType;
    private short[] tupleStrSizes;
    private int keyIndex;

    public BTClusteredLeafPage(PageId pageno, int keyType, int keyIndex, short tupleFldCnt, AttrType[] tupleAttrType, short[] tupleStrSizes)
            throws IOException,
            ConstructPageException {
        super(pageno, keyType, keyIndex);
        setType(NodeType.LEAF_CLUSTERED);
        this.tupleFldCnt = tupleFldCnt;
        this.tupleAttrType = tupleAttrType;
        this.tupleStrSizes = tupleStrSizes;
        this.keyIndex = keyIndex;
    }

    public BTClusteredLeafPage(Page page, int keyType, int keyIndex, short tupleFldCnt, AttrType[] tupleAttrType, short[] tupleStrSizes)
            throws IOException,
            ConstructPageException {
        super(page, keyType, keyIndex);
        setType(NodeType.LEAF_CLUSTERED);
        this.tupleFldCnt = tupleFldCnt;
        this.tupleAttrType = tupleAttrType;
        this.tupleStrSizes = tupleStrSizes;
        this.keyIndex = keyIndex;
    }

    public BTClusteredLeafPage(int keyType, int keyIndex, short tupleFldCnt, AttrType[] tupleAttrType, short[] tupleStrSizes)
            throws IOException,
            ConstructPageException {
        super(keyType, keyIndex);
        setType(NodeType.LEAF_CLUSTERED);
        this.tupleFldCnt = tupleFldCnt;
        this.tupleAttrType = tupleAttrType;
        this.tupleStrSizes = tupleStrSizes;
        this.keyIndex = keyIndex;
    }

    public RID insertRecord(KeyClass key, Tuple data)
            throws LeafInsertRecException {
        KeyDataEntry entry;

        try {
            entry = new KeyDataEntry(key, data);

            return insertRecord(entry);
        } catch (Exception e) {
            throw new LeafInsertRecException(e, "insert record failed");
        }
    } // end of insertRecord

    public KeyDataEntry getFirst(RID rid)
            throws IteratorException {

        KeyDataEntry entry;

        try {
            rid.pageNo = getCurPage();
            rid.slotNo = 0; // begin with first slot

            if (getSlotCnt() <= 0) {
                return null;
            }

            entry = BT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0),
                    keyType, keyIndex, NodeType.LEAF_CLUSTERED, tupleFldCnt, tupleAttrType, tupleStrSizes);

            return entry;
        } catch (Exception e) {
            throw new IteratorException(e, "Get first entry failed");
        }
    } // end of getFirst

    public KeyDataEntry getNext(RID rid)
            throws IteratorException {
        KeyDataEntry entry;
        int i;
        try {
            rid.slotNo++; //must before any return;
            i = rid.slotNo;

            if (rid.slotNo >= getSlotCnt()) {
                return null;
            }

            entry = BT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i),
                    keyType, keyIndex, NodeType.LEAF_CLUSTERED, tupleFldCnt, tupleAttrType, tupleStrSizes);

            return entry;
        } catch (Exception e) {
            throw new IteratorException(e, "Get next entry failed");
        }
    }

    public KeyDataEntry getCurrent(RID rid)
            throws IteratorException {
        rid.slotNo--;
        return getNext(rid);
    }

    public RID delEntry(KeyDataEntry dEntry)
            throws LeafDeleteException {
        KeyDataEntry entry;
        RID rid = new RID();

        try {
            for (entry = getFirst(rid); entry != null; entry = getNext(rid)) {
                if (entry.equals(dEntry)) {
                    if (super.deleteSortedRecord(rid) == false)
                        throw new LeafDeleteException(null, "Delete record failed");
                    return rid;
                }

            }
            return null;
        } catch (Exception e) {
            throw new LeafDeleteException(e, "delete entry failed");
        }

    } // end of delEntry

    boolean redistribute(BTClusteredLeafPage leafPage, BTIndexPage parentIndexPage,
                         int direction, KeyClass deletedKey)
            throws LeafRedistributeException {
        boolean st;
        // assertion: leafPage pinned
        try {
            if (direction == -1) { // 'this' is the left sibling of leafPage
                if ((getSlotLength(getSlotCnt() - 1) + available_space() + 8 /*  2*sizeof(slot) */) >
                        ((MAX_SPACE - DPFIXED) / 2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // move the last record to its sibling

                    // get the last record
                    KeyDataEntry lastEntry;
                    lastEntry = BT.getEntryFromBytes(getpage(), getSlotOffset(getSlotCnt() - 1)
                            , getSlotLength(getSlotCnt() - 1), keyType, keyIndex, NodeType.LEAF_CLUSTERED, tupleFldCnt, tupleAttrType, tupleStrSizes);


                    //get its sibling's first record's key for adjusting parent pointer
                    RID dummyRid = new RID();
                    KeyDataEntry firstEntry;
                    firstEntry = leafPage.getFirst(dummyRid);

                    // insert it into its sibling
                    leafPage.insertRecord(lastEntry);

                    // delete the last record from the old page
                    RID delRid = new RID();
                    delRid.pageNo = getCurPage();
                    delRid.slotNo = getSlotCnt() - 1;
                    if (deleteSortedRecord(delRid) == false)
                        throw new LeafRedistributeException(null, "delete record failed");


                    // adjust the entry pointing to sibling in its parent
                    if (deletedKey != null)
                        st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
                    else
                        st = parentIndexPage.adjustKey(lastEntry.key,
                                firstEntry.key);
                    if (st == false)
                        throw new LeafRedistributeException(null, "adjust key failed");
                    return true;
                }
            } else { // 'this' is the right sibling of pptr
                if ((getSlotLength(0) + available_space() + 8) > ((MAX_SPACE - DPFIXED) / 2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // move the first record to its sibling

                    // get the first record
                    KeyDataEntry firstEntry;
                    firstEntry = BT.getEntryFromBytes(getpage(), getSlotOffset(0),
                            getSlotLength(0), keyType, keyIndex,
                            NodeType.LEAF_CLUSTERED, tupleFldCnt, tupleAttrType, tupleStrSizes);

                    // insert it into its sibling
                    RID dummyRid = new RID();
                    leafPage.insertRecord(firstEntry);


                    // delete the first record from the old page
                    RID delRid = new RID();
                    delRid.pageNo = getCurPage();
                    delRid.slotNo = 0;
                    if (deleteSortedRecord(delRid) == false)
                        throw new LeafRedistributeException(null, "delete record failed");


                    // get the current first record of the old page
                    // for adjusting parent pointer.
                    KeyDataEntry tmpEntry;
                    tmpEntry = getFirst(dummyRid);


                    // adjust the entry pointing to itself in its parent
                    st = parentIndexPage.adjustKey(tmpEntry.key, firstEntry.key);
                    if (st == false)
                        throw new LeafRedistributeException(null, "adjust key failed");
                    return true;
                }
            }
        } catch (Exception e) {
            throw new LeafRedistributeException(e, "redistribute failed");
        }
    }
}

    
 





















