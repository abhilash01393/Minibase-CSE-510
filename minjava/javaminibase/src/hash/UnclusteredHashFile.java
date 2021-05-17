package hash;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import bufmgr.*;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.IOException;

public class UnclusteredHashFile extends HashFile {

    public UnclusteredHashFile(String filename) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException {
        super(filename,false);
    }

    public UnclusteredHashFile(String filename, int keyType, int keySize, int targetUtilization) throws GetFileEntryException, IOException, AddFileEntryException, ConstructPageException, InvalidSlotNumberException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        super(filename, keyType, keySize, targetUtilization,false);
    }

    public RID insertRecord(KeyClass key, RID rid) throws IOException, ConstructPageException, InvalidSlotNumberException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        return super.insertRecord(key, new UnclusteredHashRecord(key, rid));
    }

    public RID deleteRecord(KeyClass key, RID rid) throws IOException, ConstructPageException, InvalidSlotNumberException, InvalidTypeException, UnknowAttrType, TupleUtilsException, InvalidTupleSizeException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        return super.deleteRecord(key, new UnclusteredHashRecord(key, rid));
    }

    public UnclusteredHashFileScan newScan(KeyClass lowKey, KeyClass highKey) {
        UnclusteredHashFileScan scan = new UnclusteredHashFileScan();
        scan.hashFile = this;
        scan.lowKey = lowKey;
        scan.highKey = highKey;
        return scan;
    }
}
