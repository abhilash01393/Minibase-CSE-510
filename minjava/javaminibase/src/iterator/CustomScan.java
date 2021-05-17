package iterator;

import btree.*;
import bufmgr.*;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import hash.ClusteredHashFile;
import hash.ClusteredHashFileScan;
import heap.*;
import index.IndexException;

import java.io.IOException;

public class CustomScan extends Iterator {
    public static final short CLUSTERED_HASH = 0;
    public static final short CLUSTERED_BTREE = 1;
    public static final short NO_INDEX = 4;
    private static AttrType[] indexAttrTypes;
    private static short[] indexAttrSizes;
    private static AttrType[] metaAttrTypes;
    private static short[] metaAttrSizes;
    private static Tuple indexTuple;
    private static Tuple metaTuple;
    public static final String INDEX_FILE_NAME = "indexCatalog";
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private static short attrStringSize = 32;
    private String relName;
    private static String METAFILE_POSTFIX = "-meta";
    private static int nColumns;
    private static AttrType[] attrType;
    private static short[] attrSizes;
    private static String[] attrNames;
    private static FldSpec[] projlist;
    private static RID rid;
    private int indexType;
    private FileScan fscan;
    private BTClusteredFileScan btscan;
    private ClusteredHashFileScan hscan;
    private BTreeClusteredFile bTreeClusteredFile;
    private ClusteredHashFile hashFile;

    public CustomScan(String relName) throws IOException, InvalidTypeException, WrongPermat, JoinsException, PredEvalException, UnknowAttrType, PageNotReadException, InvalidTupleSizeException, FieldNumberOutOfBoundException, FileScanException, TupleUtilsException, InvalidRelation, ConstructPageException, GetFileEntryException, PinPageException, IteratorException, UnpinPageException, KeyNotMatchException, InvalidSlotNumberException, hash.ConstructPageException, AddFileEntryException {
        this.relName = relName;
        initMetaAndIndexAttrs();
        indexType = findIfIndexExists(relName);
        getTableAttrsAndType(relName);

        if (indexType == NO_INDEX) {
            fscan = new FileScan(relName, attrType, attrSizes, (short) nColumns, nColumns, projlist, null);
        } else if (indexType == CLUSTERED_BTREE || indexType == (short) 5) {
            bTreeClusteredFile = new BTreeClusteredFile(relName, (short) nColumns, attrType, attrSizes);
            btscan = bTreeClusteredFile.new_scan(null, null);
        } else if (indexType == CLUSTERED_HASH) {
            hashFile = new ClusteredHashFile(relName, (short) nColumns, attrType, attrSizes);
            hscan = hashFile.newScan(null, null);
//            hashFile.printIndex();
        }
    }

    public CustomScan(String relName, String low_key, String high_key) throws IOException, InvalidTypeException, WrongPermat, JoinsException, PredEvalException, UnknowAttrType, PageNotReadException, InvalidTupleSizeException, FieldNumberOutOfBoundException, FileScanException, TupleUtilsException, InvalidRelation, ConstructPageException, GetFileEntryException, PinPageException, IteratorException, UnpinPageException, KeyNotMatchException, InvalidSlotNumberException, hash.ConstructPageException, AddFileEntryException {
        this.relName = relName;
        initMetaAndIndexAttrs();
        indexType = findIfIndexExists(relName);
        getTableAttrsAndType(relName);

        if (indexType == NO_INDEX) {
            fscan = new FileScan(relName, attrType, attrSizes, (short) nColumns, nColumns, projlist, null);
        } else if (indexType == CLUSTERED_BTREE || indexType == (short) 5) {
            BTreeClusteredFile bTreeClusteredFile = new BTreeClusteredFile(relName, (short) nColumns, attrType, attrSizes);
            btscan = bTreeClusteredFile.new_scan(new StringKey(low_key), new StringKey(high_key));
        } else if (indexType == CLUSTERED_HASH) {
            ClusteredHashFile hashFile = new ClusteredHashFile(relName, (short) nColumns, attrType, attrSizes);
            hscan = hashFile.newScan(new hash.StringKey(low_key), new hash.StringKey(high_key));
        }
    }

    public CustomScan(String relName, Integer low_key, Integer high_key) throws IOException, InvalidTypeException, WrongPermat, JoinsException, PredEvalException, UnknowAttrType, PageNotReadException, InvalidTupleSizeException, FieldNumberOutOfBoundException, FileScanException, TupleUtilsException, InvalidRelation, ConstructPageException, GetFileEntryException, PinPageException, IteratorException, UnpinPageException, KeyNotMatchException, InvalidSlotNumberException, hash.ConstructPageException, AddFileEntryException {
        this.relName = relName;
        initMetaAndIndexAttrs();
        indexType = findIfIndexExists(relName);
        getTableAttrsAndType(relName);

        if (indexType == NO_INDEX) {
            fscan = new FileScan(relName, attrType, attrSizes, (short) nColumns, nColumns, projlist, null);
        } else if (indexType == CLUSTERED_BTREE || indexType == (short) 5) {
            BTreeClusteredFile bTreeClusteredFile = new BTreeClusteredFile(relName, (short) nColumns, attrType, attrSizes);
            btscan = bTreeClusteredFile.new_scan(new IntegerKey(low_key), new IntegerKey(high_key));
        } else if (indexType == CLUSTERED_HASH) {
            ClusteredHashFile hashFile = new ClusteredHashFile(relName, (short) nColumns, attrType, attrSizes);
            hscan = hashFile.newScan(new hash.IntegerKey(low_key), new hash.IntegerKey(high_key));
        }
    }

    public CustomScan(String relName, Float low_key, Float high_key) throws IOException, InvalidTypeException, WrongPermat, JoinsException, PredEvalException, UnknowAttrType, PageNotReadException, InvalidTupleSizeException, FieldNumberOutOfBoundException, FileScanException, TupleUtilsException, InvalidRelation, ConstructPageException, GetFileEntryException, PinPageException, IteratorException, UnpinPageException, KeyNotMatchException, InvalidSlotNumberException, hash.ConstructPageException, AddFileEntryException {
        this.relName = relName;
        initMetaAndIndexAttrs();
        indexType = findIfIndexExists(relName);
        getTableAttrsAndType(relName);

        if (indexType == NO_INDEX) {
            fscan = new FileScan(relName, attrType, attrSizes, (short) nColumns, nColumns, projlist, null);
        } else if (indexType == CLUSTERED_BTREE || indexType == (short) 5) {
            BTreeClusteredFile bTreeClusteredFile = new BTreeClusteredFile(relName, (short) nColumns, attrType, attrSizes);
            btscan = bTreeClusteredFile.new_scan(new FloatKey(low_key), new FloatKey(high_key));
        } else if (indexType == CLUSTERED_HASH) {
            ClusteredHashFile hashFile = new ClusteredHashFile(relName, (short) nColumns, attrType, attrSizes);
            hscan = hashFile.newScan(new hash.FloatKey(low_key), new hash.FloatKey(high_key));
        }
    }

    public String getRelName(){
        return this.relName;
    }

    public Tuple get_next() throws IOException, JoinsException, FieldNumberOutOfBoundException, PageNotReadException, WrongPermat, InvalidTypeException, InvalidTupleSizeException, PredEvalException, UnknowAttrType, ScanIteratorException, PageUnpinnedException, ReplacerException, PageNotFoundException, PagePinnedException, BufMgrException, InvalidFrameNumberException, HashEntryNotFoundException, HashOperationException {
        rid = new RID();
//        PCounter.initialize();
        if (indexType == NO_INDEX) {
            return fscan.get_next();
        } else if (indexType == CLUSTERED_BTREE || indexType == (short) 5) {
            RID tempRid = new RID();
            KeyDataEntry data = btscan.get_next(tempRid);
            if (data == null) {
                return null;
            }
            return (((ClusteredLeafData) data.data).getData());
        } else if (indexType == CLUSTERED_HASH) {
            RID tempRid = new RID();
            return hscan.getNextTuple(tempRid);
        }
        return null;
    }

    public void close() throws IOException, JoinsException, SortException, IndexException, HashEntryNotFoundException, ReplacerException, InvalidFrameNumberException, PageUnpinnedException {
        if (indexType == NO_INDEX) {
            fscan.close();
        } else if (indexType == CLUSTERED_BTREE || indexType == (short) 5) {
            bTreeClusteredFile.close();
        } else if (indexType == CLUSTERED_HASH) {
            hashFile.close();
        }
    }

    // Return index type
    private int findIfIndexExists(String relName) throws IOException, InvalidTypeException, PageNotReadException, JoinsException, PredEvalException, WrongPermat, UnknowAttrType, InvalidTupleSizeException, FieldNumberOutOfBoundException {
        // Read data and construct tuples
        FileScan fscan = null;
        int indexType = NO_INDEX;

        FldSpec[] projections = new FldSpec[indexAttrTypes.length];

        for (int i = 0; i < indexAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(INDEX_FILE_NAME, indexAttrTypes, indexAttrSizes, (short) indexAttrTypes.length, indexAttrTypes.length, projections, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Tuple t = null;
        t = fscan.get_next();

        while (t != null) {
            if (t.getStrFld(1).equals(relName) && (t.getIntFld(3) == (short) 5 || t.getIntFld(3) == CLUSTERED_BTREE || t.getIntFld(3) == CLUSTERED_HASH)) {
                indexType = t.getIntFld(3);
                break;
            }
            t = fscan.get_next();
        }


        fscan.close();
        return indexType;
    }

    private void initMetaAndIndexAttrs() {
        // For Index
        short size;
        indexTuple = new Tuple();
        indexAttrTypes = new AttrType[4];

        indexAttrTypes[0] = new AttrType(AttrType.attrString); // For relName
        indexAttrTypes[1] = new AttrType(AttrType.attrString); // For attrName
        indexAttrTypes[2] = new AttrType(AttrType.attrInteger);// For indexType
        indexAttrTypes[3] = new AttrType(AttrType.attrInteger);// For keyIndex

        indexAttrSizes = new short[2];
        indexAttrSizes[0] = (short) attrStringSize;
        indexAttrSizes[1] = (short) attrStringSize;

        try {
            indexTuple.setHdr((short) 4, indexAttrTypes, indexAttrSizes);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to set tuple header!");
        }

        size = indexTuple.size();

        indexTuple = new Tuple(size);
        try {
            indexTuple.setHdr((short) 4, indexAttrTypes, indexAttrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }

        //For Attrs
        metaTuple = new Tuple();
        metaAttrTypes = new AttrType[3];

        metaAttrTypes[0] = new AttrType(AttrType.attrString); // For attrName
        metaAttrTypes[1] = new AttrType(AttrType.attrInteger);// For indexType
        metaAttrTypes[2] = new AttrType(AttrType.attrInteger);// For attrLen

        metaAttrSizes = new short[1];
        metaAttrSizes[0] = (short) attrStringSize;

        try {
            metaTuple.setHdr((short) 3, metaAttrTypes, metaAttrSizes);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to set tuple header!");

        }

        size = metaTuple.size();

        metaTuple = new Tuple(size);
        try {
            metaTuple.setHdr((short) 3, metaAttrTypes, metaAttrSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }
    }

    public void getTableAttrsAndType(String tableName) {
        // Read data and construct tuples
        FileScan fscan = null;

        FldSpec[] projections = new FldSpec[metaAttrTypes.length];

        for (int i = 0; i < metaAttrTypes.length; i++) {
            projections[i] = new FldSpec(rel, i + 1);
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        nColumns = 0;
        Tuple t = null;
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int stringColumns = 0;
        while (t != null) {
            try {
                if (t.getIntFld(2) == AttrType.attrString) {
                    stringColumns++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            nColumns++;

            try {
                t = fscan.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            fscan = new FileScan(tableName + METAFILE_POSTFIX, metaAttrTypes, metaAttrSizes, (short) metaAttrTypes.length, metaAttrTypes.length, projections, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        attrType = new AttrType[nColumns];
        attrSizes = new short[stringColumns];
        attrNames = new String[nColumns];
        try {
            t = fscan.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int i = 0, j = 0;
        while (t != null) {
            try {
                attrNames[i] = t.getStrFld(1);
                attrType[i] = new AttrType(t.getIntFld(2));
                if (t.getIntFld(2) == AttrType.attrString) {
                    attrSizes[j] = (short) t.getIntFld(3);
                    j++;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            i++;
            try {
                t = fscan.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        projlist = new FldSpec[nColumns];

        for (i = 0; i < nColumns; i++) {
            projlist[i] = new FldSpec(rel, i + 1);
        }

        // clean up
        try {
            fscan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
