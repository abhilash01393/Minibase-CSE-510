package btree;

import global.AttrType;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import heap.*;
import index.IndexScan;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.IOException;

public class BTreeUtil {

  public static String getAttrBTreeFileName(int i) {
    return "BTreeAttrIndex" + i;
  }

  public static String getHeapFileName(String fileFullyQualifiedPath) {
    String datafile = fileFullyQualifiedPath.substring(fileFullyQualifiedPath.lastIndexOf('/') + 1);
    String datafilename = datafile.substring(0, datafile.lastIndexOf("."));
    String heapFileName = "heap_" + datafilename;
    return heapFileName;

  }

  public static Tuple getEmptyTuple(AttrType[] attrType, short[] attrSizes) throws InvalidTypeException, InvalidTupleSizeException, IOException {
    Tuple tuple = new Tuple();
    tuple.setHdr((short) attrType.length, attrType, attrSizes);
    int size = tuple.size();
    tuple = new Tuple(size);
    tuple.setHdr((short) attrType.length, attrType, attrSizes);
    return tuple;
  }

  public static void createBtreesForPrefList(String relationName, Heapfile heapFile, AttrType[] attrType,
                                             short[] t1_str_sizes, int[] pref_attr_lst) throws Exception {

    BTreeFile[] bTreeFiles = new BTreeFile[pref_attr_lst.length];
    int i=0;
    for (int attr : pref_attr_lst) {
      String btreename = BTreeUtil.getAttrBTreeFileName(attr);
      bTreeFiles[i] = createIndex(btreename, new Scan(heapFile), attrType, t1_str_sizes, (attr));
      bTreeFiles[i].close();
      i++;
    }
    SystemDefs.JavabaseBM.flushPages();
  }

  private static BTreeFile createIndex(String btname, Scan scan, AttrType[] attrType, short[] attrSizes, int colNum) throws Exception {

    BTreeFile bTreeFile = new BTreeFile(btname, AttrType.attrReal, 4, 1/* delete */);

    RID rid = new RID();
    Tuple tuple = getEmptyTuple(attrType, attrSizes);
    float key = 0;
    // skip first row
//    scan.getNext(rid);
    Tuple tempTuple = scan.getNext(rid);

    while (tempTuple != null) {
      tuple.tupleCopy(tempTuple);
      key = tuple.getFloFld(colNum);
      bTreeFile.insert(new FloatKey(-key), rid);
      tempTuple = scan.getNext(rid);
    }
    scan.closescan();

    System.out.println(btname+" created.\n");

    return bTreeFile;
  }

  private static void scanBtree(String relationName, String btreeName, AttrType[] attrType, short[] attrSize,
                                int prefListLen, int fldNum) throws Exception {
    FldSpec[] outFlds = new FldSpec[5];
    RelSpec rel = new RelSpec(RelSpec.outer);
    for(int i=0; i<attrType.length; i++){
      outFlds[i] = new FldSpec(rel, i+1);
    }
    IndexScan indexScan = null;
    int in = prefListLen;
    int out = prefListLen;
    indexScan = new IndexScan(new IndexType(IndexType.B_Index), relationName, btreeName, attrType, attrSize,
            in, out, outFlds, null, fldNum, false);

    Tuple tuple = null;
    float key = 0;

    tuple = indexScan.get_next();
    while (tuple != null) {
      key = tuple.getFloFld(fldNum);
      tuple.print(attrType);
      tuple = indexScan.get_next();
    }
    indexScan.close();
  }

  public static IndexFile[] getBTrees(int[] pref_attr_lst) throws ConstructPageException, GetFileEntryException, PinPageException {
    BTreeFile[] btreeFileArray = new BTreeFile[pref_attr_lst.length];
    int i=0;
    for (int attr : pref_attr_lst) {
      btreeFileArray[i] = new BTreeFile(BTreeUtil.getAttrBTreeFileName(attr));
      i++;
    }

    return btreeFileArray;
  }
}
