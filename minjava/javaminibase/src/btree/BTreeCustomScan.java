package btree;

import java.io.IOException;

import global.Convert;
import global.PageId;
import global.RID;
import heap.*;

public class BTreeCustomScan {


  private Heapfile heapFile;

  public Heapfile getHeapFile() {
    return heapFile;
  }

  public BTreeCustomScan(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
    heapFile = new Heapfile(new String("pruned" + name));
  }


  public static RID getRID(byte[] bytearr) throws IOException {
    int slotNo = Convert.getIntValue(0, bytearr);
    int pageNo = Convert.getIntValue(4, bytearr);
    return new RID(new PageId(pageNo), slotNo);
  }

  public int getIndexForRid(RID rid1) throws InvalidTupleSizeException, IOException {
    int found = -1;
    Scan openScan = heapFile.openScan();
    RID rid = new RID();
    Tuple tuple;
    boolean end = false;
    int i = 0;
    while (!end) {
      tuple = openScan.getNext(rid);
      if (tuple == null) {
        end = true;
        break;
      }
      RID currRid = getRID(tuple.returnTupleByteArray());
      if (currRid.equals(rid1)) {
        end = true;
        found = i;
        break;
      }
      i++;
    }
    openScan.closescan();
    return found;

  }

  public RID addRecord(RID rid) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException {
    byte[] byteArray = new byte[8];
    rid.writeToByteArray(byteArray, 0);
    return heapFile.insertRecord(byteArray);
  }

  public void destroy() {
    try {
      this.heapFile.deleteFile();
    } catch (Exception e) {
      System.err.println("Exception while deleting heapfile \n" + e.getMessage());
    }
  }
}
