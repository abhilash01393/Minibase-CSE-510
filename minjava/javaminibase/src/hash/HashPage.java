package hash;

import global.PageId;
import global.RID;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import java.io.IOException;

public interface HashPage {
    RID insertRecord(KeyClass key, HashRecord data) throws IOException;
    PageId getNextPage() throws IOException;
    PageId getCurPage() throws IOException;
    void setNextPage(PageId pageId) throws IOException;
    RID firstRecord() throws IOException;
    byte[] getBytesFromSlot(int slotNo) throws IOException;
    void deleteRecord(RID tempRid) throws IOException, InvalidSlotNumberException;
    RID insertRecord(byte[] bytesRecord) throws IOException;
    RID nextRecord(RID tempRid) throws IOException;
    void setPrevPage(PageId curPage) throws IOException;
    boolean empty() throws IOException;
    int getPageCapacity(int keyType, int keySize) throws IOException, InvalidSlotNumberException, ConstructPageException;
}
