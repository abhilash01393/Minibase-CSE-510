package hash;

import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

public class ClusteredHashPage extends HFPage implements HashPage {

    public ClusteredHashPage(PageId pageno)
    {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public ClusteredHashPage(short pageType) throws ConstructPageException {
        super();
        try{
            Page apage=new Page();
            PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
            if (pageId==null)
                throw new ConstructPageException("new page failed");
            this.init(pageId, apage);
            setType(pageType);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new ConstructPageException("construct header page failed");

        }
    }

    public int getPageCapacity(int keyType, int keySize) throws IOException, InvalidSlotNumberException, ConstructPageException {
        int capacity = 0;
        KeyClass tempKey;
        if (keyType == AttrType.attrInteger) {
            tempKey = new IntegerKey(0);
        } else if (keyType == AttrType.attrReal) {
            tempKey = new FloatKey(0);
        } else {
            tempKey = new StringKey("");
        }
        tempKey.setKeyType(keyType);
        tempKey.setKeySize(keySize);
//        System.out.println("blah: " + this.getSlotCnt());
        RID tempRid = insertRecord(tempKey, new ClusteredHashRecord(tempKey, new Tuple()));

        capacity++;
        while (tempRid != null) {
            tempRid = insertRecord(tempKey, new ClusteredHashRecord(tempKey, new Tuple()));
            capacity++;
        }
//        System.out.println("blah: " + this.available_space());
        return --capacity;
    }

    public void deleteRecord ( RID rid ) throws IOException, InvalidSlotNumberException {
        super.deleteRecord(rid);
        // if overflow page gets empty, delete the page
        if (this.getType() == HashPageType.HASH_OVERFLOW) {
            if (this.empty()) {
                if (this.getNextPage().pid != HFPage.INVALID_PAGE) {
                    ClusteredHashPage nextPage = new ClusteredHashPage(this.getNextPage());
                    nextPage.setPrevPage(this.getPrevPage());
                    ClusteredHashPage prevPage = new ClusteredHashPage(this.getPrevPage());
                    prevPage.setNextPage(this.getNextPage());
                } else {
                    ClusteredHashPage prevPage = new ClusteredHashPage(this.getPrevPage());
                    prevPage.setNextPage(new PageId(HFPage.INVALID_PAGE));
                }
            }
        }
    }

    public RID insertRecord(KeyClass key, HashRecord data) throws IOException {
        byte[] tempData = data.getBytesFromRecord();
        return super.insertRecord(tempData);
    }

    public PageId getNextPage() throws IOException {
        return super.getNextPage();
    }

    public PageId getCurPage() throws IOException {
        return super.getCurPage();
    }

    public void setNextPage(PageId pageId) throws IOException {
        super.setNextPage(pageId);
    }

    public byte[] getBytesFromSlot(int slotNo) throws IOException {
        int slotOffset = getSlotOffset(slotNo);
        int slotLength = getSlotLength(slotNo);
        byte[] tempData = new byte[slotLength];
        System.arraycopy(data, slotOffset, tempData, 0, slotLength);
        return tempData;
    }
}
