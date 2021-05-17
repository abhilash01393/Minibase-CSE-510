package hash;

import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import java.io.IOException;

public class UnclusteredHashPage extends HFPage implements HashPage {

    public UnclusteredHashPage(PageId pageno)
    {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public UnclusteredHashPage(short pageType) throws ConstructPageException {
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

    public void deleteRecord ( RID rid ) throws IOException, InvalidSlotNumberException {
        super.deleteRecord(rid);
        // if overflow page gets empty, delete the page
        if (this.getType() == HashPageType.HASH_OVERFLOW) {
            if (this.empty()) {
                if (this.getNextPage().pid != HFPage.INVALID_PAGE) {
                    UnclusteredHashPage nextPage = new UnclusteredHashPage(this.getNextPage());
                    nextPage.setPrevPage(this.getPrevPage());
                    UnclusteredHashPage prevPage = new UnclusteredHashPage(this.getPrevPage());
                    prevPage.setNextPage(this.getNextPage());
                } else {
                    UnclusteredHashPage prevPage = new UnclusteredHashPage(this.getPrevPage());
                    prevPage.setNextPage(new PageId(HFPage.INVALID_PAGE));
                }
            }
        }
    }

    public int getPageCapacity(int keyType, int keySize) throws IOException, InvalidSlotNumberException {
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
        RID tempRid = insertRecord(tempKey, new UnclusteredHashRecord(tempKey, new RID(new PageId(1), 1 )));

        capacity++;
        while (tempRid != null) {
            tempRid = insertRecord(tempKey, new UnclusteredHashRecord(tempKey, new RID(new PageId(1), 1 )));
            capacity++;
        }
//        System.out.println("blah: " + this.available_space());
        return --capacity;
    }

    public byte[] getBytesFromSlot(int slotNo) throws IOException {
        int slotOffset = getSlotOffset(slotNo);
        int slotLength = getSlotLength(slotNo);
        byte[] tempData = new byte[slotLength];
        System.arraycopy(data, slotOffset, tempData, 0, slotLength);
        return tempData;
    }

    public RID insertRecord(KeyClass key, HashRecord data) throws IOException {
        byte[] tempData = data.getBytesFromRecord();
        return super.insertRecord(tempData);
    }

    public void setNextPage(PageId pageId) throws IOException {
        super.setNextPage(pageId);
    }
}
