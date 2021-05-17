package hash;

import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

public class ClusteredDataPage extends HFPage {
    public ClusteredDataPage(PageId pageno)
    {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public ClusteredDataPage(short pageType) throws ConstructPageException {
        super();
        try{
            Page apage=new Page();
            PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
            if (pageId==null)
                throw new ConstructPageException("new page failed");
            this.init(pageId, apage);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new ConstructPageException("construct header page failed");

        }
    }

    public Tuple getTupleFromSlot(int slotNo) throws IOException {
        int slotOffset = getSlotOffset(slotNo);
        int slotLength = getSlotLength(slotNo);
        byte[] tempData = new byte[slotLength];
        System.arraycopy(data, slotOffset, tempData, 0, slotLength);
        Tuple tup = new Tuple(tempData, 0, slotLength);
        return tup;
    }

    public void deleteRecord ( RID rid ) throws IOException, InvalidSlotNumberException {
        super.deleteRecord(rid);
        // if overflow page gets empty, delete the page
        if (this.getType() == HashPageType.CLUSTERED_DATA_OVERFLOW) {
            if (this.empty()) {
                if (this.getNextPage().pid != HFPage.INVALID_PAGE) {
                    ClusteredDataPage nextPage = new ClusteredDataPage(this.getNextPage());
                    nextPage.setPrevPage(this.getPrevPage());
                    ClusteredDataPage prevPage = new ClusteredDataPage(this.getPrevPage());
                    prevPage.setNextPage(this.getNextPage());
                } else {
                    ClusteredDataPage prevPage = new ClusteredDataPage(this.getPrevPage());
                    prevPage.setNextPage(new PageId(HFPage.INVALID_PAGE));
                }
            }
        }
    }
}
