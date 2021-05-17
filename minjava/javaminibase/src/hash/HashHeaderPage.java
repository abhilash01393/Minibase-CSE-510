package hash;

import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import java.io.IOException;

class HashHeaderPage extends HFPage {
    private int n;
    private int next;
    private int bucketCount;
    private int targetUtilization;
    private int numOfRecords;
    private int pageCapacity;
    private int keyType;
    private int keySize;

    private final int nValueSlot = 0;
    private final int nextValueSlot = 1;
    private final int bucketCountSlot = 2;
    private final int targetUtilizationSlot = 3;
    private final int numOfRecordsSlot = 4;
    private final int pageCapacitySlot = 5;
    private final int keyTypeSlot = 6;
    private final int keySizeSlot = 7;
    private final int bucketSlotsStart = 8;

    public HashHeaderPage(PageId pageno)
    {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public HashHeaderPage( ) throws ConstructPageException {
        super();
        try{
            Page apage=new Page();
            PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
            if (pageId==null)
                throw new ConstructPageException("new page failed");
            this.init(pageId, apage);
        }
        catch (Exception e) {
            throw new ConstructPageException("construct header page failed");
        }
    }

    void initialiseFirstTime(int keyType, int keySize, int targetUtilization, boolean isClustered) throws IOException, ConstructPageException, InvalidSlotNumberException {
        byte[] tempData = new byte[4];

        n = 4;
        Convert.setIntValue(n, 0, tempData);
        this.insertRecord(tempData);

        next = 0;
        Convert.setIntValue(next, 0, tempData);
        this.insertRecord(tempData);

        bucketCount = -1;
        Convert.setIntValue(bucketCount, 0, tempData);
        this.insertRecord(tempData);

        Convert.setIntValue(targetUtilization, 0, tempData);
        this.insertRecord(tempData);
        this.targetUtilization = targetUtilization;

        numOfRecords = 0;
        Convert.setIntValue(numOfRecords, 0, tempData);
        this.insertRecord(tempData);

        HashPage tempPage = getNewHashPage(isClustered, HashPageType.HASH_BUCKET);
        pageCapacity = tempPage.getPageCapacity(keyType, keySize);
        Convert.setIntValue(pageCapacity, 0, tempData);
        this.insertRecord(tempData);

        Convert.setIntValue(keyType, 0, tempData);
        this.insertRecord(tempData);
        this.keyType = keyType;

        Convert.setIntValue(keySize, 0, tempData);
        this.insertRecord(tempData);
        this.keySize = keySize;

    }

    public HashPage getNewHashPage(boolean isClustered, short pageType) throws ConstructPageException {
        if (isClustered) {
            return new ClusteredHashPage(pageType);
        } else {
            return new UnclusteredHashPage(pageType);
        }
    }

    void initialiseAlreadyExisting() throws IOException {

        n = Convert.getIntValue(getSlotOffset(nValueSlot), data);
        next = Convert.getIntValue(getSlotOffset(nextValueSlot), data);
        bucketCount = Convert.getIntValue(getSlotOffset(bucketCountSlot), data);
        targetUtilization = Convert.getIntValue(getSlotOffset(targetUtilizationSlot), data);
        numOfRecords = Convert.getIntValue(getSlotOffset(numOfRecordsSlot), data);
        pageCapacity = Convert.getIntValue(getSlotOffset(pageCapacitySlot), data);
        keyType = Convert.getIntValue(getSlotOffset(keyTypeSlot), data);
        keySize = Convert.getIntValue(getSlotOffset(keySizeSlot), data);
    }

    void printAllSlotValues() throws IOException {
        System.out.println(Convert.getIntValue(getSlotOffset(nValueSlot), data));
        System.out.println(Convert.getIntValue(getSlotOffset(nextValueSlot), data));
        System.out.println(Convert.getIntValue(getSlotOffset(bucketCountSlot), data));
        System.out.println(Convert.getIntValue(getSlotOffset(targetUtilizationSlot), data));
        System.out.println(Convert.getIntValue(getSlotOffset(numOfRecordsSlot), data));
    }

//    public void updateAllBuckets() throws IOException {
//        int bucketSlot = numOfRecordsSlot + 1;
//        for (int i=0; i < buckets.size(); i++) {
//            Convert.setIntValue(i, getSlotOffset(bucketSlot), data);
//            Convert.setIntValue(buckets.get(i), getSlotOffset(bucketSlot) + 4, data);
//            bucketSlot++;
//        }
//    }

    public float getCurrentUtilization() {
//        System.out.println(pageCapacity);
        return ((float) numOfRecords/((float)pageCapacity*((float)bucketCount+1))) * 100;
    }

    public int getBucketSlotsStart() {
        return bucketSlotsStart;
    }

    public int getNValue() {
        return n;
    }

    public void setNValue(int n) throws IOException {
        Convert.setIntValue(n, getSlotOffset(nValueSlot), data);
        this.n = n;
    }

    public int getNextValue() {
        return next;
    }

    public void setNextValue(int next) throws IOException {
        Convert.setIntValue(next, getSlotOffset(nextValueSlot), data);
        this.next = next;
    }

    public int getBucketCount() {
        return bucketCount;
    }

    public void setBucketCount(int bucketCount) throws IOException {
        Convert.setIntValue(bucketCount, getSlotOffset(bucketCountSlot), data);
        this.bucketCount = bucketCount;
    }

    public int getTargetUtilization() {
        return targetUtilization;
    }

    public void setTargetUtilization(int targetUtilization) throws IOException {
        Convert.setIntValue(targetUtilization, getSlotOffset(targetUtilizationSlot), data);
        this.targetUtilization = targetUtilization;
    }

    public int getNumOfRecords() {
        return numOfRecords;
    }

    public void setNumOfRecords(int numOfRecords) throws IOException {
        Convert.setIntValue(numOfRecords, getSlotOffset(numOfRecordsSlot), data);
        this.numOfRecords = numOfRecords;
    }

    public int getPageCapacity() {
        return pageCapacity;
    }

    public void setPageCapacity(int pageCapacity) throws IOException {
        Convert.setIntValue(pageCapacity, getSlotOffset(pageCapacitySlot), data);
        this.pageCapacity = pageCapacity;
    }

    public int getKeyType() {
        return keyType;
    }

    public void setKeyType(int keyType) throws IOException {
        Convert.setIntValue(keyType, getSlotOffset(keyTypeSlot), data);
        this.keyType = keyType;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) throws IOException {
        Convert.setIntValue(keySize, getSlotOffset(keySizeSlot), data);
        this.keySize = keySize;
    }


    void setPageId(PageId pageno)
            throws IOException
    {
        setCurPage(pageno);
    }

    PageId getPageId()
            throws IOException
    {
        return getCurPage();
    }

    void  set_rootId( PageId rootID )
            throws IOException
    {
        setNextPage(rootID);
    };

    PageId get_rootId()
            throws IOException
    {
        return getNextPage();
    }
}
