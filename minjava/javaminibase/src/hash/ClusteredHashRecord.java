package hash;

import global.AttrType;
import global.Convert;
import global.PageId;
import global.RID;
import heap.Tuple;

import java.io.IOException;

public class ClusteredHashRecord implements HashRecord {

    public static final int RECORD_SIZE_WITHOUT_KEY = 4;

    private KeyClass key;
    private Tuple data;

    public void setPageId(PageId pageId) {
        this.pageId = pageId;
    }

    public PageId getPageId() {
        return pageId;
    }

    private PageId pageId;

    public ClusteredHashRecord(KeyClass key, Tuple data) throws ConstructPageException, IOException {
        this.key = key;
        this.data = data;
        this.pageId = new PageId();
    }

    public ClusteredHashRecord(byte[] recordBytes, int keyType, int keySize) throws IOException {
        if (keyType == AttrType.attrInteger) {
            int val = Convert.getIntValue(0, recordBytes);
            this.key = new IntegerKey(val);
        } else if (keyType == AttrType.attrReal) {
            float val = Convert.getFloValue(0, recordBytes);
            this.key = new FloatKey(val);
        } else {
            String val = Convert.getStrValue(0, recordBytes, keySize);
            this.key = new StringKey(val);
        }
        this.key.setKeyType(keyType);
        this.key.setKeySize(keySize);

        int pid = Convert.getIntValue(keySize, recordBytes);
        this.pageId = new PageId(pid);
    }

    public byte[] getTupleBytes() {
        return data.getTupleByteArray();
    }

    public byte[] getBytesFromRecord() throws IOException {
        byte[] data = new byte[RECORD_SIZE_WITHOUT_KEY + key.getKeySize()];
        if (key.getKeyType() == AttrType.attrInteger) {
            Convert.setIntValue(((IntegerKey)key).getKey(), 0, data);
        } else if (key.getKeyType() == AttrType.attrReal) {
            Convert.setFloValue(((FloatKey)key).getKey(), 0, data);
        } else {
            Convert.setStrValue(((StringKey)key).getKey(), 0, data);
        }

        Convert.setIntValue(pageId.pid, key.getKeySize(), data);
        return data;
    }

    public KeyClass getKey() {
        return key;
    }

    public boolean equals(HashRecord record) {
        return ((ClusteredHashRecord) record).getKey().equals(key)  && ((ClusteredHashRecord)record).getPageId().pid == pageId.pid;
    }

    public Tuple getData() {
        return data;
    }

    public String toString() {
        return "" +key + "";
//        return "" +key + "" + ", <" + pageId.pid + ">";
//        return "< " + key + ", < " + rid.pageNo + ", " + rid.slotNo + " >>";
    }
}
