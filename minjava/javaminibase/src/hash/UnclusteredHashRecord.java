package hash;

import global.AttrType;
import global.Convert;
import global.PageId;
import global.RID;

import java.io.IOException;

public class UnclusteredHashRecord implements HashRecord {
    public static final int RECORD_SIZE_WITHOUT_KEY = 8;

    private KeyClass key;
    private RID rid;

    public UnclusteredHashRecord(KeyClass key, RID rid) {
        this.key = key;
        this.rid = rid;
    }

    public UnclusteredHashRecord(byte[] data, int keyType, int keySize) throws IOException {
        if (keyType == AttrType.attrInteger) {
            int val = Convert.getIntValue(0, data);
            this.key = new IntegerKey(val);
        } else if (keyType == AttrType.attrReal) {
            float val = Convert.getFloValue(0, data);
            this.key = new FloatKey(val);
        } else {
            String val = Convert.getStrValue(0, data, keySize);
            this.key = new StringKey(val);
        }
        this.key.setKeyType(keyType);
        this.key.setKeySize(keySize);

        int pageId = Convert.getIntValue(keySize, data);
        int slotNo = Convert.getIntValue(keySize+4, data);
        this.rid = new RID(new PageId(pageId), slotNo);
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

        Convert.setIntValue(rid.pageNo.pid, key.getKeySize(), data);
        Convert.setIntValue(rid.slotNo, key.getKeySize() + 4, data);
        return data;
    }

    public boolean equals(HashRecord record) {
        return ((UnclusteredHashRecord) record).getKey().equals(key) && ((UnclusteredHashRecord) record).getRid().equals(rid);
    }

    public KeyClass getKey() {
        return key;
    }

    public void setKey(KeyClass key) {
        this.key = key;
    }

    public RID getRid() {
        return rid;
    }

    public void setRid(RID rid) {
        this.rid = rid;
    }

    public String toString() {
        return "" +key + "";
//        return "< " + key + ", < " + rid.pageNo + ", " + rid.slotNo + " >>";
    }
}
