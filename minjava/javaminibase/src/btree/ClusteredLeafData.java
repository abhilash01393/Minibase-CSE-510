package btree;

import global.RID;
import heap.Tuple;

public class ClusteredLeafData extends DataClass {
    private Tuple data;

    ClusteredLeafData(Tuple data) {
        this.data = data;
    }

    public Tuple getData() {
        return data;
    }

    public void setData(Tuple data) {
        this.data = data;
    }
}   
