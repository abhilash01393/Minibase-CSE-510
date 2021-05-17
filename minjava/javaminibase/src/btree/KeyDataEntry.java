/*
 * @(#) bt.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *        Author Xiaohu Li (xiaohu@cs.wisc.edu)
 */
package btree;

import global.*;
import heap.Tuple;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

import java.io.IOException;

/**
 * KeyDataEntry: define (key, data) pair.
 */
public class KeyDataEntry {
    /**
     * key in the (key, data)
     */
    public KeyClass key;
    /**
     * data in the (key, data)
     */
    public DataClass data;

    /**
     * Class constructor
     */
    public KeyDataEntry(Integer key, PageId pageNo) {
        this.key = new IntegerKey(key);
        this.data = new IndexData(pageNo);
    }

    ;

    // [SG]: add real type constructor
    public KeyDataEntry(Float key, PageId pageNo) {
        this.key = new FloatKey(key);
        this.data = new IndexData(pageNo);
    }

    ;


    /**
     * Class constructor.
     */
    public KeyDataEntry(KeyClass key, PageId pageNo) {

        data = new IndexData(pageNo);
        if (key instanceof IntegerKey)
            this.key = new IntegerKey(((IntegerKey) key).getKey());
        else if (key instanceof StringKey)
            this.key = new StringKey(((StringKey) key).getKey());
        else if (key instanceof FloatKey) {
            // [SG]: add real type support
            this.key = new FloatKey(((FloatKey) key).getKey());
        }
    }

    ;


    /**
     * Class constructor.
     */
    public KeyDataEntry(String key, PageId pageNo) {
        this.key = new StringKey(key);
        this.data = new IndexData(pageNo);
    }

    ;

    /**
     * Class constructor.
     */
    public KeyDataEntry(Integer key, RID rid) {
        this.key = new IntegerKey(key);
        this.data = new LeafData(rid);
    }

    ;

    // [SG]: add real type constructor
    public KeyDataEntry(Float key, RID rid) {
        this.key = new FloatKey(key);
        this.data = new LeafData(rid);
    }

    ;

    /**
     * Class constructor.
     */
    public KeyDataEntry(String key, RID rid) {
        this.key = new StringKey(key);
        this.data = new LeafData(rid);
    }

    ;

    /**
     * Class constructor.
     */
    public KeyDataEntry(Integer key, Tuple data) {
        this.key = new IntegerKey(key);
        this.data = new ClusteredLeafData(data);
    }

    ;

    // [SG]: add real type constructor
    public KeyDataEntry(Float key, Tuple data) {
        this.key = new FloatKey(key);
        this.data = new ClusteredLeafData(data);
    }

    ;

    /**
     * Class constructor.
     */
    public KeyDataEntry(String key, Tuple data) {
        this.key = new StringKey(key);
        this.data = new ClusteredLeafData(data);
    }

    ;


    /**
     * Class constructor.
     */
    public KeyDataEntry(KeyClass key, RID rid) {
        data = new LeafData(rid);
        if (key instanceof IntegerKey)
            this.key = new IntegerKey(((IntegerKey) key).getKey());
        else if (key instanceof StringKey)
            this.key = new StringKey(((StringKey) key).getKey());
        else if (key instanceof FloatKey) {
            // [SG]: add real type support
            this.key = new FloatKey(((FloatKey) key).getKey());
        }
    }

    ;

    /**
     * Class constructor.
     */
    public KeyDataEntry(KeyClass key, Tuple data) {
        this.data = new ClusteredLeafData(data);
        if (key instanceof IntegerKey)
            this.key = new IntegerKey(((IntegerKey) key).getKey());
        else if (key instanceof StringKey)
            this.key = new StringKey(((StringKey) key).getKey());
        else if (key instanceof FloatKey) {
            // [SG]: add real type support
            this.key = new FloatKey(((FloatKey) key).getKey());
        }
    }

    ;


    /**
     * Class constructor.
     */
    public KeyDataEntry(KeyClass key, DataClass data) {
        if (key instanceof IntegerKey)
            this.key = new IntegerKey(((IntegerKey) key).getKey());
        else if (key instanceof StringKey)
            this.key = new StringKey(((StringKey) key).getKey());
        else if (key instanceof FloatKey) {
            // [SG]: add real type support
            this.key = new FloatKey(((FloatKey) key).getKey());
        }
        if (data instanceof IndexData)
            this.data = new IndexData(((IndexData) data).getData());
        else if (data instanceof LeafData)
            this.data = new LeafData(((LeafData) data).getData());
        else if (data instanceof ClusteredLeafData)
            this.data = new ClusteredLeafData(((ClusteredLeafData) data).getData());
    }

    /**
     * shallow equal.
     *
     * @param entry the entry to check again key.
     * @return true, if entry == key; else, false.
     */
    public boolean equals(KeyDataEntry entry) {
        boolean st1, st2 = false;

        if (key instanceof IntegerKey)
            st1 = ((IntegerKey) key).getKey().equals
                    (((IntegerKey) entry.key).getKey());
        else if (key instanceof FloatKey) {
            // [SG]: add real type support
            st1 = ((FloatKey) key).getKey().equals
                    (((FloatKey) entry.key).getKey());
        } else
            st1 = ((StringKey) key).getKey().equals
                    (((StringKey) entry.key).getKey());

        if (data instanceof IndexData)
            st2 = ((IndexData) data).getData().pid ==
                    ((IndexData) entry.data).getData().pid;
        else if (data instanceof LeafData)
            st2 = ((RID) ((LeafData) data).getData()).equals
                    (((RID) ((LeafData) entry.data).getData()));
        else {
            Tuple t1 = ((Tuple) ((ClusteredLeafData) data).getData());
            Tuple t2 = ((Tuple) ((ClusteredLeafData) entry.data).getData());
            try {
                st2 = TupleUtils.Equal(t1, t2, t1.getTypes(), t1.noOfFlds());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (UnknowAttrType unknowAttrType) {
                unknowAttrType.printStackTrace();
            } catch (TupleUtilsException e) {
                e.printStackTrace();
            }
        }

        return (st1 && st2);
    }
}

