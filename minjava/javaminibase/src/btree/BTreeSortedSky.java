package btree;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.*;
import iterator.Iterator;
import iterator.TupleUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class BTreeSortedSky implements GlobalConst {
	
	private AttrType[] attrType;
	private int attr_len;
	private short[] t1_str_sizes;
	private Iterator am1;
	private String relationName;
	private int[] pref_list;
	private int pref_list_length;
	private IndexFile index_file;
	private int n_pages;

	boolean status = OK;
	private static List<Tuple> buffer_window;
	
	private Heapfile temp;
	
	// Constructor for BTreeSortedSky
	public BTreeSortedSky(AttrType[] attrType, int attr_len, short[] t1_str_sizes, Iterator am1, String relationName, int[] pref_list, int pref_list_length, IndexFile index_file,
			int n_pages) throws Exception {

		this.relationName = relationName;
		this.index_file = index_file;
		this.attrType = attrType;
		this.attr_len = attr_len;
		this.t1_str_sizes = t1_str_sizes;
		this.am1 = am1;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		this.n_pages = n_pages; 	
	}
	
	public String computeSkyline() throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {
		
		Heapfile hf = new Heapfile(relationName);
		String temp_heap_name = Heapfile.getRandomHFName();
		temp = new Heapfile(temp_heap_name);
		
		BTFileScan scan = ((BTreeFile) index_file).new_scan(null, null);
		KeyDataEntry entry;
		RID rid;
		
		Tuple t = getTuple();
			
		buffer_window = new ArrayList<>();
		
		int size = (MINIBASE_PAGESIZE / t.size()) * n_pages;
		
		//Getting the first tuple
	    entry = scan.get_next();
		
		// Total read tuples
		int total = 0;
		
		// Enter the tuples in temp heap and buffer window
		rid = ((LeafData) entry.data).getData();
		Tuple temp_tuple = hf.getRecord(rid);
		
		temp_tuple.setHdr((short) attr_len, attrType, t1_str_sizes); 
		buffer_window.add(temp_tuple);

		// temp_tuple.print(attrType);
		
		rid = temp.insertRecord(temp_tuple.returnTupleByteArray());
		entry = scan.get_next();

		// Iterate through the heap file and find dominating tuples (skyline candidates)
        while (entry != null) {
            boolean check = false;
            total++;
            Tuple heap_tuple = getTuple();
            
            rid = ((LeafData) entry.data).getData();
            heap_tuple.tupleCopy(hf.getRecord(rid));
		
			for(int i = 0; i < buffer_window.size(); i++){
				// Replace tuple from heap file with the tuple in window as it is dominated
				if (TupleUtils.Dominates(buffer_window.get(i) , attrType, heap_tuple, attrType, (short) attr_len, t1_str_sizes, pref_list, pref_list_length)) {
					check = true;
					break;
				} 
			}

			// Add the heap file tuple in temp heap file as it is not dominated by any tuple in the window
			if(!check){

				try {
					heap_tuple.setHdr((short) attr_len, attrType, t1_str_sizes); 
					rid = temp.insertRecord(heap_tuple.returnTupleByteArray());
					if(buffer_window.size() < size){
						buffer_window.add(heap_tuple);
					}
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			} 
			entry = scan.get_next();
        }
		
		scan.DestroyBTreeFileScan();

		System.out.println("Temp file records:" + temp.getRecCnt());
        System.out.println("Total data: " + total);
        System.out.println("Buffer Size: " + size);

		SystemDefs.JavabaseBM.flushPages();
//		QueryInterface.runSortFirstSky(temp_heap_name);
		return temp_heap_name;
//		hf.deleteFile();
	}
	
	private Tuple getTuple() throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}

}
