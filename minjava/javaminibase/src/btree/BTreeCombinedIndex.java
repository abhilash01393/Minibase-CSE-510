package btree;

import heap.*;
import global.*;

public class BTreeCombinedIndex{
    BTreeFile file;
    int id=0;
    public int prefix = 0;
    public static String random_string1, random_string2;
    
    public BTreeCombinedIndex(){
    }
    
    float floatKey(double[] values, int[] pref_list, int pref_list_length) {
        float sum = 0.0f;
        int j = 0;
		for(int i = 0; i < values.length; i++) {
			if(j < pref_list_length && pref_list[j] == (i+1)) {
                sum += (float) values[i];
                j++;
			}
		}
		return sum;
    }

    public IndexFile combinedIndex(String filePath, AttrType[] attrTypes, short[] t1_str_sizes, int[] pref_list, int pref_list_length)
            throws Exception {
        
        // Get data file in heapfile
        Heapfile hf = new Heapfile(filePath);
        int cols = attrTypes.length;

        double[][] records = new double[hf.getRecCnt()][cols];    
    
        RID tempRid = new RID();
        Scan scanTempHF = new Scan(hf);
        int j = 0; 

        // Put the tuples from heap file to array
        while (true) {
            Tuple tempHFTuple = scanTempHF.getNext(tempRid);
			if (tempHFTuple == null)
				break;
            tempHFTuple.setHdr((short) cols, attrTypes, t1_str_sizes); 
            for(int i = 0; i < cols; i++)
                records[j][i] = tempHFTuple.getFloFld(i+1);
            j++;
		}

		scanTempHF.closescan(); 

        int keyType = AttrType.attrReal;
        int keySize = 4;
        
        random_string1 = Heapfile.getRandomHFName();
        random_string2 = Heapfile.getRandomHFName();
        Heapfile heapfile = new Heapfile(random_string1);
        
        // Initialize Index File 
        file = new BTreeFile(random_string2, keyType, keySize, 1);

        Tuple t = new Tuple();

        t.setHdr((short) attrTypes.length,attrTypes, t1_str_sizes);
        int size = t.size();
        
        t = new Tuple(size);
        t.setHdr((short) attrTypes.length, attrTypes, t1_str_sizes);

        RID rid;
        
        float fkey;
        KeyClass ffkey;
        
        // Set key and rid and insert in index file
        for(double[] value :records){
            
            fkey = floatKey(value, pref_list, pref_list_length);
            ffkey = new FloatKey(-fkey);
            for(int i = 0; i<value.length; i++) {
                t.setFloFld(i+1, (float) value[i]);
            }
            
            rid = heapfile.insertRecord(t.returnTupleByteArray());       
            file.insert(ffkey, rid);
        }
        
        return file;
    }

}