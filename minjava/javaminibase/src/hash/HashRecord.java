package hash;

import java.io.IOException;

public interface HashRecord {
    byte[] getBytesFromRecord() throws IOException;
    KeyClass getKey();
    boolean equals(HashRecord record);
}