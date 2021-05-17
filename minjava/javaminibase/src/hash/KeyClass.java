package hash;

public interface KeyClass {
    int getHash();
    int getKeySize();
    void setKeySize(int keySize);
    int getKeyType();
    void setKeyType(int keyType);
    boolean equals(KeyClass key);
}
