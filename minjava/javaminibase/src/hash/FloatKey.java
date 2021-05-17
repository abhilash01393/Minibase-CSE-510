package hash;

public class FloatKey implements KeyClass {

    private Float key;
    private int keySize;
    private int keyType;

    public String toString(){
        return key.toString();
    }

    public FloatKey(Float value)
    {
        key= value;
    }

    public FloatKey(float value)
    {
        key= value;
    }

    public int getHash() {
        return key.intValue();
    }

    public float getKey() {
        return key;
    }

    public void setKey(Float key) {
        key = key;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public int getKeyType() {
        return keyType;
    }

    public void setKeyType(int keyType) {
        this.keyType = keyType;
    }

    public boolean equals(KeyClass key) {
        return this.key.equals(((FloatKey) key).getKey());
    }
}
