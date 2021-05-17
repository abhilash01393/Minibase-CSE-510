package hash;

public class IntegerKey implements KeyClass {

  private Integer key;
  private int keySize;
  private int keyType;

  public String toString(){
     return key.toString();
  }

  public IntegerKey(Integer value) {
    key= value;
  }

  public IntegerKey(int value) {
    key= value;
  }

  public int getHash()
  {
    return key;
  }

  public int getKey() {
    return key;
  }

  public void setKey(Integer value) 
  { 
    key= value;
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
    return this.key == ((IntegerKey) key).getKey();
  }
}
