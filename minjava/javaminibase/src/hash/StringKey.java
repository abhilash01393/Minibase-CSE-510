package hash;

public class StringKey implements KeyClass {

  private String key;
  private int keySize;
  private int keyType;

  public String toString(){
     return key;
  }

  public StringKey(String s) { key = new String(s); }

  public int getHash() {return Math.abs(key.hashCode());}

  public String getKey() {
    return key;
  }

  public void setKey(String s) { key=new String(s);}

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
    return this.key.equals(((StringKey) key).getKey());
  }
}
