package btree;
// [SG]: add attrReal support
/** Class RealKey contains the definition of support
 * for Float type keys in minibasedb
 */
public class FloatKey extends KeyClass {

    private Float key;

    public String toString(){
        return key.toString();
    }

    /** Class constructor
     *  @param     value   the value of the real key to be set
     */
    public FloatKey(Float value)
    {
        key=new Float(value.floatValue());
    }

    /** Class constructor
     *  @param     value   the value of the primitive type real key to be set
     */
    public FloatKey(float value)
    {
        key=new Float(value);
    }

    public Float getKey() {
        return new Float(key.floatValue());
    }

    public void setKey(Float key) {
        key = new Float(key.floatValue());
    }
}
