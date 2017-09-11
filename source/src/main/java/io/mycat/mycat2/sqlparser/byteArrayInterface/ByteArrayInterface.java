package io.mycat.mycat2.sqlparser.byteArrayInterface;

/**
 * Created by jamie on 2017/8/29.
 */
public interface ByteArrayInterface {
    int offset = 0;
    byte get(int index);

    int length();

    void set(int index, byte value);

    void setOffset(int offset);
    int getOffset();

    default String getString(int pos, int size) {
        byte[] bytes = new byte[size];
        for (int i = pos, j = 0; j < size; i++, j++) {
            bytes[j] = get(i);
        }
        String res=new String(bytes);
        return res;
    }
}
