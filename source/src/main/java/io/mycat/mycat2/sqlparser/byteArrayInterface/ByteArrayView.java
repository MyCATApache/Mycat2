package io.mycat.mycat2.sqlparser.byteArrayInterface;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;

/**
 * Created by jamie on 2017/8/29.
 */
public interface ByteArrayView {

    byte get(int index);

    int length();

    default void set(int index, byte value){

    }

    default void setOffset(int offset){

    }
    default  int getOffset(){
        return 0;
    }

    default String getString(int pos, int size) {
        byte[] bytes = new byte[size];
        for (int i = pos, j = 0; j < size; i++, j++) {
            bytes[j] = get(i);
        }
        String res=new String(bytes);
        return res;
    }
    default String getStringByHashArray(int pos, HashArray hashArray) {
        String res = this.getString(hashArray.getPos(pos), hashArray.getSize(pos));
        return res;
    }
}
