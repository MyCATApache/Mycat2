package io.mycat.mycat2.sqlparser.SQLParseUtils;

/**
 * Created by Fanfan on 2017/3/21.
 */
public class HashArray {
    long[] hashArray;
    int pos = 0;
    
    public HashArray(){
    	hashArray = new long[4096];
    }
    
    public HashArray(int size){
    	hashArray = new long[size];
    }

    public void init() {
        while(pos>=0) {
            hashArray[pos--] = 0;
        }
        pos = 0;
    }
    public void set(int type, int start, int size) {
        set(type, start, size, 0L); }
    public void set(int type, int start, int size, long hash) {
        if (size >= 0xFF) {
            size = 0xFF;
        }
        hashArray[pos++] = (long)type << 32 | size << 24 | start&0xFFFFFF; hashArray[pos++] = hash; }
    public int getPos(int idx) { return (((int)hashArray[idx<<1]) & 0xFFFF); }
    public int getSize(int idx) {
        int size = (((int)hashArray[idx<<1]&0xFF000000) >>> 24);
        if (size >= 0xFF) {
            size = getPos(idx+1) - getPos(idx);
        }
        return size; }
    public int getType(int idx) { return (int)((hashArray[idx<<1]&0xFFFFFFFF00000000L) >>> 32); }
    public void setType(int idx, int type) { hashArray[idx<<1] = (hashArray[idx<<1] & 0xFFFFFFFFL) | ((long)type << 32); }
    public long getHash(int idx) { return hashArray[(idx<<1)+1]; }
    public int getIntHash(int idx) {
        long value = hashArray[idx << 1];
        return (int)(value >>> 24);
    }
    public int getCount() {return pos>>1;}
}
