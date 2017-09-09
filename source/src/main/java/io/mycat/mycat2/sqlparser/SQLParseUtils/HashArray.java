package io.mycat.mycat2.sqlparser.SQLParseUtils;

/**
 * Created by Fanfan on 2017/3/21.
 */
public class HashArray {
    long[] hashArray = new long[4096];
    int pos = 0;

    public void init() {
        while(pos>=0) {
            hashArray[pos--] = 0;
        }
        pos = 0;
    }
    public void set(int type, int start, int size) {
        hashArray[pos++] = (long)type << 32 | size << 16 | start; pos++; }
    public void set(int type, int start, int size, long hash) { hashArray[pos++] = (long)type << 32 | size << 16 | start; hashArray[pos++] = hash; }
    public int getPos(int idx) { return (((int)hashArray[idx<<1]) & 0xFFFF); }
    public int getSize(int idx) { return (((int)hashArray[idx<<1]&0xFFFF0000) >>> 16); }
    public int getType(int idx) { return (int)((hashArray[idx<<1]&0xFFFFFFFF00000000L)>>>32); }
    public void setType(int idx, int type) { hashArray[idx<<1] = (hashArray[idx<<1] & 0xFFFFFFFFL) | ((long)type << 32); }
    public long getHash(int idx) { return hashArray[(idx<<1)+1]; }
    public int getIntHash(int idx) {
        long value = hashArray[idx << 1];
        return (int)(value >>> 16);
    }
    public int getCount() {return pos>>1;}
}
