package io.mycat.sqlparser.util;

import static io.mycat.sqlparser.util.Tokenizer.DIGITS;
import static io.mycat.sqlparser.util.Tokenizer.STRINGS;

public class SQLMapAnnotation {

  int processorPos;
  short[] keyStartIndex = new short[8];
  short[] valueStartIndex = new short[8];
  byte index;
  HashArray array;
  ByteArrayView byteArrayView;

  public int getProcessorPos() {
    return processorPos;
  }

  public void setProcessorPos(int processorPos) {
    this.processorPos = processorPos;
  }

  public short[] getKeyStartIndex() {
    return keyStartIndex;
  }

  public void setKeyStartIndex(short[] keyStartIndex) {
    this.keyStartIndex = keyStartIndex;
  }

  public short[] getValueStartIndex() {
    return valueStartIndex;
  }

  public void setValueStartIndex(short[] valueStartIndex) {
    this.valueStartIndex = valueStartIndex;
  }

  public void addKeyValue(int keyStartIndex, int valueIndex) {
    this.keyStartIndex[index] = (short) keyStartIndex;
    this.valueStartIndex[index] = (short) valueIndex;
    this.index++;
  }

  public void clear() {
    this.index = 0;
    this.array = null;
    this.byteArrayView = null;
  }

  public void init(HashArray array,
      ByteArrayView byteArrayView) {
    clear();
    this.array = array;
    this.byteArrayView = byteArrayView;
  }

  public <T extends PutKeyValueAble> T toMapAndClear(T map) {
    try {
      map.put("annotationName", byteArrayView.getStringByHashArray(processorPos, array));
      for (int i = 0; i < keyStartIndex.length && i < index; i++) {
        String key = byteArrayView.getStringByHashArray(keyStartIndex[i], array);
        short value = valueStartIndex[i];
        switch (array.getType(value)) {
          case DIGITS:
            long hash = array.getHash(value);
            map.put(key, hash);
            break;
          case STRINGS:
//          String s = byteArrayView.getStringByHashArray(value, array);
//          map.put(key, s.substring(1,s.length()-1));
            map.put(key, byteArrayView.getStringByHashArray(value, array));
            break;
          default:
            map.put(key, byteArrayView.getStringByHashArray(value, array));
            break;
        }
      }
      return map;
    } finally {
      clear();
    }
  }

  public interface PutKeyValueAble {

    void put(String key, long value);

    void put(String key, String value);
  }
}