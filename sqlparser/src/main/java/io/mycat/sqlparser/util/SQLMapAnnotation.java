package io.mycat.sqlparser.util;

import static io.mycat.sqlparser.util.Tokenizer.DIGITS;

import java.util.Map;

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
    this.array = array;
    this.byteArrayView = byteArrayView;
  }

  public Map<String, Object> toMapAndClear(Map<String, Object> map) {
    map.put("annotationName", byteArrayView.getStringByHashArray(processorPos, array));
    for (int i = 0; i < keyStartIndex.length && i < index; i++) {
      String key = byteArrayView.getStringByHashArray(keyStartIndex[i], array);
      short value = valueStartIndex[i];
      switch (array.getType(value)) {
        case DIGITS:
          long hash = array.getHash(value);
          map.put(key, hash);
          break;
        default:
          map.put(key, byteArrayView.getStringByHashArray(value, array));
          break;
      }
    }
    return map;
  }
}