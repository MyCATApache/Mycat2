package io.mycat.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-02 03:28
 **/
public class SQLMatchResult {

  CharSequence sql;
  SQLTextCharacteristic textCharacteristic;
  Map<String,String> characteristicPositionList;

  public SQLTextCharacteristic getTextCharacteristic() {
    return textCharacteristic;
  }

  public void setTextCharacteristic(SQLTextCharacteristic textCharacteristic) {
    this.textCharacteristic=(textCharacteristic);
  }

  public Map<String, String> getCharacteristicPositionList() {
    return characteristicPositionList;
  }

  public void setCharacteristicPositionList(
      Map<String, String> characteristicPositionList) {
    this.characteristicPositionList = characteristicPositionList;
  }

  public SQLMatchResult(CharSequence sql) {
    this.sql = sql;
  }


  public static class Position {

    int startPos;
    int endPos;

    public int getStartPos() {
      return startPos;
    }

    public void setStartPos(int startPos) {
      this.startPos = startPos;
    }

    public int getEndPos() {
      return endPos;
    }

    public void setEndPos(int endPos) {
      this.endPos = endPos;
    }
  }

}
