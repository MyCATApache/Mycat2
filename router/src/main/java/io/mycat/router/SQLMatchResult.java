/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router;

import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-02 03:28
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
