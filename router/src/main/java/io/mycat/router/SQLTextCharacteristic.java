/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-02 03:34
 **/
public class SQLTextCharacteristic {

  List<String> characteristicPatterns;
  Map<Integer, String> idCharacteristicPatternsMap = new HashMap<>();

  public void putIdOfCharacteristicPattern(int id, String characteristicPattern) {
    idCharacteristicPatternsMap.put(id, characteristicPattern);
  }

  public String getCharacteristicPatternById(int id) {
    return idCharacteristicPatternsMap.get(id);
  }

  public SQLTextCharacteristic(
      List<String> characteristicPatterns) {
    this.characteristicPatterns = characteristicPatterns;
  }

  List<String> patterns() {
    ArrayList<String> strings = new ArrayList<>();
    strings.addAll(characteristicPatterns);
    return strings;
  }


  public List<String> getCharacteristicPatterns() {
    return characteristicPatterns;
  }
}
