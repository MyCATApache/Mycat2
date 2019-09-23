/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package cn.lightfish.pattern;

import java.util.HashMap;
import java.util.Map;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class GPositionRecorder {
    final HashMap<String, GPatternPosition> map = new HashMap<>();
    GPatternPosition currentPosition;
    String name;

    public GPositionRecorder(Map<String, GPatternPosition> variables) {
        for (Map.Entry<String, GPatternPosition> stringGPatternPositionEntry : variables.entrySet()) {
            map.put(stringGPatternPositionEntry.getKey(), this.currentPosition = new GPatternPosition());
            this.currentPosition.start = Integer.MAX_VALUE;
            this.currentPosition.end = Integer.MIN_VALUE;
        }
    }

    public void startRecordName(String name, int startOffset) {
        if (this.name == null || !this.name.equals(name)) {
            this.currentPosition = map.get(name);
            if (this.currentPosition==null){
                return;
            }
            this.name = name;
            this.currentPosition.start = Integer.MAX_VALUE;
            this.currentPosition.end = Integer.MIN_VALUE;
        }
        currentPosition.start = Math.min(currentPosition.start, startOffset);
    }

    public void record(int endOffset) {
        if (currentPosition!=null) {
            currentPosition.end = Math.max(currentPosition.end, endOffset);
        }
    }

}
