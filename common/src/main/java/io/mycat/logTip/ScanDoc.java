/**
 * Copyright (C) <2020>  <chen junwen>
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

package io.mycat.logTip;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScanDoc {

  public static void main(String[] args) throws IOException {
    Pattern pattern = Pattern.compile(
        "((warn|info|error|debug|errorPacket|MycatException)\\((\"[\\s\\S]+?\\\")+?)|((errorPacket|MycatException)\\([0-9],(\"[\\s\\S]+?\\\")+?)"
    );
    Files.walk(Paths.get(args[0])).filter(i -> i.getFileName().toString().endsWith(".java"))
        .forEach(i -> {
          try {
            if (Files.isDirectory(i)) {
              return;
            }
            Path path = i.toAbsolutePath();
            Iterator<String> iterator = Files.lines(path).iterator();
            while (iterator.hasNext()) {
              Matcher matcher = pattern.matcher(iterator.next());
              while (matcher.find()) {
                String group = matcher.group(0);
                System.out.println(group);
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

  }
}