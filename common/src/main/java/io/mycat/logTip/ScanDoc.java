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
        "((warn|info|error|debug|errorPacket)\\((\"[\\s\\S]+?\\\")+?)|((errorPacket|MycatException)\\([0-9],(\"[\\s\\S]+?\\\")+?)"
    );
    Files.walk(Paths.get("D:\\newgit\\f")).filter(i -> i.getFileName().toString().endsWith(".java"))
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