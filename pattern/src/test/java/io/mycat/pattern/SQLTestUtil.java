//package cn.lightfish.pattern;
//
//import lombok.AllArgsConstructor;
//import lombok.Getter;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//
//public class SQLTestUtil {
//    public static void main(String[] args) throws IOException {
//
//
//        Iterator<String> iterator =
//                Files.lines(Paths.get("D:\\git\\GPattern2\\src\\test\\resources\\test.txt")).iterator();
//
//        while (iterator.hasNext()){
//
//        }
//        new Iterator<Item>(){
//            private Item item;
//            StringBuilder sb = new StringBuilder();
//            String header = "";
//            @Override
//            public boolean hasNext() {
//                String line = iterator.next();
//                if (line.startsWith("-- ")){
//                    String[] split = header.split(";");
//                    Map<String,String> map =   new HashMap<>();
//                    for (String s : split) {
//                        String[] kv = s.split(":");
//                        map.put(kv[0], kv[1]);
//                    }
//                    this.item = new Item(map, sb.toString());
//                    this.header = line;
//                }else {
//                    sb.append(line);
//                }
//            }
//
//            @Override
//            public Item next() {
//                return null;
//            }
//        }
//    }
//
//    @AllArgsConstructor
//    @Getter
//    public static class Item{
//        Map<String,String> attributes;
//        String text = "";
//    }
//}