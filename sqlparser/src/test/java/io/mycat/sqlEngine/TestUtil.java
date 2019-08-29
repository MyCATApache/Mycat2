package io.mycat.sqlEngine;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestUtil {
    public static void main(String[] args) throws IOException {
        List<String> lines = Files.lines(Paths.get("D:\\newgit2\\mycat3\\sqlparser\\src\\test\\resources\\test.txt")).collect(Collectors.toList());
        ListIterator<String> iterator = lines.listIterator();
        List<Item> res = new ArrayList<>();
        while (iterator.hasNext()) {
            String start = iterator.next().trim();
            String sql = null;
            if (!"-".startsWith(start)){
                sql = start;
            }else {
                sql = iterator.next().trim();
            }
            String resultHeader = iterator.next().trim();
            if (resultHeader.startsWith("ok")&& "-".equals(iterator.next().trim())) {
                res.add(new Item(sql,null,null,true));
                continue;
            } else if (resultHeader.startsWith("|")) {
                resultHeader = resultHeader.substring(1, resultHeader.length() - 1);
                String[] columnList = resultHeader.split("\\|");
                List<String[]> rows = new ArrayList<>();
                while (iterator.hasNext()){
                    String rowsText = iterator.next();
                    if ("-".equals(rowsText)) {
                        break;
                    }
                    try {
                        String[] row = rowsText.substring(1, rowsText.length() - 1).split("\\|");
                        rows.add(row);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                res.add(new Item(sql,columnList,rows,false));
            }
        }
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    static class Item {
        String sql;
        String[] columnList;
        List<String[]> rows;
        boolean ok = false;
    }

}