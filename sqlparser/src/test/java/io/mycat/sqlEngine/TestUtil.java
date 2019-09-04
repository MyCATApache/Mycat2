package io.mycat.sqlEngine;

import cn.lightfish.sqlEngine.executor.logicExecutor.Executor;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;
import cn.lightfish.sqlEngine.schema.DbConsole;
import cn.lightfish.sqlEngine.schema.DbSchemaManager;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TestUtil {
    public static void main(String[] args) throws IOException {
        TestUtil.class.getResourceAsStream("baseSQL.sql");
        List<String> lines = Files.lines(Paths.get("D:\\newgit2\\mycat3\\sqlparser\\src\\test\\resources\\baseSQL.sql")).collect(Collectors.toList());
        ListIterator<String> iterator = lines.listIterator();
        DbConsole console = DbSchemaManager.INSTANCE.createConsole();
        SQLTestExecutor testExecutor = new SQLTestExecutor() {
            String[] columnList;
            ArrayList<String[]> rows = new ArrayList<>();

            @Override
            public void input(String sql) {
                Iterator<Executor> input = console.input(sql);
                Executor next = input.next();
                BaseColumnDefinition[] columnDefs = next.columnDefList();
                if (columnDefs != null && columnDefs.length != 0) {
                    columnList = Arrays.stream(columnDefs).map(i -> i.getColumnName()).toArray(i -> new String[i]);
                }
                next.apply();
                while (next.hasNext()) {
                    rows.add(Arrays.stream(next.next()).map(i -> Objects.toString(i)).toArray(i -> new String[i]));
                }
            }

            @Override
            public String[] columnList() {
                return columnList;
            }

            @Override
            public List<String[]> rows() {
                return rows;
            }

            @Override
            public boolean isOk() {
                return columnList == null;
            }
        };
        test(iterator, testExecutor);
    }

    private static void test(ListIterator<String> iterator, SQLTestExecutor testExecutor) {
        List<String> errorMeeage = new ArrayList<>();
        while (iterator.hasNext()) {
            String start = iterator.next().trim();
            String sql = null;
            if (!"-".startsWith(start)) {
                sql = start;
            } else {
                sql = iterator.next().trim();
            }
            String resultHeader = iterator.next().trim();
            if (resultHeader.startsWith("ok") && "-".equals(iterator.next().trim())) {
                testExecutor.input(sql);
                if (testExecutor.isOk() && testExecutor.columnList() != null && testExecutor.rows() != null) {
                    continue;
                } else {
                    errorMeeage.add(sql);
                    continue;
                }
            } else if (resultHeader.startsWith("|")) {
                resultHeader = resultHeader.substring(1, resultHeader.length() - 1);
                String[] cols = resultHeader.split("\\|");
                String[] columnList = Arrays.stream(cols).map(i -> i == null ? i : i.trim()).toArray(i -> new String[cols.length]);
                List<String[]> rows = new ArrayList<>();
                while (iterator.hasNext()) {
                    String rowsText = iterator.next();
                    if ("-".equals(rowsText)) {
                        break;
                    }
                    String[] row = rowsText.substring(1, rowsText.length() - 1).split("\\|");
                    rows.add(Arrays.stream(row).map(i -> i == null ? i : i.trim()).toArray(i -> new String[row.length]));
                }
                testExecutor.input(sql);
                String[] strings = testExecutor.columnList();
                boolean isColumnEquals = Arrays.deepEquals(columnList, Arrays.stream(strings).map(i -> i == null ? i : i.trim()).toArray(i -> new String[strings.length]));
                if ((!testExecutor.isOk()) && isColumnEquals && (listEqual(testExecutor.rows(), rows))) {
                    continue;
                } else {
                    errorMeeage.add(sql);
                    continue;
                }
            }
        }
    }

    public static boolean listEqual(List<String[]> left, List<String[]> right) {
        if (left == right) {
            return true;
        }
        if (left == null && right != null) {
            return false;
        }
        if (left != null && right == null) {
            return false;
        }
        if (left.size() != right.size()) {
            return false;
        }
        int size = left.size();
        for (int i = 0; i < size; i++) {
            String[] strings = left.get(i);
            String[] strings1 = right.get(i);
            if (!Arrays.deepEquals(strings, strings1)) {
                return false;
            }
        }
        return true;
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    static class Item {
        String sql;
        String[] columnList;
        List<String[]> rows;
        boolean ok = false;
    }

    interface SQLTestExecutor {
        void input(String sql);

        String[] columnList();

        List<String[]> rows();

        boolean isOk();
    }

}