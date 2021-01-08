package io.mycat.hbt;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hbt.ast.base.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelRunners;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.*;
import java.util.ArrayList;

public class TextConvertor {

    public static String dumpExplain(Schema schema) {
        ExplainVisitor explainVisitor = new ExplainVisitor(true);
        schema.accept(explainVisitor);
        return explainVisitor.getString();
    }

    public static void dumpResultSet(RelNode rel, Writer writer) {
        try (PreparedStatement preparedStatement = RelRunners.run(rel)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            dumpResultSet(writer, resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String dumpResultSet(ResultSet resultSet) {
        CharArrayWriter writer = new CharArrayWriter(8192);
        dumpResultSet(resultSet, true, new PrintWriter(writer));
        return writer.toString().trim();
    }

    public static void dumpResultSet(Writer writer, ResultSet resultSet) {
        dumpResultSet(resultSet, true, new PrintWriter(writer));
    }

    public static String dumpResultSet(RowBaseIterator resultSet) {
        CharArrayWriter writer = new CharArrayWriter();
        dumpResultSet(resultSet, true, new PrintWriter(writer));
        return writer.toString();
    }


    public static void dumpResultSet(ResultSet resultSet, boolean newline, PrintWriter writer) {
        dumpResultSet(new JdbcRowBaseIterator(null, null, null, resultSet, null, null), newline, writer);
    }

    public static void dumpResultSet(RowBaseIterator resultSet, boolean newline, PrintWriter writer) {
        MycatRowMetaData metaData = resultSet.getMetaData();
        metaData.toSimpleText();
        final int columnCount = metaData.getColumnCount();
        int r = 0;
        while (resultSet.next()) {
            if (!newline && r++ > 0) {
                writer.print(",");
            }
            if (columnCount == 0) {
                if (newline) {
                    writer.println("()");
                } else {
                    writer.print("()");
                }
            } else {
                writer.print('(');
                dumpColumn(resultSet, 1, writer);
                for (int i = 2; i <= columnCount; i++) {
                    writer.print(',');
                    dumpColumn(resultSet, i, writer);
                }
                if (newline) {
                    writer.println(')');
                } else {
                    writer.print(")");
                }
            }
        }
    }

    private static void dumpColumn(RowBaseIterator resultSet, int i, PrintWriter writer) {
        final int t = resultSet.getMetaData().getColumnType(i);
        if (t == Types.REAL) {
            writer.print(resultSet.getString(i));
            writer.print("F");
            return;
        } else {
            writer.print(resultSet.getString(i));
        }
    }

    public static String dumpResultSet(RelNode rel) {
        CharArrayWriter writer = new CharArrayWriter(8192);
        dumpResultSet(rel, writer);
        return new String(writer.toCharArray()).replaceAll("\r", "");
    }

    public static String dumpMetadata(JdbcRowMetaData metaData) {
        int columnCount = metaData.getColumnCount();
        ArrayList<String> names = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            names.add(metaData.getColumnName(i));
        }
        return String.join(",",names).trim();
    }

    public static String dumpMetadata(ResultSetMetaData metaData) {
        return dumpMetadata(new JdbcRowMetaData(metaData));
    }
}