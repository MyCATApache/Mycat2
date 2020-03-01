package io.mycat.hbt;

import io.mycat.hbt.ast.base.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelRunners;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class TextConvertor {

    public static String dump(Schema schema){
        ExplainVisitor explainVisitor = new ExplainVisitor(true);
        schema.accept(explainVisitor);
        return explainVisitor.getString();
    }

    public static void dump(RelNode rel, Writer writer) {
        try (PreparedStatement preparedStatement = RelRunners.run(rel)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            dump(writer, resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String  dump(ResultSet resultSet) throws SQLException {
        CharArrayWriter writer = new CharArrayWriter(8192);
        dump(resultSet, true, new PrintWriter(writer));
        return writer.toString();
    }
    public static void dump(Writer writer, ResultSet resultSet) throws SQLException {
        dump(resultSet, true, new PrintWriter(writer));
    }

    public static void dump(ResultSet resultSet, boolean newline, PrintWriter writer)
            throws SQLException {
        final int columnCount = resultSet.getMetaData().getColumnCount();
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

    private static void dumpColumn(ResultSet resultSet, int i, PrintWriter writer)
            throws SQLException {
        final int t = resultSet.getMetaData().getColumnType(i);
        if (t == Types.REAL) {
            writer.print(resultSet.getString(i));
            writer.print("F");
            return;
        } else {
            writer.print(resultSet.getString(i));
        }
    }

    public static String dump(RelNode rel) {
        CharArrayWriter writer = new CharArrayWriter(8192);
        dump(rel, writer);
        return new String(writer.toCharArray()).replaceAll("\r", "");
    }
}