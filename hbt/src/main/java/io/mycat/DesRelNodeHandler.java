/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat;

import io.mycat.describer.Describer;
import io.mycat.describer.NodeVisitorImpl;
import io.mycat.describer.ParseNode;
import io.mycat.rsqlBuilder.DesBuilder;
import io.mycat.rsqlBuilder.DotCallResolver;
import io.mycat.hbt.DesComplier;
import io.mycat.hbt.QueryOp;
import io.mycat.hbt.ast.base.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelRunners;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class DesRelNodeHandler {
    final FrameworkConfig config;
    final DesComplier complier = new DesComplier();

    public DesRelNodeHandler(FrameworkConfig config) {
        this.config = config;
    }

    public static <T extends ParseNode> T parse2SyntaxAst(String text) {
        Describer describer = new Describer(text);
        ParseNode primary = processDotCall(describer.expression());
        return (T) primary;
    }

    public static String toflatSyntaxAstText(String text) {
        return syntaxAstToFlatSyntaxAstText(parse2SyntaxAst(text));
    }

    public static String syntaxAstToFlatSyntaxAstText(ParseNode node) {
        NodeVisitorImpl nodeVisitor = new NodeVisitorImpl();
        node.accept(nodeVisitor);
        return nodeVisitor.getText();
    }

    public static void dump(RelNode rel, Writer writer) {
        try (PreparedStatement preparedStatement = RelRunners.run(rel)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            dump(resultSet, true, new PrintWriter(writer));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    private static ParseNode processDotCall(ParseNode primary) {
        DotCallResolver callResolver = new DotCallResolver();
        primary.accept(callResolver);
        primary = callResolver.getStack();
        return primary;
    }

    public Schema complieFlatSyntaxAstText(String text) {
        try {
            return complier.complie(text);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public RelNode toRelNode(Schema node) {
        return new QueryOp(DesBuilder.create(config)).complie(node);
    }

    public PreparedStatement handle(String text) {
        return RelRunners.run(toRelNode(complieFlatSyntaxAstText(syntaxAstToFlatSyntaxAstText(parse2SyntaxAst(text)))));
    }

    public String dump(String text) {
        return dump(toRelNode(complieFlatSyntaxAstText(syntaxAstToFlatSyntaxAstText(parse2SyntaxAst(text)))));
    }

    public static String dump(RelNode rel) {
        CharArrayWriter writer = new CharArrayWriter(8192);
        dump(rel, writer);
        return new String(writer.toCharArray()).replaceAll("\r", "");
    }
}