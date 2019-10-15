package io.mycat.lib.impl;

import io.mycat.MycatException;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.calcite.CalciteEnvironment;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.function.Supplier;

public enum CalciteLib {
    INSTANCE;

    public Response responseQueryCalcite(String sql) {
        return JdbcLib.response(queryCalcite(sql));
    }

    public Supplier<MycatResultSetResponse[]> queryCalcite(String sql) {
        return () -> {
            try {
                CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection();
                Statement statement = null;
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
                return new MycatResultSetResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)};
            } catch (Exception e) {
                throw new MycatException(e);
            }
        };
    }

    public Supplier<MycatResultSetResponse[]> queryCalcite(RelNode rootRel) {
        return () -> {
                CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection();
                try {
                    PreparedStatement  statement = connection.unwrap(RelRunner.class).prepare(rootRel);
                    ResultSet resultSet = statement.executeQuery();
                    JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
                    return new MycatResultSetResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)};
                } catch (SQLException e) {
                    throw new MycatException(e);
                }
        };
    }

    public Response responseTest() {
        return JdbcLib.response(test());
    }

    public Supplier<MycatResultSetResponse[]> test() {
        CalciteConnection connection = CalciteEnvironment.INSTANCE.getRawConnection();

        SchemaPlus rootSchema1 = connection.getRootSchema();
        CalciteEnvironment.INSTANCE.setSchemaMap(rootSchema1);
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setCaseSensitive(false).setLex(Lex.MYSQL).build())
                .defaultSchema(rootSchema1)
                .build();
        RelBuilder relBuilder = RelBuilder.create(config);
        RelNode table = relBuilder
                .scan("testdb","travelrecord")
                .scan("testdb","address")
                .join(JoinRelType.INNER, "id")
                .project(relBuilder.field("id"), relBuilder.field("user_id"))
                .build();
        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES,false);
         table.explain(relWriter);
        System.out.println(relWriter.toString());
        table.explain(new RelXmlWriter(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES));

        RelJsonWriter jsonWriter;
        table.explain(jsonWriter =new RelJsonWriter());
        System.out.println(jsonWriter.asString());
        Supplier<MycatResultSetResponse[]> supplier = queryCalcite(table);
        return supplier;
    }
}