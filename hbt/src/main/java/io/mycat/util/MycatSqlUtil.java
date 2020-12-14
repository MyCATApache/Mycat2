package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.calcite.MycatCalciteMySqlNodeVisitor;
import io.mycat.calcite.MycatSqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MycatSqlUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(MycatSqlUtil.class);

//    public static String getCalciteSQL(SQLStatement sqlStatement) {
//        SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
//        MycatCalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new MycatCalciteMySqlNodeVisitor();
//        sqlStatement.accept(calciteMySqlNodeVisitor);
//        SqlNode sqlNode = calciteMySqlNodeVisitor.getSqlNode();
//        return sqlNode.toSqlString(MycatSqlDialect.DEFAULT).getSql();
//    }


//    public RowBaseIterator fetchResultSet(MycatRowMetaData mycatMetaData, String targetName, String sql) {
//        try {
//            JdbcConnectionManager connectionManager = JdbcRuntime.INSTANCE.getConnectionManager();
//            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
//            try (Connection connection = connectionManager.getDatasourceInfo().get(targetName).getDataSource().getConnection()) {
//                try (Statement statement = connection.createStatement()) {
//                    try (ResultSet resultSet = statement.executeQuery(sql)) {
//                        if (mycatMetaData == null) {
//                            mycatMetaData = new JdbcRowMetaData(resultSet.getMetaData());
//                        }
//                        int columnCount = mycatMetaData.getColumnCount();
//                        resultSetBuilder.
//                        while (resultSet.next()){
//                            resultSet.
//                        }
//                        for (int i = 0; i < columnCount; i++) {
//
//                        }
//                    }
//                }
//            }
//        } catch (Throwable e) {
//
//        }
//    }
}