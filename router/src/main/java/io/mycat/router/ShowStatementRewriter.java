package io.mycat.router;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowVariantsStatement;
import com.alibaba.druid.sql.builder.impl.SQLSelectBuilderImpl;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
//import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import io.mycat.MycatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

/**
 * chenjunwen
 * 完成show语句到普通sql的转换
 */
public class ShowStatementRewriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowStatementRewriter.class);

    public static String rewriteShowTables(String defaultSchema, SQLShowTablesStatement ast) {
        SQLExpr where = ast.getWhere();
        SQLName from = ast.getFrom();
        SQLExpr like = ast.getLike();
        boolean full = ast.isFull();
        String schema = SQLUtils.normalize(from == null ? defaultSchema : from.getSimpleName());
        if (schema == null) {
            throw new MycatException(1046, "No database selected");
        }
        String schemaCondition = " TABLE_SCHEMA = '" + schema + "' ";
        String whereCondition = " " + (where == null ? "true" : where.toString()) + " ";
        String likeCondition = like == null ? " true " : " TABLE_NAME like " + " " + like.toString() + " ";
        String fullCondition = !full ? " true " : " TABLE_TYPE  = 'BASE TABLE' ";

        String sql = MessageFormat.format("select TABLE_NAME as {0} from information_schema.`TABLES` where {1} ",
                "`" + "Tables_in_" + schema + "`", String.join(" and ", schemaCondition, whereCondition, likeCondition, fullCondition)
        );
        LOGGER.info(ast + "->" + sql);
        return sql;
    }


    public static String showTableStatus(MySqlShowTableStatusStatement ast, String databaseName, String tableName) {
        if (databaseName == null) {
            throw new MycatException(1046, "No database selected");
        }
        String like = ast.getLike() == null ? new SQLBooleanExpr(true).toString() : " TABLE_NAME like "+ast.getLike();
        SQLExpr where = ast.getWhere() == null ? new SQLBooleanExpr(true) : ast.getWhere();

        String schemaCondition = " TABLE_SCHEMA = '" + databaseName + "' ";
        String tableCondition = tableName != null ? " TABLE_NAME = '" + tableName + "' " : " true ";

        SQLSelectBuilderImpl sqlSelectBuilder = new SQLSelectBuilderImpl(DbType.mysql);
        String sql = sqlSelectBuilder
                .selectWithAlias("TABLE_NAME", "Name")
                .selectWithAlias("ENGINE", "Engine")
                .selectWithAlias("VERSION", "Version")
                .selectWithAlias("ROW_FORMAT", "Row_format")
                .selectWithAlias("AVG_ROW_LENGTH", "Avg_row_length")
                .selectWithAlias("DATA_LENGTH", "Data_length")
                .selectWithAlias("MAX_DATA_LENGTH", "Data_length")
                .selectWithAlias("INDEX_LENGTH", "Max_data_length")
                .selectWithAlias("DATA_FREE", "Data_free")
                .selectWithAlias("AUTO_INCREMENT", "Auto_increment")
                .selectWithAlias("CREATE_TIME", "UPDATE_TIME")
                .selectWithAlias("UPDATE_TIME", "Update_time")
                .selectWithAlias("CHECK_TIME", "Check_time")
                .selectWithAlias("TABLE_COLLATION", "Collation")
                .selectWithAlias("CHECKSUM", "Checksum")
                .selectWithAlias("CREATE_OPTIONS", "Create_options")
                .selectWithAlias("TABLE_COMMENT", "Comment")
                .from("information_schema.`TABLES`")
                .whereAnd(schemaCondition)
                .whereAnd(tableCondition)
                .whereAnd(like.toString())
                .whereAnd(where.toString())
                .toString();
        LOGGER.info(ast + "->" + sql);
        return sql;
    }

    public static String rewriteVariables(SQLShowVariantsStatement ast, String tableName) {
        if (tableName == null) {
            tableName = !ast.isGlobal() ? "SESSION_VARIABLES" : "GLOBAL_VARIABLES";
        }
        SQLSelectBuilderImpl sqlSelectBuilder = new SQLSelectBuilderImpl(DbType.mysql);
        sqlSelectBuilder.selectWithAlias("VARIABLE_NAME", "Variable_name");
        sqlSelectBuilder.selectWithAlias("VARIABLE_VALUE", "Value");
        if (ast.getLike() != null) {
            sqlSelectBuilder.whereAnd(" VARIABLE_NAME like "+ast.getLike().toString());
        }
        if (ast.getWhere() != null) {
            sqlSelectBuilder.whereAnd(ast.getWhere().toString());
        }
        return sqlSelectBuilder.from("information_schema." + tableName)
                .toString();
    }

}
