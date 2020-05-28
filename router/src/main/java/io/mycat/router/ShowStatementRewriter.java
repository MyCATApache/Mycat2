package io.mycat.router;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.statement.SQLShowTablesStatement;
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
        String likeCondition = like == null ? "true" : " TABLE_NAME like " + " " + like.toString() + " ";
        String fullCondition = !full ? " true " : " TABLE_TYPE  = 'BASE TABLE' ";

        return MessageFormat.format("select TABLE_NAME as {0} from information_schema.`TABLES` where {1} ",
                "Tables_in_" + schema, String.join(" and ", schemaCondition, whereCondition, likeCondition, fullCondition)
        );
    }
}