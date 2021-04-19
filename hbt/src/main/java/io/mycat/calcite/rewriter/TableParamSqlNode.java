//package io.mycat.calcite.rewriter;
//
//import org.apache.calcite.sql.*;
//import org.apache.calcite.sql.parser.SqlParserPos;
//
//public class TableParamSqlNode extends SqlBasicCall {
//    public static final SqlSpecialOperator OPERATOR =
//            new SqlSpecialOperator("?", SqlKind.IDENTIFIER) {
//                @Override public SqlCall createCall(SqlLiteral functionQualifier,
//                                                    SqlParserPos pos, SqlNode... operands) {
//                    return new TableParamSqlNode((SqlLiteral)operands[0]);
//                }
//            };
//    public TableParamSqlNode(SqlLiteral sqlLiteral) {
//        super(OPERATOR, new SqlNode[]{sqlLiteral}, SqlParserPos.ZERO);
//    }
//}
