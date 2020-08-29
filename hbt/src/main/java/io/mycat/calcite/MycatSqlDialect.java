package io.mycat.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class MycatSqlDialect extends MysqlSqlDialect {
    /**
     * Creates a MysqlSqlDialect.
     *
     * @param context
     */
    public MycatSqlDialect(Context context) {
        super(context);
    }
    public static final SqlDialect DEFAULT = new MycatSqlDialect(DEFAULT_CONTEXT);

    @Override
    public SqlNode getCastSpec(RelDataType type) {
        if (type.getSqlTypeName() == SqlTypeName.BOOLEAN){
            return new SqlDataTypeSpec(
                    new SqlAlienSystemTypeNameSpec(
                            "SIGNED",
                            SqlTypeName.INTEGER,
                            SqlParserPos.ZERO),
                    SqlParserPos.ZERO);
        }
        return super.getCastSpec(type);
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, String charsetName, String val) {
        buf.append(literalQuoteString);
        buf.append(val);
        buf.append(literalEndQuoteString);
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlOperator operator = call.getOperator();
        if (operator instanceof SqlFunction){// should not with `` in fun name
            List<SqlNode> operandList = call.getOperandList();
            writer.print(operator.getName());
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode sqlNode : operandList) {
                writer.sep(",");
                sqlNode.unparse(writer, 0, 0);
            }
            writer.endFunCall(frame);
        }else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
}