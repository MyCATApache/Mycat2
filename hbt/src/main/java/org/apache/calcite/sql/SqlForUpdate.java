package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import javax.annotation.Nonnull;
import java.util.List;

public class SqlForUpdate extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("for update", SqlKind.IDENTIFIER) {
                @Override public SqlCall createCall(SqlLiteral functionQualifier,
                                                    SqlParserPos pos, SqlNode... operands) {
                    return new SqlForUpdate(pos, operands[0]);
                }
            };
    private final SqlNode operand;

    public SqlForUpdate(SqlParserPos pos, SqlNode operand) {
        super(pos);
        this.operand = operand;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(operand);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        operand.unparse(writer,leftPrec,rightPrec);
        writer.keyword(" for update ");
    }
}