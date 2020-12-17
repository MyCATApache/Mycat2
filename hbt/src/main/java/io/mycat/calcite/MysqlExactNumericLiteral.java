package io.mycat.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;

public class MysqlExactNumericLiteral extends SqlNumericLiteral {
    protected MysqlExactNumericLiteral(BigDecimal value,SqlParserPos pos) {
        super(value, value.precision(), value.scale(), true, pos);
    }

    public static MysqlExactNumericLiteral create(BigDecimal value,SqlParserPos pos){
        return new MysqlExactNumericLiteral(value, pos);
    }

    @Override
    public SqlTypeName getTypeName() {
        return SqlTypeName.DECIMAL;
    }

    @Override
    public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);
    }
}