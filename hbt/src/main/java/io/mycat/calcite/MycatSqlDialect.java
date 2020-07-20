package io.mycat.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

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

}