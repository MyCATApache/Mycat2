package io.mycat.calcite;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class TextSqlNode extends SqlLiteral {

    public TextSqlNode(Object value) {
        super(value, SqlTypeName.VARCHAR, SqlParserPos.ZERO);
    }
}
