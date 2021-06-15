package io.mycat.calcite;

import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

@Getter
public class TableParamSqlNode extends SqlIdentifier {
    private final String hint;

    public TableParamSqlNode(List<String> names, String hint) {
        super(names, SqlParserPos.ZERO);
        this.hint = hint;
    }
    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new TableParamSqlNode(names,hint);
    }
}
