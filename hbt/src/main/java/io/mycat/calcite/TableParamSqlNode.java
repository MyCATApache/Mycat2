package io.mycat.calcite;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.List;

public class TableParamSqlNode extends SqlIdentifier {

    public TableParamSqlNode(List<String> names) {
        super(names, SqlParserPos.ZERO);
    }
}
