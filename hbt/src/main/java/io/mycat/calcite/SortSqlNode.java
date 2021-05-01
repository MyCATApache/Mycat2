package io.mycat.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class SortSqlNode extends SqlDynamicParam {


    public SortSqlNode(int index, SqlParserPos pos) {
        super(index, pos);
    }
}
