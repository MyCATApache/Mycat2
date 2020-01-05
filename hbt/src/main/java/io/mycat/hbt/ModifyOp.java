package io.mycat.hbt;

import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.modify.*;

import java.util.Arrays;
import java.util.List;

public class ModifyOp extends BaseQuery {
    public static void main(String[] args) {
        update(fromModifyTable("db1", "travelrecord", "id"), delete());
    }


    public static ModifyStatement update(Schema source, List<RowModifer> tables) {
        return new ModifyStatement(source, tables);
    }

    public static ModifyStatement update(Schema source, RowModifer... tables) {
        return update(source, Arrays.asList(tables));
    }

    public static ModifyTable fromModifyTable(String schema, String table, String primaryColumn) {
        return new ModifyTable(schema, table, primaryColumn);
    }

    public static TargetModifyColumn targetModifyColumn(String schema, String table, String columnName) {
        return new TargetModifyColumn(schema, table, columnName);
    }

    public static RowModifer modify(TargetModifyColumn table, Node expr) {
        return new ModifyRowModifer();
    }

    public static RowModifer delete() {
        return new DeleteRowModifer();
    }

    public static RowModifer merge(Node matcher, List<RowModifer> rowModifer, List<Node> values) {
        return new MergeRowModifer(matcher, rowModifer, values);
    }
}