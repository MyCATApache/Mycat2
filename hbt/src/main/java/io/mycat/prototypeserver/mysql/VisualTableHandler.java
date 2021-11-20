package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.IndexInfo;
import io.mycat.LogicTableType;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.table.LogicTable;
import io.mycat.util.CalciteConvertors;
import io.mycat.util.SQL2ResultSetUtil;
import io.reactivex.rxjava3.core.Observable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class VisualTableHandler implements TableHandler {
   final LogicTable logicTable;

    public static VisualTableHandler createByMySQL(String sql, Supplier< Observable<Object[]> > rowsProvider){
        SQLStatement sqlStatement1 = SQLUtils.parseSingleMysqlStatement(sql);
        if (!(sqlStatement1 instanceof MySqlCreateTableStatement)){
            System.out.println();
        }
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement)sqlStatement1;
        String schema = SQLUtils.normalize(sqlStatement.getSchema()).toLowerCase();
        String tableName =  SQLUtils.normalize(sqlStatement.getTableName()).toLowerCase();
        MycatRowMetaData mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData((MySqlCreateTableStatement) sqlStatement);
        List<SimpleColumnInfo> columnInfo = CalciteConvertors.getColumnInfo(Objects.requireNonNull(mycatRowMetaData));
        LogicTable logicTable = new LogicTable(LogicTableType.NORMAL, schema, tableName, columnInfo, Collections.emptyMap(), sql);
        return new VisualTableHandler(logicTable){

            @Override
            public Observable<Object[]> scanAll() {
                return rowsProvider.get();
            }
        };
    }

    public VisualTableHandler(LogicTable logicTable) {
        this.logicTable = logicTable;
    }

    @Override
    public LogicTableType getType() {
        return LogicTableType.VISUAL;
    }

    @Override
    public String getSchemaName() {
        return logicTable.getSchemaName();
    }

    @Override
    public String getTableName() {
        return logicTable.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return logicTable.getCreateTableSQL();
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return logicTable.getRawColumns();
    }

    @Override
    public Map<String, IndexInfo> getIndexes() {
        return Collections.emptyMap();
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return  logicTable.getRawColumns().stream().filter(i->i.getColumnName().equals(name)).findFirst().orElse(null);
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return logicTable.getRawColumns().stream().filter(i->i.isAutoIncrement()).findFirst().orElse(null);
    }

    @Override
    public String getUniqueName() {
        return logicTable.getUniqueName();
    }

    @Override
    public Supplier<Number> nextSequence() {
        return null;
    }

    @Override
    public void createPhysicalTables() {

    }

    @Override
    public void dropPhysicalTables() {

    }

    public abstract Observable<Object[]> scanAll();
}
