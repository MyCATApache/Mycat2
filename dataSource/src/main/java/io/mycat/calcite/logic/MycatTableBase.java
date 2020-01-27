package io.mycat.calcite.logic;

import io.mycat.calcite.CalciteConvertors;
import io.mycat.calcite.MetadataManager;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.lang.reflect.Type;
import java.util.List;

public  abstract class MycatTableBase extends AbstractQueryableTable implements TranslatableTable,ProjectableFilterableTable {
    protected MycatTableBase() {
        super(Object[].class);
    }

    protected MycatTableBase(Type elementType) {
        super(elementType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        return null;
    }

    public abstract MetadataManager.LogicTable logicTable();

        @Override
         public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return CalciteConvertors.getRelDataType(logicTable().getRawColumns(), typeFactory);
        }

        @Override
         public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        @Override
        public   Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        @Override
        public  boolean isRolledUp(String column) {
            return false;
        }

        @Override
        public  boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            return false;
        }

//        @Override
//        default public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
//            List<QueryBackendTask> backendTasks = getQueryBackendTasks(logicTable(),  filters, projects);
//            return new MyCatResultSetEnumerable(CalciteUtls.getCancelFlag(root), backendTasks);
//        }

        @Override
         public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
          //  return new MycatFetchSource(context.getCluster(),context.getCluster().traitSet(), relOptTable,relOptTable.getRowType());
       return LogicalTableScan.create(context.getCluster(),relOptTable);
        }

    @Override
    public   <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName){
            return new SplunkTableQueryable(queryProvider,schema,tableName,this);
    }

}