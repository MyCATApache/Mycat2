package io.mycat.calcite.relBuilder;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * chenjunwen
 */
public class MycatTransientSQLTable extends AbstractQueryableTable
        implements TransientTable, ModifiableTable, ScannableTable,ProjectableFilterableTable {
    private static final Type TYPE = Object[].class;
    private final String name;
    private final RelDataType protoRowType;
    private RelNode input;

    public MycatTransientSQLTable(String name, RelNode input) {
        super(TYPE);
        this.name = name;
        this.protoRowType = input.getRowType();
        this.input = input;
    }

    @Override
    public TableModify toModificationRel(
            RelOptCluster cluster,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode child,
            TableModify.Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public Collection getModifiableCollection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // add the table into the schema, so that it is accessible by any potential operator
        root.getRootSchema().add(name, this);

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new Enumerator<Object[]>() {
                    private final List list = new ArrayList();
                    private int i = -1;

                    // TODO cleaner way to handle non-array objects?
                    @Override
                    public Object[] current() {
                        Object current = list.get(i);
                        return current.getClass().isArray()
                                ? (Object[]) current
                                : new Object[]{current};
                    }

                    @Override
                    public boolean moveNext() {
                        if (cancelFlag != null && cancelFlag.get()) {
                            return false;
                        }

                        return ++i < list.size();
                    }

                    @Override
                    public void reset() {
                        i = -1;
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
    }

    public Expression getExpression(SchemaPlus schema, String tableName,
                                    Class clazz) {
        return Schemas.tableExpression(schema, elementType, tableName, clazz);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                        SchemaPlus schema, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            public Enumerator<T> enumerator() {
                //noinspection unchecked
                return (Enumerator<T>) Linq4j.enumerator(Collections.emptyList());
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(protoRowType);
    }

    @Override
    public Type getElementType() {
        return TYPE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        return scan(root);
    }
}