///**
// * Copyright (C) <2020>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.calcite.executor;
//
//import com.google.common.collect.ImmutableMultimap;
//import io.mycat.*;
//import io.mycat.api.collector.ComposeRowBaseIterator;
//import io.mycat.api.collector.RowBaseIterator;
//import io.mycat.api.collector.RowIteratorCloseCallback;
//import io.mycat.calcite.MycatCalciteSupport;
//import io.mycat.calcite.resultset.CalciteRowMetaData;
//import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
//import io.mycat.calcite.logical.MycatView;
//import io.mycat.calcite.DataSourceFactory;
//import io.mycat.calcite.Executor;
//import io.mycat.calcite.ExplainWriter;
//import io.mycat.mpp.Row;
//import io.mycat.sqlrecorder.SqlRecord;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.rex.RexCorrelVariable;
//import org.apache.calcite.rex.RexFieldAccess;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.rex.RexShuttle;
//import org.apache.calcite.sql.util.SqlString;
//
//import java.sql.Connection;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import static io.mycat.calcite.executor.MycatPreparedStatementUtil.executeQuery;
//
///**
// * it must be on the right of MycatBatchNestedLoopJoinExecutor
// */
//public class MycatLookupExecutor implements Executor {
//
//    private MycatDataContext context;
//    private final MycatView view;
//    private final CalciteRowMetaData metaData;
//    private DataSourceFactory factory;
//    private List<Object> params;
//    private MyCatResultSetEnumerator myCatResultSetEnumerator = null;
//    private List<MycatConnection> tmpConnections;
//
//    public MycatLookupExecutor(MycatDataContext context, MycatView view, DataSourceFactory factory, List<Object> params) {
//        this.context = context;
//        this.view = view;
//        this.factory = factory;
//        this.params = params;
//        this.metaData = new CalciteRowMetaData(this.view.getRowType().getFieldList());
//    }
//
//    public static MycatLookupExecutor create(MycatDataContext context, MycatView view, DataSourceFactory factory, List<Object> params) {
//        return new MycatLookupExecutor(context,view, factory, params);
//    }
//
//    void setIn(List<Row> args) {
//        if (myCatResultSetEnumerator != null) {
//            myCatResultSetEnumerator.close();
//            myCatResultSetEnumerator = null;
//        }
//        if (tmpConnections != null) {
//            factory.recycleTmpConnections(tmpConnections);
//            tmpConnections = null;
//        }
//        //convert relNode to sql with cor variable
//        RelNode accept = this.view.getRelNode().accept(new RexShuttle() {
//            @Override
//            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
//                RelDataTypeField field = fieldAccess.getField();
//                int index = field.getIndex();
//                RelDataType type = field.getType();
//                if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
//                    RexCorrelVariable variable = (RexCorrelVariable) fieldAccess.getReferenceExpr();
//                    int id = variable.id.getId();
//                    Row row = args.get(id);
//                    return MycatCalciteSupport.RexBuilder.makeLiteral(row.getObject(index), type, false);
//                }
//                return super.visitFieldAccess(fieldAccess);
//            }
//        });
//        MycatView newView = view.changeTo(accept, this.view.getDistribution());
//        ImmutableMultimap<String, SqlString> expandToSqls = newView.expandToSql(false, params);
//
//        MycatWorkerProcessor instance = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
//        NameableExecutor mycatWorker = instance.getMycatWorker();
//        LinkedList<RowBaseIterator> futureArrayList = new LinkedList<>();
//        this.tmpConnections = factory.getTmpConnections(expandToSqls.keys().asList());
//        int i = 0;
//
//        SqlRecord sqlRecord = context.currentSqlRecord();
//        for (Map.Entry<String, SqlString> entry : expandToSqls.entries()) {
//            MycatConnection connection = tmpConnections.get(i);
//            String target = entry.getKey();
//            SqlString sql = entry.getValue();
//            long startTime = SqlRecord.now();
//            futureArrayList.add(executeQuery(connection.unwrap(Connection.class), connection, metaData, sql, params, new RowIteratorCloseCallback() {
//                @Override
//                public void onClose(long rowCount) {
//                    sqlRecord.addSubRecord(sql,startTime,SqlRecord.now(),target,rowCount);
//                }
//            }));
//            i++;
//        }
//        AtomicBoolean flag = new AtomicBoolean();
//        ComposeRowBaseIterator composeFutureRowBaseIterator = new ComposeRowBaseIterator(metaData, futureArrayList);
//        this.myCatResultSetEnumerator = new MyCatResultSetEnumerator(composeFutureRowBaseIterator);
//    }
//
//    @Override
//    public void open() {
//
//    }
//
//    @Override
//    public Row next() {
//        if (myCatResultSetEnumerator.moveNext()) {
//            return Row.of(myCatResultSetEnumerator.current());
//        }
//        return null;
//    }
//
//    @Override
//    public void close() {
//        if (this.myCatResultSetEnumerator != null) {
//            this.myCatResultSetEnumerator.close();
//        }
//        if (tmpConnections != null) {
//            factory.recycleTmpConnections(tmpConnections);
//            tmpConnections = null;
//        }
//    }
//
//    @Override
//    public boolean isRewindSupported() {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        ExplainWriter explainWriter = writer.name(this.getClass().getName())
//                .into();
//        view.explain(writer);
//        explainWriter.item("params",params);
//        return explainWriter.ret();
//    }
//}