package io.mycat.hbt;

import io.mycat.PlanRunner;
import io.mycat.TextUpdateInfo;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ColumnInfo;
import io.mycat.beans.mycat.DefMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.calcite.prepare.MycatHbtCalcitePrepareObject;
import io.mycat.calcite.prepare.MycatHbtPrepareObject;
import io.mycat.calcite.prepare.MycatTextUpdatePrepareObject;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import io.mycat.hbt.ast.query.CommandSchema;
import io.mycat.hbt.parser.HBTParser;
import io.mycat.hbt.parser.ParseNode;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.PrepareObject;
import org.jetbrains.annotations.NotNull;

import java.sql.Types;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class HBTRunners implements HBTRunner {
    final private MycatDBContext dbContext;

    public HBTRunners(MycatDBContext dbContext) {
        this.dbContext = dbContext;
    }

    @Override
    public RowBaseIterator run(String text) {
        HBTParser hbtParser = new HBTParser(text);
        ParseNode statement = hbtParser.statement();

        SchemaConvertor schemaConvertor = new SchemaConvertor();
        Schema originSchema = schemaConvertor.transforSchema(statement);

        MycatHbtPrepareObject prepareObject = complieHBT(null, 0, originSchema);

        return prepareObject.plan(Collections.emptyList()).run();
    }

    @Override
    public RowBaseIterator run(Schema schema) {
        return complieHBT(null,0,schema).plan(Collections.emptyList()).run();
    }


    public PrepareObject prepareHBT(Long id, String hbt) {
        HBTParser hbtParser = new HBTParser(hbt);
        List<ParseNode> parseNodes = hbtParser.statementList();
        if (parseNodes.size() != 1) {
            throw new UnsupportedOperationException();
        }
        return complieHBT(parseNodes.get(0), id, hbtParser.getParamCount());
    }

    private MycatHbtPrepareObject complieHBT(ParseNode parseNode, Long id, int paramCount) {
        SchemaConvertor schemaConvertor = new SchemaConvertor();
        Schema originSchema = schemaConvertor.transforSchema(parseNode);
        return complieHBT(id, paramCount, originSchema);
    }

    @NotNull
    private MycatHbtPrepareObject complieHBT(Long id, int paramCount, Schema originSchema) {
        MycatHbtPrepareObject prepareObject = null;
        switch (originSchema.getOp()) {
            case MODIFY_FROM_SQL: {
                ModifyFromSql originSchema1 = (ModifyFromSql) originSchema;
                MergeModify mergeModify = new MergeModify(Collections.singleton(originSchema1));
                prepareObject = complieMergeModify(id, paramCount, mergeModify, dbContext);
                break;
            }
            case MERGE_MODIFY: {
                MergeModify originSchema1 = (MergeModify) originSchema;
                prepareObject = complieMergeModify(id, paramCount, originSchema1, dbContext);
                break;
            }
            case EXPLAIN: {
                CommandSchema commandSchema = (CommandSchema) originSchema;
                return explain(id, paramCount,commandSchema.getSchema() );
            }
            default:
                prepareObject = new MycatHbtCalcitePrepareObject(id, paramCount, originSchema, dbContext);
        }
        return prepareObject;
    }


    private MycatHbtPrepareObject explain(Long id, int paramCount, Schema schema) {
        MycatHbtPrepareObject innerPrepareObject = complieHBT(id, paramCount, schema);
        return new MycatHbtPrepareObject(id, paramCount) {

            @Override
            public MycatRowMetaData resultSetRowType() {
               return new DefMycatRowMetaData(Collections.singletonList(new ColumnInfo("plan", Types.VARCHAR)));
            }

            @Override
            public PlanRunner plan(List<Object> params) {
                PlanRunner plan = innerPrepareObject.plan(params);
                return new PlanRunner() {
                    @Override
                    public List<String> explain() {
                     return plan.explain();
                    }

                    @Override
                    public RowBaseIterator run() {
                        return plan.run();
                    }
                };
            }
        };
    }

    private MycatHbtPrepareObject complieMergeModify(Long id, int paramCount, MergeModify mergeModify, MycatDBContext dbContext) {
        return new MycatHbtPrepareObject(id, paramCount) {
            @Override
            public MycatRowMetaData resultSetRowType() {
                return UpdateRowMetaData.INSTANCE;
            }

            @Override
            public PlanRunner plan(List<Object> params) {
                return new MycatTextUpdatePrepareObject(id, paramCount, (mycatTextUpdatePrepareObject, list) -> {
                    Iterator<ModifyFromSql> iterator = mergeModify.getList().iterator();
                    return getTextUpdateInfoIterator(iterator);
                }, dbContext).plan(params);
            }
        };
    }

    @NotNull
    private Iterator<TextUpdateInfo> getTextUpdateInfoIterator(Iterator<ModifyFromSql> iterator) {
        return new Iterator<TextUpdateInfo>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public TextUpdateInfo next() {
                ModifyFromSql next = iterator.next();
                return TextUpdateInfo.create(next.getTargetName(), Collections.singletonList(next.getSql()));
            }
        };
    }
}