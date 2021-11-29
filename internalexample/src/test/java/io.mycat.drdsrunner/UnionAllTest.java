package io.mycat.drdsrunner;

import io.mycat.DrdsConst;
import io.mycat.DrdsExecutorCompiler;
import io.mycat.DrdsSqlCompiler;
import io.mycat.DrdsSqlWithParams;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.spm.SpecificSql;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.util.NameMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class UnionAllTest extends DrdsTest {

    @BeforeClass
    public static void beforeClass() {
        DrdsTest.drdsRunner = null;
        DrdsTest.metadataManager = null;
    }

    public static Explain parse(String sql) {
        DrdsSqlCompiler drds = getDrds();
        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sql, null);
        OptimizationContext optimizationContext = new OptimizationContext();
        MycatRel dispatch = drds.dispatch(optimizationContext, drdsSqlWithParams);
        Plan plan = new PlanImpl(dispatch, DrdsExecutorCompiler.getCodeExecuterContext(optimizationContext.relNodeContext.getConstantMap(), dispatch, false), drdsSqlWithParams.getAliasList());
        return new Explain(plan, drdsSqlWithParams);
    }

    @Test
    public void testSelectTest() throws Exception {
        Explain explain = parse("select 1 union all select 1");
        Assert.assertEquals("[{columnType=INTEGER, nullable=false, columnName=1}]", explain.getColumnInfo());
        Assert.assertEquals("MycatUnion(all=[true])   MycatProject(?=[?0])     MycatValues(tuples=[[{ 0 }]])   MycatProject(?=[?1])     MycatValues(tuples=[[{ 0 }]])", explain.dumpPlan());
    }

    @Test
    public void testSelectNormal() throws Exception {
        Explain explain = parse("select 1 from db1.normal union all select 1 from db1.normal");
        Assert.assertEquals("[{columnType=INTEGER, nullable=false, columnName=1}]", explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.normal]])", explain.dumpPlan());
    }

    @Test
    public void testSelectNormalGlobal() throws Exception {
        Explain explain = parse("select 1 from db1.normal union all select 1 from db1.global");
        Assert.assertEquals("[{columnType=INTEGER, nullable=false, columnName=1}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.normal]])", explain.dumpPlan());
    }
    @Test
    public void testSelectNormalSharding() throws Exception {
        Explain explain = parse("select 1 from db1.normal union all select 1 from db1.sharding");
        Assert.assertEquals("[{columnType=INTEGER, nullable=false, columnName=1}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatUnion(all=[true])   MycatView(distribution=[[db1.normal]])   MycatView(distribution=[[db1.sharding]])", explain.dumpPlan());
    }

}
