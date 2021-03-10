package io.mycat.drdsrunner;

import io.mycat.calcite.spm.Plan;
import org.junit.Assert;
import org.junit.Test;

public class SingleShardingTest extends DrdsTest {
    @Test
    public void testSelect1() throws Exception {
        Explain explain = parse("select 1");
        Assert.assertEquals("[{columnType=INTEGER, nullable=false, columnName=1}]",explain.getColumnInfo());
        Assert.assertEquals("MycatProject(1=[1])\n" +
                "  MycatValues(tuples=[[{ 0 }]])", explain.dumpPlan());
    }

    @Test
    public void testSelectNormal() throws Exception {
        Explain explain  = parse("select * from db1.normal");
        Assert.assertEquals("[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[Distribution{normalTables= db1.normal ])\n" +
                "prototype=SELECT * FROM db1.normal",explain.dumpPlan());
    }

    @Test
    public void testSelectSharding() throws Exception {
        Explain explain  = parse("select * from db1.sharding");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[Distribution{shardingTables= db1.sharding ])\n" +
                "c0=(SELECT *     FROM db1_0.travelrecord_0     UNION ALL     SELECT *     FROM db1_0.travelrecord_1)\n" +
                "c1=(SELECT *     FROM db1_1.travelrecord_0     UNION ALL     SELECT *     FROM db1_1.travelrecord_1)",explain.dumpPlan());
    }
}
