package cn.lightfish.pattern.dynamicSQL;

import cn.lightfish.pattern.DynamicSQLMatcher;
import cn.lightfish.pattern.DynamicSQLMatcherBuilder;
import cn.lightfish.pattern.Instruction;
import cn.lightfish.pattern.InstructionSetImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class DynamicSQLMatcherBuilderTest {

    @Test
    public void test() throws Exception {
        DynamicSQLMatcherBuilder builder = new DynamicSQLMatcherBuilder("db1");
        builder.add("select DB2.TABLE2.id from DB1.TABLE,DB2.TABLE2", "return Integer.valueOf(0);");
        builder.add("select 1;", "return Integer.valueOf(1);");
        builder.addSchema("DB1.TABLE,DB2.TABLE2", "select * from {tables}", "return Integer.valueOf(2);");
        builder.addSchema("DB1.TABLE,DB2.TABLE2", null, "return Integer.valueOf(3);");
        builder.build("cn.lightfish.pattern.methodFactory", false);
        DynamicSQLMatcher dynamicSQLMatcher = builder.createMatcher();

        Assert.assertEquals(Integer.valueOf(0), dynamicSQLMatcher.match("select DB2.TABLE2.id from DB1.TABLE,DB2.TABLE2").execute(null, dynamicSQLMatcher));
        Assert.assertEquals(Integer.valueOf(1), dynamicSQLMatcher.match("select 1;").execute(null, dynamicSQLMatcher));
        Assert.assertEquals(Integer.valueOf(2), dynamicSQLMatcher.match("select * from DB1.TABLE,DB2.TABLE2").execute(null, dynamicSQLMatcher));
        Assert.assertNull(dynamicSQLMatcher.match("select 2;"));
        Assert.assertEquals(Integer.valueOf(3), dynamicSQLMatcher.match("select DB1.TABLE.id,DB2.TABLE2.id from DB1.TABLE,DB2.TABLE2").execute(null, dynamicSQLMatcher));
    }

    @Test
    public void test1() throws Exception {
        DynamicSQLMatcherBuilder builder = new DynamicSQLMatcherBuilder(null);
        builder.add("select {n};", "return toUpperCase(ctx,one())+getNameAsString(matcher,\"n\");");
        builder.build("cn.lightfish", false);
        DynamicSQLMatcher dynamicSQLMatcher = builder.createMatcher();
        Instruction match = dynamicSQLMatcher.match("select 1;");
        HashMap<Byte, Object> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put(InstructionSetImpl.one(), "a");
        Object execute = match.execute(objectObjectHashMap, dynamicSQLMatcher);
        Assert.assertEquals("A1", execute);
    }


}