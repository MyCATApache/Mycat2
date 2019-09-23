package cn.lightfish.pattern.methodFactory;

import cn.lightfish.pattern.AddMehodClassFactory;
import cn.lightfish.pattern.Instruction;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class AddMehodClassAsSubClassFactoryTest {
    @Test
    public void test() throws Exception {
        AddMehodClassFactory factory = new AddMehodClassFactory("Name", Object.class);
        Class o = factory.build(false);
        Object o1 = o.newInstance();
        Assert.assertNotNull(o1);
        Assert.assertEquals(o1.getClass().getName(), "Name");
    }

    @Test
    public void test1() throws Exception {
        AddMehodClassFactory factory = new AddMehodClassFactory("Name1", Object.class);
        Class o = factory.build(true);
        Object o1 = o.newInstance();
        Assert.assertNotNull(o1);
    }

    @Test
    public void test2() throws Exception {
        AddMehodClassFactory factory = new AddMehodClassFactory("Name2", Object.class);
        factory.addMethod("public String name(){return \"hello\";}");
        Class o = factory.build(true);
        Object o1 = o.newInstance();
        Method name = o1.getClass().getDeclaredMethod("name");
        String value = (String) name.invoke(o1);
        Assert.assertEquals("hello", value);
    }

    @Test
    public void test3() throws Exception {
        AddMehodClassFactory factory = new AddMehodClassFactory("Name3", Object.class);
        factory.addExpender(TestExpenderCollection.class);
        Class o = factory.build(true);
        Object o1 = o.newInstance();
        Method name = o1.getClass().getDeclaredMethod("name");
        String value = (String) name.invoke(o1);
        Assert.assertEquals("name", value);
    }

    @Test
    public void test4() throws Exception {
        AddMehodClassFactory factory = new AddMehodClassFactory("Name4", Object.class);
        factory.addExpender("cn.lightfish.pattern.methodFactory", TestExpenderInterface.class);
        Class o = factory.build(true);
        Object o1 = o.newInstance();
        Method name = o1.getClass().getDeclaredMethod("name");
        String value = (String) name.invoke(o1);
        Assert.assertEquals("name", value);
    }

    @Test
    public void test5() throws Exception {
        AddMehodClassFactory factory = new AddMehodClassFactory("Name5", Instruction.class);
        factory.implMethod("execute", "Object ctx = $1;", "System.out.println(ctx);return null;");
        Class o = factory.build(true);
        Instruction o1 = (Instruction) o.newInstance();
        o1.execute(null, null);
    }
}