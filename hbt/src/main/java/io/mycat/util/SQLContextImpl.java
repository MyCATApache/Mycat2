package io.mycat.util;

import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.upondb.MycatDBClientMediator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SQLContextImpl implements SQLContext {
    final Map<String, MySQLFunction> map = new HashMap<>();
    private MycatDBClientMediator mycatDBClientMediator;

    public SQLContextImpl(MycatDBClientMediator mycatDBClientMediator) {
        this.mycatDBClientMediator = mycatDBClientMediator;
        map.put("next_value_for", new MySQLFunction() {
            @Override
            public String getFunctionName() {
                return "next_value_for";
            }

            @Override
            public int getArgumentSize() {
                return 1;
            }

            @Override
            public Object eval(Object[] args) {
                String name = args[0].toString().replaceAll("MYCATSEQ_", "");
                Supplier<String> sequence = SequenceGenerator.INSTANCE.getSequence(name);
                String s = sequence.get();
                return s;
            }
        });
    }

    @Override
    public Object getSQLVariantRef(String toString) {
        return null;
    }

    @Override
    public List<Object> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public void setParameters(List<Object> parameters) {

    }

    @Override
    public Map<String, MySQLFunction> functions() {
        return map;
    }
}