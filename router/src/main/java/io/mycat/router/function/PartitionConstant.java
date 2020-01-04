package io.mycat.router.function;

import io.mycat.router.RuleFunction;

import java.util.Map;

/**
 * @author jamie12221 date 2020-01-04
 **/
public class PartitionConstant extends RuleFunction {

    private int defaultNode;
    private int[] nodes;

    @Override
    public String name() {
        return "PartitionConstant";
    }

    @Override
    public void init(Map<String, String> properties, Map<String, String> ranges) {
        this.defaultNode = Integer.parseInt(properties.get("defaultNode"));
        this.nodes = new int[]{defaultNode};
    }

    @Override
    public int calculate(String columnValue) {
        return defaultNode;
    }

    @Override
    public int[] calculateRange(String beginValue, String endValue) {
        return nodes;
    }

    @Override
    public int getPartitionNum() {
        return 1;
    }
}
