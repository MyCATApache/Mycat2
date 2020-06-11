package io.mycat.router.function;

import io.mycat.router.SingleValueRuleFunction;

import java.util.List;
import java.util.Map;

/**
 *
 * @author chenjunwen
 */
public class ColumnJoinerRuleFunction {
    final String name;
    final SingleValueRuleFunction ruleFunction;

    public ColumnJoinerRuleFunction(String name, SingleValueRuleFunction ruleFunction) {
        this.name = name;
        this.ruleFunction = ruleFunction;
    }

    public String name() {
        return name;
    }

    public int calculate(List<String> columnValues) {
       return ruleFunction.calculate(join(columnValues));
    }
    public  void callInit(Map<String, String> prot, Map<String, String> ranges) {
        ruleFunction.callInit(prot, ranges);
    }
    private String join(List<String> columnValues) {
        return String.join("", columnValues);
    }

    public int[] calculateRange(List<String> beginValues, List<String> endValues) {
        return ruleFunction.calculateRange(join(beginValues),join(endValues));
    }

}