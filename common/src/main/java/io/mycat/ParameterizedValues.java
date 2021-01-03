package io.mycat;

import lombok.Data;

import java.util.List;

@Data
public class ParameterizedValues {
    String target;
    String sql;
    List<Object> params;

    public static ParameterizedValues of(
            String target,
            String sql,
            List<Object> params
    ) {
        ParameterizedValues parameterizedValues = new ParameterizedValues();
        parameterizedValues.setParams(params);
        parameterizedValues.setSql(sql);
        parameterizedValues.setTarget(target);
        return parameterizedValues;
    }
}