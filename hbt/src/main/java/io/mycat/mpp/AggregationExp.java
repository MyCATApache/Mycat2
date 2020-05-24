package io.mycat.mpp;

import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.Type;
import org.apache.calcite.interpreter.Row;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AggregationExp extends FunctionExp {
    final String columnName;

    public AggregationExp(String name, List<SqlValue> param, String columnName) {
        super(name, param);
        this.columnName = columnName;
    }

    public abstract void accept(DataAccessor tuple);

    public abstract Object getValue();

    public  abstract void reset();

    public abstract Class type();

    public String getColumnName() {
        return columnName;
    }
    @NotNull
    public static AggregationExp of(String aggCallName, String columnName, List<Integer> argsList) {
        List<AccessDataExpr> accessList = argsList.stream()
                .map(AccessDataExpr::of).collect(Collectors.toList());
        AggregationExp aggregationExp;
        switch (aggCallName){
            case "count":
                aggregationExp = new CountExp((List)accessList,columnName);
                break;
            case "sum":
                aggregationExp = new SumExp((List)accessList,columnName);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return aggregationExp;
    }

    public void merge(AggregationExp column) {

    }
}
    