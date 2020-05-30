package io.mycat.mpp;

import java.util.List;


public abstract class FunctionExp implements ASTExp {
   protected String name;
    protected List<SqlValue> params;

    public FunctionExp(String name, List<SqlValue> params) {
        this.name = name;
        this.params = params;
    }

}